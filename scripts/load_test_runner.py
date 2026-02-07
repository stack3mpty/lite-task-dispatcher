#!/usr/bin/env python3
import argparse
import json
import statistics
import threading
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed


def http_get_json(url: str, timeout: float = 5.0):
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body)


def http_post_json(url: str, payload: dict, timeout: float = 5.0):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
        return resp.getcode(), json.loads(body)


def percentile(values, p):
    if not values:
        return 0.0
    sorted_values = sorted(values)
    k = int(round((p / 100.0) * (len(sorted_values) - 1)))
    return float(sorted_values[k])


def main():
    parser = argparse.ArgumentParser(description="Load test for lite-task-dispatcher")
    parser.add_argument("--base-url", default="http://localhost:8081")
    parser.add_argument("--total", type=int, default=5000)
    parser.add_argument("--rps", type=float, default=200.0)
    parser.add_argument("--concurrency", type=int, default=300)
    parser.add_argument("--delay-ms", type=int, default=200)
    parser.add_argument("--created-by", required=True)
    parser.add_argument("--wait-timeout-sec", type=int, default=900)
    parser.add_argument("--poll-interval-sec", type=float, default=2.0)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    submit_url = f"{args.base_url}/api/v1/tasks"
    stats_url = f"{args.base_url}/api/v1/stats/dashboard"

    baseline_raw = http_get_json(stats_url)
    baseline = baseline_raw.get("data", {})

    lock = threading.Lock()
    accepted = 0
    rejected = 0
    transport_errors = 0
    latencies_ms = []

    def submit_one(i: int):
        nonlocal accepted, rejected, transport_errors
        payload = {
            "taskType": "LOG",
            "priority": i % 5,
            "createdBy": args.created_by,
            "params": {
                "message": f"load-task-{i}",
                "delay": args.delay_ms,
                "uniq": str(uuid.uuid4()),
            },
        }
        start = time.perf_counter()
        try:
            code, body = http_post_json(submit_url, payload, timeout=8.0)
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            with lock:
                latencies_ms.append(elapsed_ms)
                if code == 200 and body.get("code") == 0:
                    accepted += 1
                else:
                    rejected += 1
        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            TimeoutError,
            json.JSONDecodeError,
        ):
            with lock:
                transport_errors += 1

    submit_start = time.time()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = []
        for i in range(args.total):
            target = submit_start + (i / args.rps)
            now = time.time()
            if target > now:
                time.sleep(target - now)
            futures.append(pool.submit(submit_one, i))

        for f in as_completed(futures):
            _ = f.result()

    submit_end = time.time()
    submit_duration_sec = submit_end - submit_start

    snapshots = []
    wait_start = time.time()
    completed_delta = 0
    while time.time() - wait_start <= args.wait_timeout_sec:
        raw = http_get_json(stats_url)
        data = raw.get("data", {})
        total_delta = data.get("totalTasks", 0) - baseline.get("totalTasks", 0)
        success_delta = data.get("successTasks", 0) - baseline.get("successTasks", 0)
        failed_delta = data.get("failedTasks", 0) - baseline.get("failedTasks", 0)
        pending_delta = data.get("pendingTasks", 0) - baseline.get("pendingTasks", 0)
        running_delta = data.get("runningTasks", 0) - baseline.get("runningTasks", 0)
        completed_delta = success_delta + failed_delta

        snapshots.append(
            {
                "ts": time.time(),
                "total_delta": total_delta,
                "success_delta": success_delta,
                "failed_delta": failed_delta,
                "pending_delta": pending_delta,
                "running_delta": running_delta,
                "queue_lengths": data.get("queueLengths", {}),
            }
        )

        if completed_delta >= accepted:
            break

        time.sleep(args.poll_interval_sec)

    final_raw = http_get_json(stats_url)
    final_data = final_raw.get("data", {})

    result = {
        "config": {
            "base_url": args.base_url,
            "total": args.total,
            "rps": args.rps,
            "concurrency": args.concurrency,
            "delay_ms": args.delay_ms,
            "created_by": args.created_by,
        },
        "submission": {
            "accepted": accepted,
            "rejected": rejected,
            "transport_errors": transport_errors,
            "duration_sec": submit_duration_sec,
            "actual_submit_rps": (accepted + rejected + transport_errors)
            / submit_duration_sec
            if submit_duration_sec > 0
            else 0,
            "latency_ms": {
                "avg": statistics.fmean(latencies_ms) if latencies_ms else 0,
                "p50": percentile(latencies_ms, 50),
                "p95": percentile(latencies_ms, 95),
                "p99": percentile(latencies_ms, 99),
                "max": max(latencies_ms) if latencies_ms else 0,
            },
        },
        "dashboard_baseline": baseline,
        "dashboard_final": final_data,
        "delta": {
            "total": final_data.get("totalTasks", 0) - baseline.get("totalTasks", 0),
            "success": final_data.get("successTasks", 0)
            - baseline.get("successTasks", 0),
            "failed": final_data.get("failedTasks", 0) - baseline.get("failedTasks", 0),
            "pending": final_data.get("pendingTasks", 0)
            - baseline.get("pendingTasks", 0),
            "running": final_data.get("runningTasks", 0)
            - baseline.get("runningTasks", 0),
            "completed": completed_delta,
        },
        "snapshots": snapshots,
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=True, indent=2)

    print(
        json.dumps(
            {
                "accepted": accepted,
                "rejected": rejected,
                "transport_errors": transport_errors,
                "submit_duration_sec": round(submit_duration_sec, 3),
                "actual_submit_rps": round(
                    result["submission"]["actual_submit_rps"], 2
                ),
                "completed_delta": completed_delta,
            }
        )
    )


if __name__ == "__main__":
    main()
