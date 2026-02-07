# Lite Task Dispatcher 压测报告（V2）

## 1. 测试目标

- 复测并验证执行链路修复是否生效（重点：`PENDING -> RUNNING -> SUCCESS`）。
- 复现上一版同档位场景：主场景 `5000任务 @ 200 RPS`。
- 复现高可用场景：压测期间下线一个节点，验证剩余节点持续可用。
- 采集健康、日志、数据库、Micrometer/Prometheus证据，形成可追溯结论。

## 2. 测试环境

- 测试时间：2026-02-07。
- 应用实例：
  - node-a: `server.port=8080`
  - node-b: `server.port=8081`
- 基础设施：
  - Postgres: `task-dispatcher-postgres` (`5432`)
  - Redis: `task-dispatcher-redis` (`6379`)
  - Kafka: `localhost:9092`
- Worker参数（每实例）：
  - `task.worker.threads=32`
  - `task.worker.poll-interval-ms=5`
  - `task.worker.idle-sleep-ms=20`

## 3. 测试方法与执行步骤

### 3.1 预检

- 双实例健康检查通过（DB/Redis均为UP）：
  - `.perf/retest-health-8080.json`
  - `.perf/retest-health-8081.json`
- 指标端点可访问：
  - `/actuator/prometheus`
  - `/actuator/metrics/http.server.requests`

### 3.2 主场景（与上一版同档位）

- 脚本：`scripts/load_test_runner.py`
- 参数：`--total 5000 --rps 200 --concurrency 300 --delay-ms 200`
- 基准地址：`http://localhost:8081`
- `createdBy`：
  - 首轮：`loadtest-20260207-retest-main`
  - 复跑（V2主结果）：`loadtest-20260207-retest-main-v2`

### 3.3 高可用场景（HA）

- 参数：`--total 1000 --rps 100 --concurrency 200`
- `createdBy`：`loadtest-20260207-retest-ha`
- 执行动作：启动压测后下线 `8080`，仅保留 `8081` 对外服务。

## 4. 关键结果

### 4.1 主场景提交与延迟（V2最终结果）

- 提交结果：
  - accepted: `4999`
  - rejected: `0`
  - transport_errors: `1`
  - actual_submit_rps: `199.94`
- 提交延迟：
  - p50=`5.43ms`
  - p95=`22.24ms`
  - p99=`43.99ms`
  - max=`97.23ms`
- 结果文件：
  - `.perf/retest-load-test-summary-v2.json`
  - `.perf/retest-load-test-result-v2.json`

### 4.2 执行链路（状态流转）

- 主场景任务终态（DB）：
  - `created_by='loadtest-20260207-retest-main-v2'` -> `SUCCESS=4999`
- HA场景任务终态（DB）：
  - `created_by='loadtest-20260207-retest-ha'` -> `SUCCESS=1000`
- 执行日志落库：
  - 主场景 `task_execution_log=4999`
  - HA场景 `task_execution_log=1000`
- 结论：执行链路闭环成功，未出现任务卡在 `PENDING` 的问题。

### 4.3 历史缺陷回归验证

- 关键错误回归：`Cannot complete task in status PENDING, expected RUNNING`
  - `retest-app-8080.log`: `0`
  - `retest-app-8081.log`: `0`
- 执行错误日志：`Error executing task` 计数为 `0`（两实例新日志）。

### 4.4 高可用验证

- 故障动作：压测进行中停止 `8080`。
- 健康检查：
  - `8080` -> `502`
  - `8081` -> `200`
- HA提交结果（1000@100RPS）：
  - accepted=`1000`
  - transport_errors=`0`
  - completed_delta=`1000`
- 结论：单节点下线后，剩余节点提交与执行均连续可用。

### 4.5 可观测性验证

- Prometheus快照行数：`304 -> 356`（前后快照均可抓取）。
- `http.server.requests` 指标可用：
  - COUNT=`11016`
  - TOTAL_TIME=`119.8439s`
  - MAX=`0.6475s`
- 证据文件：
  - `.perf/retest-prometheus-before.txt`
  - `.perf/retest-prometheus-after.txt`
  - `.perf/retest-metrics-http-server-requests-after.json`

## 5. 与上一版报告对比结论

- 上一版（2026-02-07旧报告）结论：
  - 提交面通过，但执行面存在状态流转异常，任务大量卡在 `PENDING`。
- 本版（V2）结论：
  - 提交面：通过（高RPS下稳定，错误极低）。
  - 执行面：通过（终态落库与执行日志一致，无 `PENDING` 堆积）。
  - 高可用：通过（节点下线后单节点持续承载提交与执行）。
  - 可观测性：通过（指标与日志可支撑定位与验收）。

## 6. 通过性评估（V2）

- 高并发（提交吞吐）：**通过**
- 执行链路（状态流转与终态一致性）：**通过**
- 高可用（单点故障切换）：**通过**
- 可观测性：**通过**

## 7. 执行过程中的环境调整说明

- 为匹配压测脚本任务类型，补充了 `LOG` 任务定义（`executor_type=LOG`）。
- 首轮主场景受限流影响（`transport_errors=2416`），将 `LOG` 的 `rate_limit` 调整为 `10000` 后复跑得到V2主结果。
- 首轮结果文件保留用于追溯：
  - `.perf/retest-load-test-summary.json`
  - `.perf/retest-load-test-result.json`

## 8. 产物清单

- 主场景（V2最终）
  - `.perf/retest-load-test-summary-v2.json`
  - `.perf/retest-load-test-result-v2.json`
- 主场景（首轮）
  - `.perf/retest-load-test-summary.json`
  - `.perf/retest-load-test-result.json`
- HA场景
  - `.perf/retest-load-test-ha-summary.json`
  - `.perf/retest-load-test-ha-result.json`
- 健康与指标
  - `.perf/retest-health-8080.json`
  - `.perf/retest-health-8081.json`
  - `.perf/retest-prometheus-before.txt`
  - `.perf/retest-prometheus-after.txt`
  - `.perf/retest-metrics-http-server-requests-after.json`
- 应用日志
  - `.perf/retest-app-8080.log`
  - `.perf/retest-app-8081.log`
