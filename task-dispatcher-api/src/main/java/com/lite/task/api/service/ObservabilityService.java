package com.lite.task.api.service;

import com.lite.task.api.dto.response.ObservabilitySnapshotResponse;
import com.lite.task.api.observability.RequestTraceEntry;
import com.lite.task.api.observability.RequestTraceStore;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class ObservabilityService {

    private final MeterRegistry meterRegistry;
    private final RequestTraceStore requestTraceStore;

    @Value("${task.observability.trace.query-limit:30}")
    private int traceQueryLimit;

    @Value("${task.observability.endpoint-metrics.limit:20}")
    private int endpointMetricsLimit;

    public ObservabilitySnapshotResponse snapshot() {
        List<RequestTraceEntry> traces = requestTraceStore.recent(Math.max(traceQueryLimit, 1));
        List<ObservabilitySnapshotResponse.EndpointMetrics> endpointMetrics = meterRegistry
                .find("api.request.duration.endpoint")
                .timers()
                .stream()
                .map(this::toEndpointMetrics)
                .sorted(Comparator.comparingDouble(ObservabilitySnapshotResponse.EndpointMetrics::getP99Ms)
                        .reversed())
                .limit(Math.max(endpointMetricsLimit, 1))
                .toList();

        ObservabilitySnapshotResponse.NetworkMetrics network = buildNetworkMetrics(traces);

        return ObservabilitySnapshotResponse.builder()
                .generatedAt(LocalDateTime.now())
                .apiLatency(readTimer("api.request.duration"))
                .taskLatency(readTaskLatency())
                .resources(buildResourceMetrics())
                .network(network)
                .endpointMetrics(endpointMetrics)
                .recentTraces(traces.stream().map(this::toTraceDto).toList())
                .build();
    }

    private ObservabilitySnapshotResponse.LatencyMetrics readTaskLatency() {
        Timer global = meterRegistry.find("task.execution.duration.all").timer();
        if (global != null) {
            return readTimer("task.execution.duration.all");
        }

        var timers = meterRegistry.find("task.execution.duration").timers();
        if (timers.isEmpty()) {
            return emptyLatency();
        }

        long count = 0L;
        double totalMs = 0D;
        double maxMs = 0D;
        for (Timer timer : timers) {
            count += timer.count();
            totalMs += timer.totalTime(TimeUnit.MILLISECONDS);
            maxMs = Math.max(maxMs, timer.max(TimeUnit.MILLISECONDS));
        }

        return ObservabilitySnapshotResponse.LatencyMetrics.builder()
                .count(count)
                .avgMs(count > 0 ? round(totalMs / count) : 0D)
                .p50Ms(0D)
                .p95Ms(0D)
                .p99Ms(0D)
                .maxMs(round(maxMs))
                .build();
    }

    private ObservabilitySnapshotResponse.LatencyMetrics readTimer(String meterName) {
        Timer timer = meterRegistry.find(meterName).timer();
        if (timer == null) {
            return emptyLatency();
        }

        HistogramSnapshot snapshot = timer.takeSnapshot();
        return ObservabilitySnapshotResponse.LatencyMetrics.builder()
                .count(timer.count())
                .avgMs(round(timer.mean(TimeUnit.MILLISECONDS)))
                .p50Ms(round(percentileMs(snapshot, 0.5)))
                .p95Ms(round(percentileMs(snapshot, 0.95)))
                .p99Ms(round(percentileMs(snapshot, 0.99)))
                .maxMs(round(timer.max(TimeUnit.MILLISECONDS)))
                .build();
    }

    private ObservabilitySnapshotResponse.ResourceMetrics buildResourceMetrics() {
        double systemCpu = gaugeValue("system.cpu.usage") * 100;
        double processCpu = gaugeValue("process.cpu.usage") * 100;
        double heapUsedBytes = gaugesSum("jvm.memory.used", "area", "heap");
        double heapMaxBytes = gaugesSum("jvm.memory.max", "area", "heap");
        double nonHeapUsedBytes = gaugesSum("jvm.memory.used", "area", "nonheap");
        double heapUsagePct = heapMaxBytes > 0 ? (heapUsedBytes / heapMaxBytes) * 100 : 0D;
        long liveThreads = Math.round(gaugeValue("jvm.threads.live"));
        double uptimeSeconds = gaugeValue("process.uptime");

        return ObservabilitySnapshotResponse.ResourceMetrics.builder()
                .systemCpuUsagePct(round(systemCpu))
                .processCpuUsagePct(round(processCpu))
                .heapUsedMb(round(bytesToMb(heapUsedBytes)))
                .heapMaxMb(round(bytesToMb(heapMaxBytes)))
                .heapUsagePct(round(heapUsagePct))
                .nonHeapUsedMb(round(bytesToMb(nonHeapUsedBytes)))
                .liveThreads(liveThreads)
                .processUptimeSeconds(round(uptimeSeconds))
                .build();
    }

    private ObservabilitySnapshotResponse.NetworkMetrics buildNetworkMetrics(List<RequestTraceEntry> traces) {
        double ingressTotal = requestTraceStore.ingressBytesTotal();
        double egressTotal = requestTraceStore.egressBytesTotal();

        LocalDateTime minuteAgo = LocalDateTime.now().minusMinutes(1);
        long ingressLastMinute = traces.stream()
                .filter(trace -> trace.getTimestamp() != null && trace.getTimestamp().isAfter(minuteAgo))
                .mapToLong(RequestTraceEntry::getRequestBytes)
                .sum();
        long egressLastMinute = traces.stream()
                .filter(trace -> trace.getTimestamp() != null && trace.getTimestamp().isAfter(minuteAgo))
                .mapToLong(RequestTraceEntry::getResponseBytes)
                .sum();

        return ObservabilitySnapshotResponse.NetworkMetrics.builder()
                .ingressBytesTotal(round(ingressTotal))
                .egressBytesTotal(round(egressTotal))
                .ingressKbpsLastMinute(round(bytesPerMinuteToKbps(ingressLastMinute)))
                .egressKbpsLastMinute(round(bytesPerMinuteToKbps(egressLastMinute)))
                .build();
    }

    private ObservabilitySnapshotResponse.EndpointMetrics toEndpointMetrics(Timer timer) {
        HistogramSnapshot snapshot = timer.takeSnapshot();
        Meter.Id id = timer.getId();
        return ObservabilitySnapshotResponse.EndpointMetrics.builder()
                .method(tagOrDefault(id, "method"))
                .path(tagOrDefault(id, "path"))
                .status(tagOrDefault(id, "status"))
                .count(timer.count())
                .avgMs(round(timer.mean(TimeUnit.MILLISECONDS)))
                .p95Ms(round(percentileMs(snapshot, 0.95)))
                .p99Ms(round(percentileMs(snapshot, 0.99)))
                .maxMs(round(timer.max(TimeUnit.MILLISECONDS)))
                .build();
    }

    private ObservabilitySnapshotResponse.RequestTraceDto toTraceDto(RequestTraceEntry trace) {
        return ObservabilitySnapshotResponse.RequestTraceDto.builder()
                .traceId(trace.getTraceId())
                .method(trace.getMethod())
                .path(trace.getPath())
                .status(trace.getStatus())
                .durationMs(trace.getDurationMs())
                .clientIp(trace.getClientIp())
                .requestBytes(trace.getRequestBytes())
                .responseBytes(trace.getResponseBytes())
                .timestamp(trace.getTimestamp())
                .build();
    }

    private double percentileMs(HistogramSnapshot snapshot, double percentile) {
        for (ValueAtPercentile valueAtPercentile : snapshot.percentileValues()) {
            if (Math.abs(valueAtPercentile.percentile() - percentile) < 0.0001D) {
                return valueAtPercentile.value(TimeUnit.MILLISECONDS);
            }
        }
        return 0D;
    }

    private double gaugeValue(String meterName, String... tags) {
        var gauge = tags == null || tags.length == 0
                ? meterRegistry.find(meterName).gauge()
                : meterRegistry.find(meterName).tags(tags).gauge();
        if (gauge == null) {
            return 0D;
        }
        double value = gauge.value();
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return 0D;
        }
        return value;
    }

    private double gaugesSum(String meterName, String... tags) {
        var search = meterRegistry.find(meterName);
        if (tags != null && tags.length > 0) {
            search = search.tags(tags);
        }
        return search.gauges().stream()
                .mapToDouble(gauge -> {
                    double value = gauge.value();
                    if (Double.isNaN(value) || Double.isInfinite(value)) {
                        return 0D;
                    }
                    return value;
                })
                .sum();
    }

    private String tagOrDefault(Meter.Id id, String key) {
        String value = id.getTag(key);
        return value == null || value.isBlank() ? "unknown" : value;
    }

    private ObservabilitySnapshotResponse.LatencyMetrics emptyLatency() {
        return ObservabilitySnapshotResponse.LatencyMetrics.builder()
                .count(0L)
                .avgMs(0D)
                .p50Ms(0D)
                .p95Ms(0D)
                .p99Ms(0D)
                .maxMs(0D)
                .build();
    }

    private double bytesToMb(double bytes) {
        return bytes / (1024D * 1024D);
    }

    private double bytesPerMinuteToKbps(double bytesPerMinute) {
        return (bytesPerMinute / 60D) / 1024D;
    }

    private double round(double value) {
        return Math.round(value * 100D) / 100D;
    }
}
