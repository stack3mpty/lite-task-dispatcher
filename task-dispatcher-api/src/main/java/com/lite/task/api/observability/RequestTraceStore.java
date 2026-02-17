package com.lite.task.api.observability;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * In-memory rolling store for recent HTTP request traces.
 */
@Component
@RequiredArgsConstructor
public class RequestTraceStore {

    private final MeterRegistry meterRegistry;
    private final Deque<RequestTraceEntry> traces = new ConcurrentLinkedDeque<>();
    private final LongAdder ingressBytes = new LongAdder();
    private final LongAdder egressBytes = new LongAdder();

    @Value("${task.observability.trace.max-records:500}")
    private int maxRecords;

    public void record(String traceId,
                       String method,
                       String path,
                       int status,
                       long durationMs,
                       String clientIp,
                       long requestBytes,
                       long responseBytes) {
        RequestTraceEntry entry = RequestTraceEntry.builder()
                .traceId(traceId)
                .method(method)
                .path(path)
                .status(status)
                .durationMs(Math.max(durationMs, 0))
                .clientIp(clientIp)
                .requestBytes(Math.max(requestBytes, 0))
                .responseBytes(Math.max(responseBytes, 0))
                .timestamp(LocalDateTime.now())
                .build();

        traces.addFirst(entry);
        trimOverflow();

        ingressBytes.add(entry.getRequestBytes());
        egressBytes.add(entry.getResponseBytes());

        meterRegistry.timer("api.request.duration")
                .record(entry.getDurationMs(), TimeUnit.MILLISECONDS);
        meterRegistry.timer(
                "api.request.duration.endpoint",
                "method", safeTag(method),
                "path", safeTag(path),
                "status", String.valueOf(status)
        ).record(entry.getDurationMs(), TimeUnit.MILLISECONDS);
        meterRegistry.counter(
                "api.network.ingress.bytes",
                "method", safeTag(method),
                "path", safeTag(path)
        ).increment(entry.getRequestBytes());
        meterRegistry.counter(
                "api.network.egress.bytes",
                "method", safeTag(method),
                "path", safeTag(path)
        ).increment(entry.getResponseBytes());
    }

    public List<RequestTraceEntry> recent(int limit) {
        if (limit <= 0) {
            return List.of();
        }
        List<RequestTraceEntry> result = new ArrayList<>(Math.min(limit, traces.size()));
        int count = 0;
        for (RequestTraceEntry trace : traces) {
            result.add(trace);
            count++;
            if (count >= limit) {
                break;
            }
        }
        return result;
    }

    public long ingressBytesTotal() {
        return ingressBytes.sum();
    }

    public long egressBytesTotal() {
        return egressBytes.sum();
    }

    private void trimOverflow() {
        int safeMax = Math.max(maxRecords, 100);
        while (traces.size() > safeMax) {
            traces.pollLast();
        }
    }

    private String safeTag(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        return value;
    }
}

