package com.lite.task.api.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ObservabilitySnapshotResponse {

    private LocalDateTime generatedAt;
    private LatencyMetrics apiLatency;
    private LatencyMetrics taskLatency;
    private ResourceMetrics resources;
    private NetworkMetrics network;
    private List<EndpointMetrics> endpointMetrics;
    private List<RequestTraceDto> recentTraces;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LatencyMetrics {
        private long count;
        private double avgMs;
        private double p50Ms;
        private double p95Ms;
        private double p99Ms;
        private double maxMs;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ResourceMetrics {
        private double systemCpuUsagePct;
        private double processCpuUsagePct;
        private double heapUsedMb;
        private double heapMaxMb;
        private double heapUsagePct;
        private double nonHeapUsedMb;
        private long liveThreads;
        private double processUptimeSeconds;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NetworkMetrics {
        private double ingressBytesTotal;
        private double egressBytesTotal;
        private double ingressKbpsLastMinute;
        private double egressKbpsLastMinute;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EndpointMetrics {
        private String method;
        private String path;
        private String status;
        private long count;
        private double avgMs;
        private double p95Ms;
        private double p99Ms;
        private double maxMs;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RequestTraceDto {
        private String traceId;
        private String method;
        private String path;
        private int status;
        private long durationMs;
        private String clientIp;
        private long requestBytes;
        private long responseBytes;
        private LocalDateTime timestamp;
    }
}

