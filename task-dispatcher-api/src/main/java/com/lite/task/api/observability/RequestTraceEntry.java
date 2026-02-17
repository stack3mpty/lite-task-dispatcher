package com.lite.task.api.observability;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RequestTraceEntry {

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

