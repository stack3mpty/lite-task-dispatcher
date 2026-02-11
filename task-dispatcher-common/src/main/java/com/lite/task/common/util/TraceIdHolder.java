package com.lite.task.common.util;

import org.slf4j.MDC;

import java.util.UUID;

/**
 * Trace/task log context holder based on MDC.
 */
public final class TraceIdHolder {

    public static final String TRACE_ID_KEY = "traceId";
    public static final String TASK_ID_KEY = "taskId";

    private TraceIdHolder() {
    }

    public static String bindTraceId(String incomingTraceId) {
        String traceId = normalize(incomingTraceId);
        if (traceId == null) {
            traceId = generateTraceId();
        }
        MDC.put(TRACE_ID_KEY, traceId);
        return traceId;
    }

    public static String getOrCreateTraceId() {
        String traceId = normalize(MDC.get(TRACE_ID_KEY));
        if (traceId == null) {
            traceId = generateTraceId();
            MDC.put(TRACE_ID_KEY, traceId);
        }
        return traceId;
    }

    public static void bindTaskId(String taskId) {
        String normalized = normalize(taskId);
        if (normalized == null) {
            MDC.remove(TASK_ID_KEY);
            return;
        }
        MDC.put(TASK_ID_KEY, normalized);
    }

    public static void clearTaskId() {
        MDC.remove(TASK_ID_KEY);
    }

    public static void clearTraceId() {
        MDC.remove(TRACE_ID_KEY);
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static String generateTraceId() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}
