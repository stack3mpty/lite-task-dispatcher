package com.lite.task.api.filter;

import com.lite.task.api.observability.RequestTraceStore;
import com.lite.task.common.util.TraceIdHolder;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingRequestWrapper;
import org.springframework.web.util.ContentCachingResponseWrapper;
import org.springframework.web.servlet.HandlerMapping;

import java.io.IOException;

/**
 * Captures interface-level request traces for observability dashboard.
 */
@Component
@RequiredArgsConstructor
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class RequestTraceFilter extends OncePerRequestFilter {

    private static final long DEFAULT_UNKNOWN_SIZE = 0L;
    private final RequestTraceStore requestTraceStore;

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String uri = request.getRequestURI();
        return uri == null || !uri.startsWith("/api/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        ContentCachingRequestWrapper requestWrapper = new ContentCachingRequestWrapper(request);
        ContentCachingResponseWrapper responseWrapper = new ContentCachingResponseWrapper(response);

        long startNs = System.nanoTime();
        int status = 500;
        try {
            filterChain.doFilter(requestWrapper, responseWrapper);
            status = responseWrapper.getStatus();
        } finally {
            long durationMs = (System.nanoTime() - startNs) / 1_000_000L;
            String traceId = TraceIdHolder.getOrCreateTraceId();
            String path = resolvePathPattern(requestWrapper);
            long requestBytes = extractRequestBytes(requestWrapper);
            long responseBytes = extractResponseBytes(responseWrapper);
            String clientIp = resolveClientIp(requestWrapper);

            requestTraceStore.record(
                    traceId,
                    requestWrapper.getMethod(),
                    path,
                    status,
                    durationMs,
                    clientIp,
                    requestBytes,
                    responseBytes
            );
            responseWrapper.copyBodyToResponse();
        }
    }

    private String resolvePathPattern(HttpServletRequest request) {
        Object pattern = request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
        if (pattern != null) {
            return String.valueOf(pattern);
        }
        return request.getRequestURI();
    }

    private long extractRequestBytes(ContentCachingRequestWrapper requestWrapper) {
        byte[] body = requestWrapper.getContentAsByteArray();
        if (body != null && body.length > 0) {
            return body.length;
        }
        long contentLength = requestWrapper.getContentLengthLong();
        if (contentLength > 0) {
            return contentLength;
        }
        return DEFAULT_UNKNOWN_SIZE;
    }

    private long extractResponseBytes(ContentCachingResponseWrapper responseWrapper) {
        byte[] body = responseWrapper.getContentAsByteArray();
        return body != null ? body.length : DEFAULT_UNKNOWN_SIZE;
    }

    private String resolveClientIp(HttpServletRequest request) {
        String forwarded = request.getHeader("X-Forwarded-For");
        if (forwarded != null && !forwarded.isBlank()) {
            int commaIndex = forwarded.indexOf(',');
            return commaIndex > 0 ? forwarded.substring(0, commaIndex).trim() : forwarded.trim();
        }
        return request.getRemoteAddr() != null ? request.getRemoteAddr() : "unknown";
    }
}
