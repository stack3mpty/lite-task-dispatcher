package com.lite.task.core.executor.impl;

import com.lite.task.common.util.JsonUtils;
import com.lite.task.core.executor.AbstractTaskExecutor;
import com.lite.task.core.executor.TaskResult;
import com.lite.task.domain.task.valueobject.TaskContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * HTTP Callback Executor
 *
 * Executes HTTP callbacks to external systems
 *
 * Parameters:
 * - url: Target URL (required)
 * - method: HTTP method (GET/POST/PUT, default: POST)
 * - headers: Custom headers (optional)
 * - body: Request body (optional)
 * - timeout: Timeout in seconds (optional, default: 30)
 *
 * @author lite-task-dispatcher
 */
@Slf4j
@Component
public class HttpCallbackExecutor extends AbstractTaskExecutor {

    private static final String TYPE = "HTTP_CALLBACK";
    private final WebClient webClient;

    public HttpCallbackExecutor(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    protected void validate(TaskContext context) {
        super.validate(context);

        String url = context.getParam("url");
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("Parameter 'url' is required for HTTP callback");
        }
    }

    @Override
    protected TaskResult doExecute(TaskContext context) {
        String url = context.getParam("url");
        String method = context.getParam("method", "POST");
        Map<String, String> headers = context.getParam("headers");
        Object body = context.getParam("body");
        int timeout = context.getParam("timeout", 30);

        log.info("Executing HTTP callback: url={}, method={}, taskId={}",
                url, method, context.getTaskId());

        try {
            WebClient.RequestBodySpec requestSpec = createRequest(method, url);

            // Add custom headers
            if (headers != null) {
                headers.forEach(requestSpec::header);
            }

            // Add task context headers
            requestSpec.header("X-Task-Id", context.getTaskId());
            requestSpec.header("X-Task-Type", context.getTaskType());
            requestSpec.header("X-Attempt-Number", String.valueOf(context.getAttemptNumber()));

            // Build response spec
            WebClient.ResponseSpec responseSpec;
            if (body != null && ("POST".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method))) {
                String jsonBody = body instanceof String ? (String) body : JsonUtils.toJson(body);
                responseSpec = requestSpec.bodyValue(jsonBody).retrieve();
            } else {
                responseSpec = requestSpec.retrieve();
            }

            // Execute request
            String response = responseSpec
                    .onStatus(status -> status.isError(), clientResponse -> {
                        return clientResponse.bodyToMono(String.class)
                                .flatMap(errorBody -> Mono.error(new RuntimeException(
                                        "HTTP error: " + clientResponse.statusCode() + ", body: " + errorBody)));
                    })
                    .bodyToMono(String.class)
                    .timeout(Duration.ofSeconds(timeout))
                    .block();

            log.info("HTTP callback successful: url={}, taskId={}", url, context.getTaskId());

            Map<String, Object> resultData = new HashMap<>();
            resultData.put("url", url);
            resultData.put("method", method);
            resultData.put("response", response);

            return TaskResult.success("HTTP callback executed successfully", resultData);

        } catch (Exception e) {
            log.error("HTTP callback failed: url={}, taskId={}, error={}",
                    url, context.getTaskId(), e.getMessage());
            throw e;
        }
    }

    private WebClient.RequestBodySpec createRequest(String method, String url) {
        return switch (method.toUpperCase()) {
            case "GET" -> (WebClient.RequestBodySpec) webClient.get().uri(url);
            case "POST" -> webClient.post().uri(url);
            case "PUT" -> webClient.put().uri(url);
            case "DELETE" -> (WebClient.RequestBodySpec) webClient.delete().uri(url);
            case "PATCH" -> webClient.patch().uri(url);
            default -> throw new IllegalArgumentException("Unsupported HTTP method: " + method);
        };
    }

    @Override
    protected boolean isRetryableException(Exception exception) {
        String message = exception.getMessage();
        if (message != null) {
            // 4xx errors are typically non-retryable (client errors)
            if (message.contains("4") && message.contains("HTTP error")) {
                // Except for 429 Too Many Requests
                if (!message.contains("429")) {
                    return false;
                }
            }
        }
        return super.isRetryableException(exception);
    }
}
