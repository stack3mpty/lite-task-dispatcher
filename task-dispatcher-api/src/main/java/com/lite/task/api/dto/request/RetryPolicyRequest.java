package com.lite.task.api.dto.request;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Retry policy request DTO for task definition management.
 */
@Data
public class RetryPolicyRequest {

    @Min(value = 0, message = "maxAttempts must be >= 0")
    private Integer maxAttempts = 3;

    @Min(value = 0, message = "initialDelay must be >= 0")
    private Long initialDelay = 1000L;

    @DecimalMin(value = "1.0", message = "multiplier must be >= 1.0")
    private Double multiplier = 2.0;

    @Min(value = 0, message = "maxDelay must be >= 0")
    private Long maxDelay = 60000L;

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("maxAttempts", maxAttempts != null ? maxAttempts : 3);
        map.put("initialDelay", initialDelay != null ? initialDelay : 1000L);
        map.put("multiplier", multiplier != null ? multiplier : 2.0);
        map.put("maxDelay", maxDelay != null ? maxDelay : 60000L);
        return map;
    }
}
