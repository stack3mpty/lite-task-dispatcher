package com.lite.task.common.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.lite.task.common.exception.ErrorCode;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * Unified API Response
 *
 * @param <T> Response data type
 * @author lite-task-dispatcher
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Result<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Response code (0 = success)
     */
    private int code;

    /**
     * Response message
     */
    private String message;

    /**
     * Response data
     */
    private T data;

    /**
     * Timestamp
     */
    private LocalDateTime timestamp;

    /**
     * Trace ID for distributed tracing
     */
    private String traceId;

    protected Result() {
        this.timestamp = LocalDateTime.now();
    }

    protected Result(int code, String message, T data) {
        this.code = code;
        this.message = message;
        this.data = data;
        this.timestamp = LocalDateTime.now();
    }

    /**
     * Success response without data
     */
    public static <T> Result<T> success() {
        return new Result<>(ErrorCode.SUCCESS.getCode(), ErrorCode.SUCCESS.getMessage(), null);
    }

    /**
     * Success response with data
     */
    public static <T> Result<T> success(T data) {
        return new Result<>(ErrorCode.SUCCESS.getCode(), ErrorCode.SUCCESS.getMessage(), data);
    }

    /**
     * Success response with custom message
     */
    public static <T> Result<T> success(String message, T data) {
        return new Result<>(ErrorCode.SUCCESS.getCode(), message, data);
    }

    /**
     * Failure response with error code
     */
    public static <T> Result<T> failure(ErrorCode errorCode) {
        return new Result<>(errorCode.getCode(), errorCode.getMessage(), null);
    }

    /**
     * Failure response with error code and custom message
     */
    public static <T> Result<T> failure(ErrorCode errorCode, String message) {
        return new Result<>(errorCode.getCode(), message, null);
    }

    /**
     * Failure response with code and message
     */
    public static <T> Result<T> failure(int code, String message) {
        return new Result<>(code, message, null);
    }

    /**
     * Check if response is successful
     */
    public boolean isSuccess() {
        return this.code == ErrorCode.SUCCESS.getCode();
    }

    /**
     * Set trace ID and return self for chaining
     */
    public Result<T> withTraceId(String traceId) {
        this.traceId = traceId;
        return this;
    }
}
