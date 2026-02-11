package com.lite.task.api.exception;

import com.lite.task.common.exception.ErrorCode;
import com.lite.task.common.exception.TaskException;
import com.lite.task.common.model.Result;
import com.lite.task.common.util.TraceIdHolder;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.http.converter.HttpMessageNotReadableException;

import java.util.stream.Collectors;

/**
 * Global API exception handling.
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(TaskException.class)
    public ResponseEntity<Result<Void>> handleTaskException(TaskException e) {
        HttpStatus status = e.getErrorCode().isServerError() ? HttpStatus.INTERNAL_SERVER_ERROR : HttpStatus.BAD_REQUEST;
        log.warn("Business exception: code={}, message={}", e.getCode(), e.getMessage());
        return ResponseEntity.status(status).body(
                Result.<Void>failure(e.getCode(), e.getMessage())
                        .withTraceId(TraceIdHolder.getOrCreateTraceId())
        );
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Result<Void>> handleValidationException(MethodArgumentNotValidException e) {
        String message = e.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .collect(Collectors.joining("; "));
        return validationResponse(message);
    }

    @ExceptionHandler(BindException.class)
    public ResponseEntity<Result<Void>> handleBindException(BindException e) {
        String message = e.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .collect(Collectors.joining("; "));
        return validationResponse(message);
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<Result<Void>> handleConstraintViolationException(ConstraintViolationException e) {
        String message = e.getConstraintViolations().stream()
                .map(violation -> violation.getPropertyPath() + ": " + violation.getMessage())
                .collect(Collectors.joining("; "));
        return validationResponse(message);
    }

    @ExceptionHandler({MethodArgumentTypeMismatchException.class, HttpMessageNotReadableException.class})
    public ResponseEntity<Result<Void>> handleBadRequest(Exception e) {
        return validationResponse(e.getMessage());
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Result<Void>> handleUnhandledException(Exception e) {
        log.error("Unhandled exception", e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(
                Result.<Void>failure(ErrorCode.SYSTEM_ERROR)
                        .withTraceId(TraceIdHolder.getOrCreateTraceId())
        );
    }

    private ResponseEntity<Result<Void>> validationResponse(String message) {
        String safeMessage = message == null || message.isBlank() ? ErrorCode.VALIDATION_ERROR.getMessage() : message;
        return ResponseEntity.badRequest().body(
                Result.<Void>failure(ErrorCode.VALIDATION_ERROR, safeMessage)
                        .withTraceId(TraceIdHolder.getOrCreateTraceId())
        );
    }
}
