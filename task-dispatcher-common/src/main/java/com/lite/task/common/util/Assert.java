package com.lite.task.common.util;

import com.lite.task.common.exception.ErrorCode;
import com.lite.task.common.exception.TaskException;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * Assertion Utility Class
 *
 * Provides validation methods that throw TaskException on failure
 *
 * @author lite-task-dispatcher
 */
public final class Assert {

    private Assert() {
        // Utility class - prevent instantiation
    }

    /**
     * Assert that an object is not null
     */
    public static void notNull(Object obj, String message) {
        if (obj == null) {
            throw new TaskException(ErrorCode.PARAM_MISSING, message);
        }
    }

    /**
     * Assert that an object is not null
     */
    public static void notNull(Object obj, ErrorCode errorCode) {
        if (obj == null) {
            throw new TaskException(errorCode);
        }
    }

    /**
     * Assert that an object is not null
     */
    public static void notNull(Object obj, ErrorCode errorCode, String message) {
        if (obj == null) {
            throw new TaskException(errorCode, message);
        }
    }

    /**
     * Assert that a string is not blank
     */
    public static void notBlank(String str, String message) {
        if (str == null || str.trim().isEmpty()) {
            throw new TaskException(ErrorCode.PARAM_MISSING, message);
        }
    }

    /**
     * Assert that a string is not blank
     */
    public static void notBlank(String str, ErrorCode errorCode) {
        if (str == null || str.trim().isEmpty()) {
            throw new TaskException(errorCode);
        }
    }

    /**
     * Assert that a collection is not empty
     */
    public static void notEmpty(Collection<?> collection, String message) {
        if (collection == null || collection.isEmpty()) {
            throw new TaskException(ErrorCode.PARAM_MISSING, message);
        }
    }

    /**
     * Assert that a collection is not empty
     */
    public static void notEmpty(Collection<?> collection, ErrorCode errorCode) {
        if (collection == null || collection.isEmpty()) {
            throw new TaskException(errorCode);
        }
    }

    /**
     * Assert that a map is not empty
     */
    public static void notEmpty(Map<?, ?> map, String message) {
        if (map == null || map.isEmpty()) {
            throw new TaskException(ErrorCode.PARAM_MISSING, message);
        }
    }

    /**
     * Assert that an array is not empty
     */
    public static void notEmpty(Object[] array, String message) {
        if (array == null || array.length == 0) {
            throw new TaskException(ErrorCode.PARAM_MISSING, message);
        }
    }

    /**
     * Assert that a condition is true
     */
    public static void isTrue(boolean condition, String message) {
        if (!condition) {
            throw new TaskException(ErrorCode.PARAM_INVALID, message);
        }
    }

    /**
     * Assert that a condition is true
     */
    public static void isTrue(boolean condition, ErrorCode errorCode) {
        if (!condition) {
            throw new TaskException(errorCode);
        }
    }

    /**
     * Assert that a condition is true
     */
    public static void isTrue(boolean condition, ErrorCode errorCode, String message) {
        if (!condition) {
            throw new TaskException(errorCode, message);
        }
    }

    /**
     * Assert that a condition is false
     */
    public static void isFalse(boolean condition, String message) {
        if (condition) {
            throw new TaskException(ErrorCode.PARAM_INVALID, message);
        }
    }

    /**
     * Assert that a condition is false
     */
    public static void isFalse(boolean condition, ErrorCode errorCode) {
        if (condition) {
            throw new TaskException(errorCode);
        }
    }

    /**
     * Assert that two objects are equal
     */
    public static void equals(Object expected, Object actual, String message) {
        if (!Objects.equals(expected, actual)) {
            throw new TaskException(ErrorCode.PARAM_INVALID, message);
        }
    }

    /**
     * Assert that a number is positive
     */
    public static void positive(long value, String message) {
        if (value <= 0) {
            throw new TaskException(ErrorCode.PARAM_INVALID, message);
        }
    }

    /**
     * Assert that a number is non-negative
     */
    public static void nonNegative(long value, String message) {
        if (value < 0) {
            throw new TaskException(ErrorCode.PARAM_INVALID, message);
        }
    }

    /**
     * Assert that a value is within range [min, max]
     */
    public static void inRange(long value, long min, long max, String message) {
        if (value < min || value > max) {
            throw new TaskException(ErrorCode.PARAM_INVALID,
                    String.format("%s: value %d not in range [%d, %d]", message, value, min, max));
        }
    }

    /**
     * Assert state condition
     */
    public static void state(boolean condition, String message) {
        if (!condition) {
            throw new TaskException(ErrorCode.TASK_INVALID_STATUS, message);
        }
    }

    /**
     * Assert state condition
     */
    public static void state(boolean condition, ErrorCode errorCode) {
        if (!condition) {
            throw new TaskException(errorCode);
        }
    }

    /**
     * Assert state condition
     */
    public static void state(boolean condition, ErrorCode errorCode, String message) {
        if (!condition) {
            throw new TaskException(errorCode, message);
        }
    }
}
