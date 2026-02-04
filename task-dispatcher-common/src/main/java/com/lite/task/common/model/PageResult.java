package com.lite.task.common.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Collections;
import java.util.List;

/**
 * Paginated Response
 *
 * @param <T> List element type
 * @author lite-task-dispatcher
 */
@Data
@EqualsAndHashCode(callSuper = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PageResult<T> extends Result<List<T>> {

    /**
     * Current page number (1-based)
     */
    private int page;

    /**
     * Page size
     */
    private int size;

    /**
     * Total elements
     */
    private long total;

    /**
     * Total pages
     */
    private int totalPages;

    /**
     * Has next page
     */
    private boolean hasNext;

    /**
     * Has previous page
     */
    private boolean hasPrevious;

    private PageResult() {
        super();
    }

    private PageResult(List<T> data, int page, int size, long total) {
        super();
        this.setCode(0);
        this.setMessage("Success");
        this.setData(data);
        this.page = page;
        this.size = size;
        this.total = total;
        this.totalPages = size > 0 ? (int) Math.ceil((double) total / size) : 0;
        this.hasNext = page < totalPages;
        this.hasPrevious = page > 1;
    }

    /**
     * Create paginated result
     *
     * @param data  List of data
     * @param page  Current page (1-based)
     * @param size  Page size
     * @param total Total count
     */
    public static <T> PageResult<T> of(List<T> data, int page, int size, long total) {
        return new PageResult<>(data, page, size, total);
    }

    /**
     * Create empty paginated result
     */
    public static <T> PageResult<T> empty(int page, int size) {
        return new PageResult<>(Collections.emptyList(), page, size, 0);
    }

    /**
     * Get offset for database query
     */
    public int getOffset() {
        return (page - 1) * size;
    }
}
