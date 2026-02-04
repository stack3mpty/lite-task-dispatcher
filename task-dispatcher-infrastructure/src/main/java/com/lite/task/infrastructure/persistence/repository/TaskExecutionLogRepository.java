package com.lite.task.infrastructure.persistence.repository;

import com.lite.task.domain.task.entity.TaskExecutionLog;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Task Execution Log Repository
 *
 * @author lite-task-dispatcher
 */
@Repository
public interface TaskExecutionLogRepository extends JpaRepository<TaskExecutionLog, Long> {

    /**
     * Find by task instance ID
     */
    List<TaskExecutionLog> findByTaskInstanceIdOrderByAttemptNumberDesc(Long taskInstanceId);

    /**
     * Find by task ID
     */
    List<TaskExecutionLog> findByTaskIdOrderByAttemptNumberDesc(String taskId);

    /**
     * Find by task ID with pagination
     */
    Page<TaskExecutionLog> findByTaskId(String taskId, Pageable pageable);

    /**
     * Find latest execution log for a task
     */
    @Query("SELECT l FROM TaskExecutionLog l WHERE l.taskId = :taskId ORDER BY l.attemptNumber DESC")
    List<TaskExecutionLog> findLatestByTaskId(@Param("taskId") String taskId, Pageable pageable);

    /**
     * Count execution attempts for a task
     */
    long countByTaskId(String taskId);

    /**
     * Find by status
     */
    List<TaskExecutionLog> findByStatus(String status);

    /**
     * Find by executor ID
     */
    Page<TaskExecutionLog> findByExecutorId(String executorId, Pageable pageable);

    /**
     * Find logs in date range
     */
    @Query("SELECT l FROM TaskExecutionLog l WHERE l.createdAt BETWEEN :startTime AND :endTime ORDER BY l.createdAt DESC")
    Page<TaskExecutionLog> findByCreatedAtBetween(@Param("startTime") LocalDateTime startTime,
                                                   @Param("endTime") LocalDateTime endTime,
                                                   Pageable pageable);

    /**
     * Get average duration by task type
     */
    @Query("SELECT AVG(l.durationMs) FROM TaskExecutionLog l WHERE l.taskId IN " +
           "(SELECT t.taskId FROM TaskInstance t WHERE t.taskType = :taskType) AND l.status = 'SUCCESS'")
    Double getAverageDurationByTaskType(@Param("taskType") String taskType);

    /**
     * Get success rate by task type
     */
    @Query("SELECT " +
           "COUNT(CASE WHEN l.status = 'SUCCESS' THEN 1 END) * 100.0 / COUNT(*) " +
           "FROM TaskExecutionLog l WHERE l.taskId IN " +
           "(SELECT t.taskId FROM TaskInstance t WHERE t.taskType = :taskType)")
    Double getSuccessRateByTaskType(@Param("taskType") String taskType);

    /**
     * Delete old logs
     */
    @Modifying
    @Query("DELETE FROM TaskExecutionLog l WHERE l.createdAt < :before")
    int deleteOldLogs(@Param("before") LocalDateTime before);

    /**
     * Get execution statistics by executor
     */
    @Query("SELECT l.executorId, COUNT(l), AVG(l.durationMs) FROM TaskExecutionLog l " +
           "WHERE l.createdAt >= :since GROUP BY l.executorId")
    List<Object[]> getExecutorStatistics(@Param("since") LocalDateTime since);
}
