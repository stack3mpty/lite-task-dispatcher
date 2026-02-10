package com.lite.task.infrastructure.persistence.repository;

import com.lite.task.common.enums.TaskStatus;
import com.lite.task.domain.task.entity.TaskInstance;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Task Instance Repository
 *
 * @author lite-task-dispatcher
 */
@Repository
public interface TaskInstanceRepository extends JpaRepository<TaskInstance, Long>,
        JpaSpecificationExecutor<TaskInstance> {

    /**
     * Find by task ID
     */
    Optional<TaskInstance> findByTaskId(String taskId);

    /**
     * Find by status
     */
    List<TaskInstance> findByStatus(TaskStatus status);

    /**
     * Find by status with pagination
     */
    Page<TaskInstance> findByStatus(TaskStatus status, Pageable pageable);

    /**
     * Find by task type
     */
    List<TaskInstance> findByTaskType(String taskType);

    /**
     * Find by task type with pagination
     */
    Page<TaskInstance> findByTaskType(String taskType, Pageable pageable);

    /**
     * Find by task type and status
     */
    List<TaskInstance> findByTaskTypeAndStatus(String taskType, TaskStatus status);

    /**
     * Find by task type and status with pagination
     */
    Page<TaskInstance> findByTaskTypeAndStatus(String taskType, TaskStatus status, Pageable pageable);

    /**
     * Find pending tasks ready for execution
     */
    @Query("SELECT t FROM TaskInstance t WHERE t.status = 'PENDING' AND t.executeAt <= :now ORDER BY t.priority ASC, t.createdAt ASC")
    List<TaskInstance> findPendingTasksReadyForExecution(@Param("now") LocalDateTime now, Pageable pageable);

    /**
     * Find tasks to retry (RETRYING status)
     */
    @Query("SELECT t FROM TaskInstance t WHERE t.status = 'RETRYING' ORDER BY t.priority ASC, t.updatedAt ASC")
    List<TaskInstance> findTasksToRetry(Pageable pageable);

    /**
     * Find timed out tasks (RUNNING for too long)
     */
    @Query("SELECT t FROM TaskInstance t WHERE t.status = 'RUNNING' AND t.startedAt < :timeout")
    List<TaskInstance> findTimedOutTasks(@Param("timeout") LocalDateTime timeout);

    /**
     * Find delayed tasks ready for execution
     */
    @Query("SELECT t FROM TaskInstance t WHERE t.status = 'PENDING' AND t.executeAt IS NOT NULL AND t.executeAt <= :now ORDER BY t.executeAt ASC")
    List<TaskInstance> findDelayedTasksReady(@Param("now") LocalDateTime now, Pageable pageable);

    /**
     * Update task status
     */
    @Modifying
    @Transactional
    @Query("UPDATE TaskInstance t SET t.status = :status, t.updatedAt = :now WHERE t.taskId = :taskId")
    int updateStatus(@Param("taskId") String taskId, @Param("status") TaskStatus status, @Param("now") LocalDateTime now);

    /**
     * Update task status with CAS (optimistic lock)
     */
    @Modifying
    @Transactional
    @Query("UPDATE TaskInstance t SET t.status = :newStatus, t.updatedAt = :now WHERE t.taskId = :taskId AND t.status = :expectedStatus")
    int updateStatusCas(@Param("taskId") String taskId,
                        @Param("expectedStatus") TaskStatus expectedStatus,
                        @Param("newStatus") TaskStatus newStatus,
                        @Param("now") LocalDateTime now);

    /**
     * Claim task for execution with DB CAS semantics.
     */
    @Modifying
    @Transactional
    @Query("UPDATE TaskInstance t SET t.status = 'RUNNING', t.startedAt = :startedAt, t.executorId = :executorId, t.updatedAt = :startedAt " +
            "WHERE t.taskId = :taskId AND (t.status = 'PENDING' OR t.status = 'RETRYING')")
    int claimForExecution(@Param("taskId") String taskId,
                          @Param("executorId") String executorId,
                          @Param("startedAt") LocalDateTime startedAt);

    /**
     * Count by status
     */
    long countByStatus(TaskStatus status);

    /**
     * Count by task type and status
     */
    long countByTaskTypeAndStatus(String taskType, TaskStatus status);

    /**
     * Check if task ID exists
     */
    boolean existsByTaskId(String taskId);

    /**
     * Find tasks created by user
     */
    Page<TaskInstance> findByCreatedBy(String createdBy, Pageable pageable);

    /**
     * Find tasks in date range
     */
    @Query("SELECT t FROM TaskInstance t WHERE t.createdAt BETWEEN :startTime AND :endTime")
    Page<TaskInstance> findByCreatedAtBetween(@Param("startTime") LocalDateTime startTime,
                                               @Param("endTime") LocalDateTime endTime,
                                               Pageable pageable);

    /**
     * Get task statistics by status
     */
    @Query("SELECT t.status, COUNT(t) FROM TaskInstance t WHERE t.taskType = :taskType GROUP BY t.status")
    List<Object[]> countByStatusGrouped(@Param("taskType") String taskType);

    /**
     * Get overall task statistics
     */
    @Query("SELECT t.status, COUNT(t) FROM TaskInstance t GROUP BY t.status")
    List<Object[]> countAllByStatusGrouped();

    /**
     * Delete old completed tasks
     */
    @Modifying
    @Transactional
    @Query("DELETE FROM TaskInstance t WHERE t.status IN ('SUCCESS', 'DEAD', 'CANCELLED') AND t.finishedAt < :before")
    int deleteOldCompletedTasks(@Param("before") LocalDateTime before);
}
