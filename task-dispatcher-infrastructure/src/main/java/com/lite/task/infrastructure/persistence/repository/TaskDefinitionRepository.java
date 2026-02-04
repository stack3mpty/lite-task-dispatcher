package com.lite.task.infrastructure.persistence.repository;

import com.lite.task.domain.task.entity.TaskDefinition;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Task Definition Repository
 *
 * @author lite-task-dispatcher
 */
@Repository
public interface TaskDefinitionRepository extends JpaRepository<TaskDefinition, Long>,
        JpaSpecificationExecutor<TaskDefinition> {

    /**
     * Find by task type
     */
    Optional<TaskDefinition> findByTaskType(String taskType);

    /**
     * Find all enabled task definitions
     */
    List<TaskDefinition> findByStatus(String status);

    /**
     * Find all enabled task definitions
     */
    @Query("SELECT t FROM TaskDefinition t WHERE t.status = 'ENABLED'")
    List<TaskDefinition> findAllEnabled();

    /**
     * Check if task type exists
     */
    boolean existsByTaskType(String taskType);

    /**
     * Update task definition status
     */
    @Modifying
    @Query("UPDATE TaskDefinition t SET t.status = :status WHERE t.id = :id")
    int updateStatus(@Param("id") Long id, @Param("status") String status);

    /**
     * Find by executor type
     */
    List<TaskDefinition> findByExecutorType(String executorType);

    /**
     * Search by task name (case-insensitive)
     */
    @Query("SELECT t FROM TaskDefinition t WHERE LOWER(t.taskName) LIKE LOWER(CONCAT('%', :keyword, '%'))")
    List<TaskDefinition> searchByTaskName(@Param("keyword") String keyword);
}
