package com.lite.task.infrastructure.persistence.repository;

import com.lite.task.domain.task.entity.TaskOutboxEvent;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Outbox repository for reliable task dispatch.
 */
@Repository
public interface TaskOutboxEventRepository extends JpaRepository<TaskOutboxEvent, Long> {

    Optional<TaskOutboxEvent> findByTaskId(String taskId);

    @Query("SELECT e FROM TaskOutboxEvent e " +
            "WHERE e.status IN :statuses AND (e.nextRetryAt IS NULL OR e.nextRetryAt <= :now) " +
            "ORDER BY e.id ASC")
    List<TaskOutboxEvent> findDueEvents(@Param("statuses") Collection<String> statuses,
                                        @Param("now") LocalDateTime now,
                                        Pageable pageable);
}

