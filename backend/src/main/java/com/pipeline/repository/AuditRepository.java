package com.pipeline.repository;

import com.pipeline.model.AuditEvent;
import com.pipeline.model.EventStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface AuditRepository extends JpaRepository<AuditEvent, Long> {

    Optional<AuditEvent> findByEventId(String eventId);

    List<AuditEvent> findTop50ByOrderByCreatedAtDesc();

    Page<AuditEvent> findAllByOrderByCreatedAtDesc(Pageable pageable);

    Page<AuditEvent> findByStatusOrderByCreatedAtDesc(EventStatus status, Pageable pageable);

    @Query("SELECT a FROM AuditEvent a WHERE " +
           "(LOWER(a.eventId) LIKE LOWER(CONCAT('%', :query, '%')) " +
           "OR LOWER(a.eventType) LIKE LOWER(CONCAT('%', :query, '%')) " +
           "OR LOWER(a.sourceService) LIKE LOWER(CONCAT('%', :query, '%')) " +
           "OR LOWER(CAST(a.payload AS string)) LIKE LOWER(CONCAT('%', :query, '%'))) " +
           "AND (:status IS NULL OR a.status = :status) " +
           "ORDER BY a.createdAt DESC")
    Page<AuditEvent> searchEvents(@Param("query") String query,
                                  @Param("status") EventStatus status,
                                  Pageable pageable);

    long countByStatus(EventStatus status);

    @Query("SELECT a.eventType, COUNT(a) FROM AuditEvent a GROUP BY a.eventType")
    List<Object[]> countByEventType();

    @Query("SELECT AVG(a.processingLatencyMs) FROM AuditEvent a WHERE a.processingLatencyMs IS NOT NULL")
    Double getAverageProcessingLatency();
}
