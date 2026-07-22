package dev.rippleguard.governance.infrastructure.persistence;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, UUID> {
    @Query(
            value = """
                    select *
                    from outbox_event
                    where (
                        (
                            status in ('PENDING', 'FAILED')
                            and next_attempt_at <= :now
                        )
                        or (
                            status = 'PROCESSING'
                            and lease_until < :now
                        )
                    )
                    and not exists (
                        select 1
                        from outbox_event cause
                        where cause.event_id = outbox_event.causation_id
                          and cause.status <> 'PUBLISHED'
                    )
                    order by created_at
                    for update skip locked
                    limit :batchSize
                    """,
            nativeQuery = true
    )
    List<OutboxEventEntity> findClaimable(@Param("now") Instant now, @Param("batchSize") int batchSize);

    @Modifying
    @Query(
            value = """
                    update outbox_event
                    set status = 'PUBLISHED',
                        published_at = :now,
                        processing_started_at = null,
                        lease_until = null,
                        claimed_by = null,
                        claim_token = null,
                        updated_at = :now
                    where event_id = :eventId
                      and status = 'PROCESSING'
                      and claim_token = :claimToken
                    """,
            nativeQuery = true
    )
    int markPublishedIfClaimed(@Param("eventId") UUID eventId,
                               @Param("claimToken") UUID claimToken,
                               @Param("now") Instant now);

    @Modifying
    @Query(
            value = """
                    update outbox_event
                    set attempts = attempts + 1,
                        status = 'FAILED',
                        next_attempt_at = :nextAttemptAt,
                        processing_started_at = null,
                        lease_until = null,
                        claimed_by = null,
                        claim_token = null,
                        updated_at = :now
                    where event_id = :eventId
                      and status = 'PROCESSING'
                      and claim_token = :claimToken
                    """,
            nativeQuery = true
    )
    int markFailedIfClaimed(@Param("eventId") UUID eventId,
                            @Param("claimToken") UUID claimToken,
                            @Param("now") Instant now,
                            @Param("nextAttemptAt") Instant nextAttemptAt);
}
