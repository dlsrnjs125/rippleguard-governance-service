package dev.rippleguard.governance.infrastructure.kafka;

import dev.rippleguard.governance.infrastructure.persistence.OutboxEventEntity;
import dev.rippleguard.governance.infrastructure.persistence.OutboxEventRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Component
@ConditionalOnProperty(prefix = "rippleguard.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outbox;
    private final KafkaOperations<String, String> kafka;
    private final Clock clock;
    private final TransactionTemplate transactions;
    private final OutboxProperties properties;

    public OutboxPublisher(OutboxEventRepository outbox,
                           KafkaOperations<String, String> kafka,
                           Clock clock,
                           TransactionTemplate transactions,
                           OutboxProperties properties) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.clock = clock;
        this.transactions = transactions;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${OUTBOX_PUBLISHER_DELAY_MS:5000}")
    public void publishPending() {
        List<OutboxEventEntity> events = claimBatch();
        for (OutboxEventEntity event : events) {
            try {
                kafka.send(event.getEventType(), event.getAggregateId().toString(), event.getPayload()).get();
                markPublished(event);
                log.info("Published governance outbox event eventId={} eventType={}", event.getEventId(), event.getEventType());
            } catch (Exception exception) {
                markFailed(event);
                log.warn("Governance outbox publish failed eventId={} eventType={} reason={}",
                        event.getEventId(), event.getEventType(), exception.toString());
            }
        }
    }

    private List<OutboxEventEntity> claimBatch() {
        return transactions.execute(status -> {
            Instant now = clock.instant();
            List<OutboxEventEntity> events = outbox.findClaimable(now, properties.batchSize());
            Instant leaseUntil = now.plusSeconds(properties.leaseSeconds());
            events.forEach(event -> event.markProcessing(now, leaseUntil, properties.instanceId(), UUID.randomUUID()));
            return List.copyOf(events);
        });
    }

    private void markPublished(OutboxEventEntity event) {
        transactions.executeWithoutResult(status -> {
            int updated = outbox.markPublishedIfClaimed(event.getEventId(), event.getClaimToken(), clock.instant());
            if (updated == 0) {
                log.info("Skipped stale governance outbox publish result eventId={}", event.getEventId());
            }
        });
    }

    private void markFailed(OutboxEventEntity event) {
        transactions.executeWithoutResult(status -> {
            Instant now = clock.instant();
            Instant nextAttemptAt = now.plusSeconds(Math.min(300, 5L * (event.getAttempts() + 1)));
            int updated = outbox.markFailedIfClaimed(event.getEventId(), event.getClaimToken(), now, nextAttemptAt);
            if (updated == 0) {
                log.info("Skipped stale governance outbox failure result eventId={}", event.getEventId());
            }
        });
    }
}
