package dev.rippleguard.governance;

import static org.assertj.core.api.Assertions.assertThat;

import dev.rippleguard.governance.domain.OutboxStatus;
import dev.rippleguard.governance.infrastructure.kafka.OutboxProperties;
import dev.rippleguard.governance.infrastructure.kafka.OutboxPublisher;
import dev.rippleguard.governance.infrastructure.persistence.OutboxEventEntity;
import dev.rippleguard.governance.infrastructure.persistence.OutboxEventRepository;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

class OutboxPublisherTest {
    private final Clock clock = Clock.fixed(Instant.parse("2026-01-15T00:00:00Z"), ZoneOffset.UTC);

    @org.junit.jupiter.api.Test
    void publishesClaimedEventToEventTypeTopicAndMarksPublishedWithClaimToken() {
        OutboxEventEntity event = event("loan.decision.commanded.v1");
        RecordingOutbox outbox = new RecordingOutbox(List.of(event));
        RecordingKafka kafka = new RecordingKafka(false);

        publisher(outbox.repository(), kafka.operations()).publishPending();

        assertThat(kafka.topic).isEqualTo(event.getEventType());
        assertThat(kafka.key).isEqualTo(event.getAggregateId().toString());
        assertThat(kafka.payload).isEqualTo(event.getPayload());
        assertThat(outbox.publishedEventId).isEqualTo(event.getEventId());
        assertThat(outbox.publishedClaimToken).isEqualTo(event.getClaimToken());
        assertThat(outbox.publishedAt).isEqualTo(clock.instant());
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PROCESSING);
        assertThat(event.getClaimToken()).isNotNull();
    }

    @org.junit.jupiter.api.Test
    void marksFailedWithFencedClaimWhenKafkaSendFails() {
        OutboxEventEntity event = event("agent.evaluation.completed.v1");
        RecordingOutbox outbox = new RecordingOutbox(List.of(event));
        RecordingKafka kafka = new RecordingKafka(true);

        publisher(outbox.repository(), kafka.operations()).publishPending();

        assertThat(outbox.failedEventId).isEqualTo(event.getEventId());
        assertThat(outbox.failedClaimToken).isEqualTo(event.getClaimToken());
        assertThat(outbox.failedAt).isEqualTo(clock.instant());
        assertThat(outbox.nextAttemptAt).isEqualTo(clock.instant().plusSeconds(5));
    }

    private OutboxPublisher publisher(OutboxEventRepository outbox, KafkaOperations<String, String> kafka) {
        return new OutboxPublisher(outbox, kafka, clock, transactions(), new OutboxProperties(10, 60, "test-instance"));
    }

    private TransactionTemplate transactions() {
        return new TransactionTemplate(new PlatformTransactionManager() {
            @Override
            public TransactionStatus getTransaction(TransactionDefinition definition) {
                return new SimpleTransactionStatus();
            }

            @Override
            public void commit(TransactionStatus status) {
            }

            @Override
            public void rollback(TransactionStatus status) {
            }
        });
    }

    private OutboxEventEntity event(String eventType) {
        return new OutboxEventEntity(
                UUID.randomUUID(),
                eventType,
                "1.1.0",
                UUID.randomUUID(),
                UUID.randomUUID().toString(),
                null,
                "{\"eventType\":\"" + eventType + "\"}",
                clock.instant()
        );
    }

    private final class RecordingOutbox implements InvocationHandler {
        private final List<OutboxEventEntity> claimable;
        private UUID publishedEventId;
        private UUID publishedClaimToken;
        private Instant publishedAt;
        private UUID failedEventId;
        private UUID failedClaimToken;
        private Instant failedAt;
        private Instant nextAttemptAt;

        private RecordingOutbox(List<OutboxEventEntity> claimable) {
            this.claimable = claimable;
        }

        private OutboxEventRepository repository() {
            return (OutboxEventRepository) Proxy.newProxyInstance(
                    OutboxEventRepository.class.getClassLoader(),
                    new Class<?>[]{OutboxEventRepository.class},
                    this
            );
        }

        @Override
        public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) {
            return switch (method.getName()) {
                case "findClaimable" -> claimable;
                case "markPublishedIfClaimed" -> {
                    publishedEventId = (UUID) args[0];
                    publishedClaimToken = (UUID) args[1];
                    publishedAt = (Instant) args[2];
                    yield 1;
                }
                case "markFailedIfClaimed" -> {
                    failedEventId = (UUID) args[0];
                    failedClaimToken = (UUID) args[1];
                    failedAt = (Instant) args[2];
                    nextAttemptAt = (Instant) args[3];
                    yield 1;
                }
                case "toString" -> "RecordingOutbox";
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == args[0];
                default -> throw new UnsupportedOperationException(method.getName());
            };
        }
    }

    private final class RecordingKafka implements InvocationHandler {
        private final boolean fail;
        private String topic;
        private String key;
        private String payload;

        private RecordingKafka(boolean fail) {
            this.fail = fail;
        }

        @SuppressWarnings("unchecked")
        private KafkaOperations<String, String> operations() {
            return (KafkaOperations<String, String>) Proxy.newProxyInstance(
                    KafkaOperations.class.getClassLoader(),
                    new Class<?>[]{KafkaOperations.class},
                    this
            );
        }

        @Override
        public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) {
            return switch (method.getName()) {
                case "send" -> send(args);
                case "toString" -> "RecordingKafka";
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == args[0];
                default -> throw new UnsupportedOperationException(method.getName());
            };
        }

        private CompletableFuture<?> send(Object[] args) {
            topic = (String) args[0];
            key = (String) args[1];
            payload = (String) args[2];
            if (fail) {
                return CompletableFuture.failedFuture(new RuntimeException("kafka unavailable"));
            }
            return CompletableFuture.completedFuture(null);
        }
    }
}
