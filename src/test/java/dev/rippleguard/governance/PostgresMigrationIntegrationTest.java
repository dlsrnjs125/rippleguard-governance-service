package dev.rippleguard.governance;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.rippleguard.governance.application.DecisionCaseService;
import dev.rippleguard.governance.application.EventEnvelope;
import dev.rippleguard.governance.application.MockDecisionEvaluator;
import dev.rippleguard.governance.domain.DecisionCaseStatus;
import dev.rippleguard.governance.domain.FinalDecision;
import dev.rippleguard.governance.infrastructure.persistence.DecisionCaseRepository;
import dev.rippleguard.governance.infrastructure.persistence.EvaluationRunEntity;
import dev.rippleguard.governance.infrastructure.persistence.EvaluationRunRepository;
import dev.rippleguard.governance.infrastructure.persistence.OutboxEventEntity;
import dev.rippleguard.governance.infrastructure.persistence.OutboxEventRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@SpringBootTest(properties = {
        "debug=false",
        "rippleguard.kafka.enabled=false",
        "management.health.kafka.enabled=false"
})
class PostgresMigrationIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("rippleguard_governance")
            .withUsername("rippleguard_governance")
            .withPassword("rippleguard_governance");

    @DynamicPropertySource
    static void postgresProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "validate");
    }

    @Autowired
    DataSource dataSource;

    @Autowired
    OutboxEventRepository outbox;

    @Autowired
    DecisionCaseRepository decisionCases;

    @Autowired
    EvaluationRunRepository evaluationRuns;

    @Autowired
    TransactionTemplate transactions;

    @Autowired
    DecisionCaseService service;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void cleanDatabase() {
        jdbc.update("delete from evaluation_run");
        jdbc.update("delete from outbox_event");
        jdbc.update("delete from inbox_event");
        jdbc.update("delete from governance_event_quarantine");
        jdbc.update("delete from decision_case");
    }

    @Test
    void appliesFlywayMigrationOnPostgreSql() throws Exception {
        try (var connection = dataSource.getConnection();
             var statement = connection.createStatement();
             var result = statement.executeQuery("select count(*) from decision_case")) {
            assertThat(result.next()).isTrue();
            assertThat(result.getLong(1)).isGreaterThanOrEqualTo(0);
        }
    }

    @Test
    void claimsOutboxRowsWithPostgreSqlSkipLockedQuery() {
        Instant now = Instant.now();
        UUID eventId = UUID.randomUUID();
        outbox.save(new OutboxEventEntity(
                eventId,
                "governance.review.started.v1",
                "1.1.0",
                UUID.randomUUID(),
                UUID.randomUUID().toString(),
                null,
                "{\"eventType\":\"governance.review.started.v1\"}",
                now
        ));

        var claimed = transactions.execute(status -> outbox.findClaimable(now, 10));

        assertThat(claimed).hasSize(1);
        assertThat(claimed.get(0).getEventId()).isEqualTo(eventId);
    }

    @Test
    void duplicateSubmittedEventRaceCreatesSingleCase() throws Exception {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000091");
        EventEnvelope event = submitted(applicationId);

        var results = runConcurrently(
                () -> {
                    service.handleLoanApplicationSubmitted(event);
                    return null;
                },
                () -> {
                    service.handleLoanApplicationSubmitted(event);
                    return null;
                }
        );

        assertThat(results).filteredOn(Throwable.class::isInstance).isEmpty();
        assertThat(service.getByApplication(applicationId).status()).isEqualTo(DecisionCaseStatus.RESOLVED);
        assertThat(jdbc.queryForObject("select count(*) from decision_case", Long.class)).isEqualTo(1);
    }

    @Test
    void concurrentSameApplicationSamePayloadCreatesSingleCaseAndRecordsBothEvents() throws Exception {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000092");
        EventEnvelope first = submitted(applicationId);
        EventEnvelope second = new EventEnvelope(
                UUID.randomUUID(),
                first.eventType(),
                first.schemaVersion(),
                first.occurredAt(),
                first.producer(),
                first.applicationId(),
                first.caseId(),
                first.evaluationRunId(),
                first.correlationId(),
                first.causationId(),
                first.payload()
        );

        var results = runConcurrently(
                () -> {
                    service.handleLoanApplicationSubmitted(first);
                    return null;
                },
                () -> {
                    service.handleLoanApplicationSubmitted(second);
                    return null;
                }
        );

        assertThat(results).filteredOn(Throwable.class::isInstance).isEmpty();
        assertThat(service.getByApplication(applicationId).status()).isEqualTo(DecisionCaseStatus.RESOLVED);
        assertThat(jdbc.queryForObject("select count(*) from decision_case", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from outbox_event", Long.class)).isEqualTo(4);
        assertThat(jdbc.queryForObject("select count(*) from inbox_event", Long.class)).isEqualTo(2);
    }

    @Test
    void concurrentSameApplicationDifferentPayloadMarksRecalculationRequired() throws Exception {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000093");
        EventEnvelope first = submitted(applicationId, "snapshot-v1");
        EventEnvelope second = submitted(applicationId, "snapshot-v-reject-001");

        var results = runConcurrently(
                () -> {
                    service.handleLoanApplicationSubmitted(first);
                    return null;
                },
                () -> {
                    service.handleLoanApplicationSubmitted(second);
                    return null;
                }
        );

        assertThat(results).filteredOn(Throwable.class::isInstance).isEmpty();
        assertThat(service.getByApplication(applicationId).status()).isEqualTo(DecisionCaseStatus.RECALCULATION_REQUIRED);
        assertThat(jdbc.queryForObject("select count(*) from decision_case", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from inbox_event", Long.class)).isEqualTo(2);
    }

    @Test
    void evaluationRunsAllowSupersedingRunForSameCaseAndLatestLookupReturnsNewestRun() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000094");
        service.handleLoanApplicationSubmitted(submitted(applicationId, "snapshot-v1"));
        var decisionCase = decisionCases.findByApplicationId(applicationId).orElseThrow();
        var firstRun = evaluationRuns.findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(decisionCase.getCaseId())
                .orElseThrow();
        UUID firstRunId = firstRun.getEvaluationRunId();
        FinalDecision firstProposal = firstRun.getProposal();
        Instant secondCreatedAt = firstRun.getCreatedAt().plusSeconds(60);
        EvaluationRunEntity secondRun = new EvaluationRunEntity(
                UUID.randomUUID(),
                decisionCase,
                MockDecisionEvaluator.RULE_VERSION,
                "snapshot-v2",
                UUID.randomUUID(),
                "[]",
                firstRunId,
                secondCreatedAt
        );
        secondRun.start();
        secondRun.complete(FinalDecision.REJECT, new BigDecimal("0.6100"), "[\"RECALCULATED\"]", secondCreatedAt);

        evaluationRuns.saveAndFlush(secondRun);

        var reloadedFirstRun = evaluationRuns.findById(firstRunId).orElseThrow();
        var latestRun = evaluationRuns.findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(decisionCase.getCaseId())
                .orElseThrow();
        assertThat(evaluationRuns.count()).isEqualTo(2);
        assertThat(reloadedFirstRun.getProposal()).isEqualTo(firstProposal);
        assertThat(latestRun.getEvaluationRunId()).isEqualTo(secondRun.getEvaluationRunId());
        assertThat(latestRun.getSupersedesRunId()).isEqualTo(firstRunId);
    }

    private List<Object> runConcurrently(ThrowingSupplier<?> first, ThrowingSupplier<?> second) throws Exception {
        var executor = Executors.newFixedThreadPool(2);
        try {
            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch start = new CountDownLatch(1);
            Future<Object> left = executor.submit(() -> callAfterStart(first, ready, start));
            Future<Object> right = executor.submit(() -> callAfterStart(second, ready, start));
            ready.await();
            start.countDown();
            List<Object> results = new ArrayList<>();
            results.add(left.get());
            results.add(right.get());
            return results;
        } finally {
            executor.shutdownNow();
        }
    }

    private Object callAfterStart(ThrowingSupplier<?> supplier, CountDownLatch ready, CountDownLatch start) {
        ready.countDown();
        try {
            start.await();
            return supplier.get();
        } catch (Exception exception) {
            return exception;
        }
    }

    private EventEnvelope submitted(UUID applicationId) {
        return submitted(applicationId, "snapshot-v1");
    }

    private EventEnvelope submitted(UUID applicationId, String snapshotVersion) {
        return new EventEnvelope(
                UUID.randomUUID(),
                "loan.application.submitted.v1",
                "1.1.0",
                Instant.now(),
                "loan-service",
                applicationId,
                applicationId.toString(),
                null,
                applicationId.toString(),
                null,
                objectMapper.valueToTree(Map.of(
                        "applicationId", applicationId.toString(),
                        "applicantId", "customer-42",
                        "inputSnapshotVersion", snapshotVersion,
                        "submittedAt", Instant.now().toString(),
                        "submissionChannel", "WEB"
                ))
        );
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }
}
