package dev.rippleguard.governance.infrastructure.loan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.rippleguard.governance.application.LoanFeatureSnapshotAccessDeniedException;
import dev.rippleguard.governance.application.LoanFeatureSnapshotNotFoundException;
import dev.rippleguard.governance.application.LoanFeatureSnapshotTimeoutException;
import dev.rippleguard.governance.application.Phase2FeatureSnapshot;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RestLoanFeatureSnapshotClientTest {
    private static final UUID APPLICATION_ID = UUID.fromString("10000000-0000-4000-8000-000000002001");
    private static final String SNAPSHOT_VERSION = "v1";
    private static final String SERVICE_TOKEN = "snapshot-token";

    private HttpServer server;
    private ExecutorService executor;
    private RestLoanFeatureSnapshotClient client;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        executor = Executors.newSingleThreadExecutor();
        server.setExecutor(executor);
        server.start();
        String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
        client = new RestLoanFeatureSnapshotClient(new LoanFeatureSnapshotProperties(
                baseUrl,
                "/internal/api/v1/loan-applications/{applicationId}/phase2-feature-snapshots/{snapshotVersion}",
                SERVICE_TOKEN,
                Duration.ofMillis(100),
                Duration.ofMillis(150)
        ));
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
        executor.shutdownNow();
    }

    @Test
    void getsFeatureSnapshotByReferenceWithPathAndServiceToken() {
        AtomicReference<String> path = new AtomicReference<>();
        AtomicReference<String> token = new AtomicReference<>();
        server.createContext("/internal/api/v1/loan-applications/" + APPLICATION_ID
                + "/phase2-feature-snapshots/" + SNAPSHOT_VERSION, exchange -> {
            path.set(exchange.getRequestURI().getPath());
            token.set(exchange.getRequestHeaders().getFirst("X-Internal-Service-Token"));
            respond(exchange, 200, successJson());
        });

        Phase2FeatureSnapshot snapshot = client.getByReference(APPLICATION_ID, SNAPSHOT_VERSION);

        assertThat(path.get()).isEqualTo("/internal/api/v1/loan-applications/" + APPLICATION_ID
                + "/phase2-feature-snapshots/" + SNAPSHOT_VERSION);
        assertThat(token.get()).isEqualTo(SERVICE_TOKEN);
        assertThat(snapshot.schemaVersion()).isEqualTo("1.0.0");
        assertThat(snapshot.snapshotId()).isEqualTo(UUID.fromString("20000000-0000-4000-8000-000000002001"));
        assertThat(snapshot.applicationId()).isEqualTo(APPLICATION_ID);
        assertThat(snapshot.snapshotVersion()).isEqualTo(SNAPSHOT_VERSION);
        assertThat(snapshot.snapshotReference().path("referenceType").asText()).isEqualTo("MATERIALIZED_FEATURES");
        assertThat(snapshot.featurePayloadDigest())
                .isEqualTo("sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        assertThat(snapshot.featurePayload().path("features").path("annualIncome").asInt()).isEqualTo(72000000);
    }

    @Test
    void mapsHttp404ToNotFoundException() {
        server.createContext("/internal/api/v1/loan-applications/" + APPLICATION_ID
                + "/phase2-feature-snapshots/" + SNAPSHOT_VERSION,
                exchange -> respond(exchange, 404, "{}"));

        assertThatThrownBy(() -> client.getByReference(APPLICATION_ID, SNAPSHOT_VERSION))
                .isInstanceOf(LoanFeatureSnapshotNotFoundException.class);
    }

    @Test
    void mapsReadTimeoutToTimeoutException() {
        server.createContext("/internal/api/v1/loan-applications/" + APPLICATION_ID
                + "/phase2-feature-snapshots/" + SNAPSHOT_VERSION, exchange -> {
            try {
                Thread.sleep(500);
                respond(exchange, 200, successJson());
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        });

        assertThatThrownBy(() -> client.getByReference(APPLICATION_ID, SNAPSHOT_VERSION))
                .isInstanceOf(LoanFeatureSnapshotTimeoutException.class);
    }

    @Test
    void mapsHttp401ToAccessDeniedException() {
        server.createContext("/internal/api/v1/loan-applications/" + APPLICATION_ID
                + "/phase2-feature-snapshots/" + SNAPSHOT_VERSION,
                exchange -> respond(exchange, 401, "{}"));

        assertThatThrownBy(() -> client.getByReference(APPLICATION_ID, SNAPSHOT_VERSION))
                .isInstanceOf(LoanFeatureSnapshotAccessDeniedException.class);
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private static String successJson() {
        return """
                {
                  "schemaVersion": "1.0.0",
                  "snapshotId": "20000000-0000-4000-8000-000000002001",
                  "applicationId": "10000000-0000-4000-8000-000000002001",
                  "snapshotVersion": "v1",
                  "snapshotSchemaVersion": "1.0.0",
                  "featureSchemaVersion": "phase-2-loan-features.v1.0.0",
                  "snapshotReference": {
                    "schemaVersion": "1.0.0",
                    "snapshotId": "20000000-0000-4000-8000-000000002001",
                    "snapshotVersion": "v1",
                    "snapshotSchemaVersion": "1.0.0",
                    "snapshotCreatedAt": "2026-07-21T10:00:00Z",
                    "digestAlgorithm": "sha256",
                    "snapshotDigest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "snapshotReference": "snapshot://loan-feature/10000000-0000-4000-8000-000000002001/v1",
                    "referenceType": "MATERIALIZED_FEATURES"
                  },
                  "featurePayload": {
                    "schemaVersion": "1.0.0",
                    "featureSchemaVersion": "phase-2-loan-features.v1.0.0",
                    "features": {
                      "annualIncome": 72000000,
                      "monthlyIncomeMean": 6000000,
                      "monthlyIncomeVolatility": 0.18,
                      "debtToIncomeRatio": 0.32,
                      "existingDebtAmount": 18000000,
                      "delinquencyCount": 0,
                      "platformSettlementMonths": 36,
                      "platformSettlementMean": 5200000,
                      "platformSettlementVolatility": 0.21,
                      "contractDurationMonths": 48,
                      "incomeDeclarationAvailable": true,
                      "telecomPaymentDelinquencyCount": 0
                    },
                    "featurePayloadDigest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                  },
                  "featurePayloadDigest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                  "sourceLoanApplicationVersion": 3,
                  "createdAt": "2026-07-21T10:00:00Z"
                }
                """;
    }
}
