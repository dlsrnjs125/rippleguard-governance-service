package dev.rippleguard.governance.infrastructure.loan;

import dev.rippleguard.governance.application.LoanFeatureSnapshotClient;
import dev.rippleguard.governance.application.LoanFeatureSnapshotNotFoundException;
import dev.rippleguard.governance.application.LoanFeatureSnapshotTimeoutException;
import dev.rippleguard.governance.application.LoanFeatureSnapshotTransportException;
import dev.rippleguard.governance.application.Phase2FeatureSnapshot;
import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

@Component
@ConditionalOnProperty(name = "rippleguard.loan-service.enabled", havingValue = "true")
public class RestLoanFeatureSnapshotClient implements LoanFeatureSnapshotClient {
    private static final String SERVICE_TOKEN_HEADER = "X-Internal-Service-Token";

    private final RestClient client;
    private final LoanFeatureSnapshotProperties properties;

    public RestLoanFeatureSnapshotClient(LoanFeatureSnapshotProperties properties) {
        this.properties = properties;
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(properties.connectTimeout());
        requestFactory.setReadTimeout(properties.responseTimeout());
        this.client = RestClient.builder()
                .baseUrl(properties.baseUrl())
                .requestFactory(requestFactory)
                .build();
    }

    @Override
    public Phase2FeatureSnapshot getByReference(UUID applicationId, String snapshotVersion) {
        try {
            return client.get()
                    .uri(properties.featureSnapshotsPathTemplate(), Map.of(
                            "applicationId", applicationId,
                            "snapshotVersion", snapshotVersion
                    ))
                    .header(SERVICE_TOKEN_HEADER, properties.serviceToken())
                    .retrieve()
                    .body(Phase2FeatureSnapshot.class);
        } catch (HttpClientErrorException exception) {
            if (exception.getStatusCode() == HttpStatus.NOT_FOUND) {
                throw new LoanFeatureSnapshotNotFoundException(
                        "Phase 2 feature snapshot not found: " + applicationId + " " + snapshotVersion);
            }
            throw new LoanFeatureSnapshotTransportException("Loan Service snapshot request failed", exception);
        } catch (ResourceAccessException exception) {
            if (exception.getCause() instanceof SocketTimeoutException) {
                throw new LoanFeatureSnapshotTimeoutException("Loan Service snapshot request timed out", exception);
            }
            throw new LoanFeatureSnapshotTransportException("Loan Service snapshot transport failure", exception);
        } catch (RestClientException exception) {
            throw new LoanFeatureSnapshotTransportException("Loan Service snapshot request failed", exception);
        }
    }
}
