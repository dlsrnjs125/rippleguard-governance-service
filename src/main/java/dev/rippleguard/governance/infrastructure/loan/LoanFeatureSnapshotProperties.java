package dev.rippleguard.governance.infrastructure.loan;

import jakarta.validation.constraints.NotBlank;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "rippleguard.loan-service")
public record LoanFeatureSnapshotProperties(
        boolean enabled,
        @NotBlank String baseUrl,
        @NotBlank String featureSnapshotsPathTemplate,
        @NotBlank String serviceToken,
        Duration connectTimeout,
        Duration responseTimeout
) {
    public LoanFeatureSnapshotProperties {
        if (connectTimeout == null || connectTimeout.isNegative() || connectTimeout.isZero()) {
            throw new IllegalArgumentException("connectTimeout must be positive");
        }
        if (responseTimeout == null || responseTimeout.isNegative() || responseTimeout.isZero()) {
            throw new IllegalArgumentException("responseTimeout must be positive");
        }
        if (enabled && baseUrl.contains("localhost")) {
            throw new IllegalArgumentException("Loan Service baseUrl must not rely on localhost");
        }
    }
}
