package dev.rippleguard.governance.application;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "rippleguard.phase2.execution-plan")
public record Phase2ExecutionPlanProperties(
        @NotBlank String planVersion,
        @NotBlank String featureSchemaVersion,
        @NotBlank String preprocessingVersion,
        @NotBlank String modelVersion,
        @NotBlank String modelArtifactDigest,
        @NotBlank String thresholdVersion,
        @Min(1) int maxAttempts,
        Duration requestTimeout,
        Duration retryBackoff,
        Duration leaseDuration
) {
    public Phase2ExecutionPlanProperties {
        if (requestTimeout == null || requestTimeout.isNegative() || requestTimeout.isZero()) {
            throw new IllegalArgumentException("requestTimeout must be positive");
        }
        if (retryBackoff == null || retryBackoff.isNegative() || retryBackoff.isZero()) {
            throw new IllegalArgumentException("retryBackoff must be positive");
        }
        if (leaseDuration == null || leaseDuration.isNegative() || leaseDuration.isZero()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }
        if (!modelArtifactDigest.matches("^sha256:[a-f0-9]{64}$")) {
            throw new IllegalArgumentException("modelArtifactDigest must be a sha256 digest");
        }
    }
}
