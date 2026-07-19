package dev.rippleguard.governance.infrastructure.kafka;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "rippleguard.outbox")
public record OutboxProperties(
        @Min(1) int batchSize,
        @Min(5) long leaseSeconds,
        @NotBlank String instanceId
) {
}
