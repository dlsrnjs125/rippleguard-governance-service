package dev.rippleguard.governance.infrastructure.agent;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "rippleguard.agent-runtime")
public record AgentRuntimeProperties(
        boolean enabled,
        @NotBlank String baseUrl,
        @NotBlank String loanDecisionRunsPath,
        Duration connectTimeout,
        Duration responseTimeout,
        @Min(1) int maxResponseBytes
) {
    public AgentRuntimeProperties {
        if (connectTimeout == null || connectTimeout.isNegative() || connectTimeout.isZero()) {
            throw new IllegalArgumentException("connectTimeout must be positive");
        }
        if (responseTimeout == null || responseTimeout.isNegative() || responseTimeout.isZero()) {
            throw new IllegalArgumentException("responseTimeout must be positive");
        }
        if (enabled && baseUrl.contains("localhost:8080")) {
            throw new IllegalArgumentException("Agent Runtime baseUrl must not rely on localhost:8080");
        }
    }
}
