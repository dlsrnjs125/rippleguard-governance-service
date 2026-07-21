package dev.rippleguard.governance.infrastructure.contracts;

import jakarta.validation.constraints.NotBlank;
import java.nio.file.Path;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "rippleguard.contracts")
public record ContractProperties(
        @NotBlank String repository,
        @NotBlank String commit,
        @NotBlank String eventSchemaVersion,
        Path root
) {
    public ContractProperties {
        if (root == null) {
            throw new IllegalArgumentException("contracts root is required");
        }
    }
}
