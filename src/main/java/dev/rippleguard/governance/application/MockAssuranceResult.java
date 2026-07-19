package dev.rippleguard.governance.application;

import dev.rippleguard.governance.domain.AssuranceResult;

public record MockAssuranceResult(
        AssuranceResult result,
        String reasonCode
) {
}
