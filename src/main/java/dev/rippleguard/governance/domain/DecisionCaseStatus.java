package dev.rippleguard.governance.domain;

public enum DecisionCaseStatus {
    CREATED,
    PREFLIGHT_COMPLETED,
    EVALUATION_REQUESTED,
    PROPOSAL_READY,
    ASSURANCE_EVALUATED,
    VERIFICATION_REQUIRED,
    BLOCKED,
    RECALCULATION_REQUIRED,
    RESOLVED
}
