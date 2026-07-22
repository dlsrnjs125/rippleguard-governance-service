package dev.rippleguard.governance.application;

public class LoanFeatureSnapshotAccessDeniedException extends RuntimeException {
    public LoanFeatureSnapshotAccessDeniedException(String message, Throwable cause) {
        super(message, cause);
    }
}
