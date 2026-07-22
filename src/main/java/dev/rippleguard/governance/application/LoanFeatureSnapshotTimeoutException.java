package dev.rippleguard.governance.application;

public class LoanFeatureSnapshotTimeoutException extends RuntimeException {
    public LoanFeatureSnapshotTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
