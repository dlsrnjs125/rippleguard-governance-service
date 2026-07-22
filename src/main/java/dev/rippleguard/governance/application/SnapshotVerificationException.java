package dev.rippleguard.governance.application;

public class SnapshotVerificationException extends RuntimeException {
    private final String classification;
    private final String reasonCode;

    public SnapshotVerificationException(String classification, String reasonCode, String message) {
        super(message);
        this.classification = classification;
        this.reasonCode = reasonCode;
    }

    public String classification() {
        return classification;
    }

    public String reasonCode() {
        return reasonCode;
    }
}
