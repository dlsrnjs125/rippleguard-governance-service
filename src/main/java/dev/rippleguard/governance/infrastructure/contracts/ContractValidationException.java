package dev.rippleguard.governance.infrastructure.contracts;

public class ContractValidationException extends RuntimeException {
    public ContractValidationException(String message) {
        super(message);
    }
}
