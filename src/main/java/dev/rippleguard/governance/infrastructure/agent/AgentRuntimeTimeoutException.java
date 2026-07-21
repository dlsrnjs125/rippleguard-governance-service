package dev.rippleguard.governance.infrastructure.agent;

public class AgentRuntimeTimeoutException extends RuntimeException {
    public AgentRuntimeTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
