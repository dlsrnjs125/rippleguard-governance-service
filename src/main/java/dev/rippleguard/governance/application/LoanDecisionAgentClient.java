package dev.rippleguard.governance.application;

import com.fasterxml.jackson.databind.JsonNode;

public interface LoanDecisionAgentClient {
    JsonNode execute(JsonNode request);
}
