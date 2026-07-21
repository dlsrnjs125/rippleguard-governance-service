package dev.rippleguard.governance.infrastructure.agent;

import com.fasterxml.jackson.databind.JsonNode;
import dev.rippleguard.governance.application.LoanDecisionAgentClient;
import java.net.SocketTimeoutException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

@Component
@ConditionalOnProperty(name = "rippleguard.agent-runtime.enabled", havingValue = "true")
public class RestLoanDecisionAgentClient implements LoanDecisionAgentClient {
    private final RestClient client;
    private final AgentRuntimeProperties properties;

    public RestLoanDecisionAgentClient(AgentRuntimeProperties properties) {
        this.properties = properties;
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(properties.connectTimeout());
        requestFactory.setReadTimeout(properties.responseTimeout());
        this.client = RestClient.builder()
                .baseUrl(properties.baseUrl())
                .requestFactory(requestFactory)
                .build();
    }

    @Override
    public JsonNode execute(JsonNode request) {
        try {
            return client.post()
                    .uri(properties.loanDecisionRunsPath())
                    .body(request)
                    .retrieve()
                    .body(JsonNode.class);
        } catch (ResourceAccessException exception) {
            if (exception.getCause() instanceof SocketTimeoutException) {
                throw new AgentRuntimeTimeoutException("Agent Runtime timed out", exception);
            }
            throw new AgentRuntimeTransportException("Agent Runtime transport failure", exception);
        } catch (RestClientException exception) {
            throw new AgentRuntimeTransportException("Agent Runtime request failed", exception);
        }
    }
}
