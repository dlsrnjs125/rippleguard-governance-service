package dev.rippleguard.governance.infrastructure.kafka;

import dev.rippleguard.governance.application.DecisionCaseService;
import dev.rippleguard.governance.application.EventEnvelope;
import dev.rippleguard.governance.application.JsonSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "rippleguard.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class LoanEventConsumer {
    private static final Logger log = LoggerFactory.getLogger(LoanEventConsumer.class);

    private final DecisionCaseService service;
    private final JsonSupport json;

    public LoanEventConsumer(DecisionCaseService service, JsonSupport json) {
        this.service = service;
        this.json = json;
    }

    @KafkaListener(topics = "${rippleguard.kafka.topics.loan-application-submitted}")
    public void onLoanApplicationSubmitted(String message) {
        try {
            EventEnvelope event = json.fromJson(message, EventEnvelope.class);
            log.info("Consumed loan application submitted eventId={}", event.eventId());
            service.handleLoanApplicationSubmitted(event);
        } catch (IllegalArgumentException exception) {
            service.quarantineMalformedEvent(message, exception.getMessage());
            log.warn("Quarantined malformed governance input reason={}", exception.getMessage());
        }
    }
}
