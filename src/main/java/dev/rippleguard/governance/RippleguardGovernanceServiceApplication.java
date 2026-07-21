package dev.rippleguard.governance;

import dev.rippleguard.governance.infrastructure.kafka.OutboxProperties;
import dev.rippleguard.governance.infrastructure.agent.AgentRuntimeProperties;
import dev.rippleguard.governance.infrastructure.contracts.ContractProperties;
import dev.rippleguard.governance.application.Phase2ExecutionPlanProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@EnableConfigurationProperties({
        OutboxProperties.class,
        AgentRuntimeProperties.class,
        ContractProperties.class,
        Phase2ExecutionPlanProperties.class
})
@SpringBootApplication
public class RippleguardGovernanceServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(RippleguardGovernanceServiceApplication.class, args);
    }
}
