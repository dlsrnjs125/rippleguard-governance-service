package dev.rippleguard.governance;

import dev.rippleguard.governance.infrastructure.kafka.OutboxProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@EnableConfigurationProperties(OutboxProperties.class)
@SpringBootApplication
public class RippleguardGovernanceServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(RippleguardGovernanceServiceApplication.class, args);
    }
}
