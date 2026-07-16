package dev.rippleguard.governance.interfaces.rest;

import dev.rippleguard.governance.application.DecisionCaseService;
import java.util.UUID;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/decision-cases")
public class DecisionCaseController {
    private final DecisionCaseService service;

    public DecisionCaseController(DecisionCaseService service) {
        this.service = service;
    }

    @GetMapping("/{caseId}")
    DecisionCaseResponse get(@PathVariable String caseId) {
        return service.get(caseId);
    }

    @GetMapping("/by-application/{applicationId}")
    DecisionCaseResponse getByApplication(@PathVariable UUID applicationId) {
        return service.getByApplication(applicationId);
    }
}
