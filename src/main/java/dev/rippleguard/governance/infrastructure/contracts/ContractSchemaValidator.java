package dev.rippleguard.governance.infrastructure.contracts;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SchemaLocation;
import com.networknt.schema.SpecVersion;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

@Component
public class ContractSchemaValidator {
    private final Path schemasRoot;
    private final JsonSchemaFactory schemaFactory;

    public ContractSchemaValidator(ContractProperties properties) {
        this.schemasRoot = properties.root().resolve("schemas").toAbsolutePath().normalize();
        if (!Files.isDirectory(this.schemasRoot)) {
            throw new IllegalArgumentException("Contracts schemas directory not found: " + this.schemasRoot);
        }
        this.schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012, builder ->
                builder.schemaMappers(schemaMappers -> schemaMappers
                        .mapPrefix("https://schemas.rippleguard.dev/", this.schemasRoot.toUri().toString()))
                        .defaultMetaSchemaIri("https://json-schema.org/draft/2020-12/schema")
                        .enableSchemaCache(true));
    }

    public void validate(String relativeSchemaPath, JsonNode value) {
        Path schemaPath = schemasRoot.resolve(relativeSchemaPath).toAbsolutePath().normalize();
        if (!schemaPath.startsWith(schemasRoot) || !Files.isRegularFile(schemaPath)) {
            throw new ContractValidationException("Contract schema not found: " + relativeSchemaPath
                    + " root=" + schemasRoot + " resolved=" + schemaPath);
        }
        var schema = schemaFactory.getSchema(SchemaLocation.of(schemaPath.toUri().toString()));
        var errors = schema.validate(value);
        if (!errors.isEmpty()) {
            String detail = errors.stream()
                    .limit(5)
                    .map(Object::toString)
                    .collect(Collectors.joining("; "));
            throw new ContractValidationException("Contract validation failed for " + relativeSchemaPath + ": " + detail);
        }
    }
}
