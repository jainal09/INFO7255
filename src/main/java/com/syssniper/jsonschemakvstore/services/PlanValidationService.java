package com.syssniper.jsonschemakvstore.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.Set;

// ...
@Service
public class PlanValidationService {

    private final ObjectMapper mapper = new ObjectMapper();

    public boolean validateJsonAgainstSchema(String jsonData) {
        // fetch schema from resources directory
        String schemaPath = "/schema.json";
        try {
            JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
            InputStream schemaInputStream = getClass().getResourceAsStream(schemaPath);
            JsonSchema schema = factory.getSchema(schemaInputStream);
            JsonNode jsonNode = mapper.readTree(jsonData);
            Set<ValidationMessage> errors = schema.validate(jsonNode);
            return errors.isEmpty();
        } catch (Exception e) {
            throw new RuntimeException("Error validating JSON", e);
        }
    }
}

