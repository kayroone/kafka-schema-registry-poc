package de.jwiegmann.registry.poc.control;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class SchemaRegistryService {

    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    public JsonSchema getLatestSchema(final String subject) throws Exception {
        String url = schemaRegistryUrl + "/subjects/" + subject + "/versions/latest";
        JsonNode response = restTemplate.getForObject(url, JsonNode.class);
        String schemaString = response.get("schema").asText();
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
        return factory.getSchema(schemaString);
    }
}
