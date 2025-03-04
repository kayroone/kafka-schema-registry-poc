package de.jwiegmann.registry.poc.control;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.ValidationMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class KafkaConsumerService {

    private final List<String> validMessages = new CopyOnWriteArrayList<>();
    private final JsonSchema schema;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public KafkaConsumerService(SchemaRegistryService registryService,
                                @Value("${schema.subject}") String schemaSubject) throws Exception {
        this.schema = registryService.getLatestSchema(schemaSubject);
    }

    public boolean validateAgainstSchema(String message) {
        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            Set<ValidationMessage> errors = schema.validate(jsonNode);
            return errors.isEmpty();
        } catch (Exception e) {
            return false;
        }
    }

    @KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.group}")
    public void consume(String message) {
        if (validateAgainstSchema(message)) {
            validMessages.add(message);
        }
    }

    public List<String> getValidMessages() {
        return validMessages;
    }
}
