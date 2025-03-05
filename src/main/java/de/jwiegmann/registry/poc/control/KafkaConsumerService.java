package de.jwiegmann.registry.poc.control;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.ValidationMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Service, der Nachrichten aus einem Kafka-Topic konsumiert, diese gegen ein JSON-Schema validiert und
 * gültige Nachrichten speichert.
 * <p>
 * Das Schema wird zur Laufzeit aus der Schema Registry abgerufen.
 */
@Component
public class KafkaConsumerService {

    /**
     * Liste, in der alle gültigen Nachrichten gespeichert werden.
     */
    private final List<String> validMessages = new CopyOnWriteArrayList<>();

    /**
     * Das JSON-Schema, das zur Validierung verwendet wird.
     */
    private final JsonSchema schema;

    /**
     * ObjectMapper zum Parsen von JSON-Nachrichten.
     */
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Service zum Abrufen des Schemas aus der Schema Registry.
     */
    private final SchemaRegistryService registryService;

    /**
     * Das Subject (Name) des Schemas in der Schema Registry.
     */
    private final String schemaSubject;

    /**
     * Konstruktor für KafkaConsumerService.
     *
     * @param registryService der Service, der das Schema aus der Schema Registry abruft (als Lazy-Bean geladen)
     * @param schemaSubject   der Name (Subject) des Schemas, das validiert werden soll; wird aus den Properties geladen
     * @throws Exception falls das Schema nicht abgerufen werden kann
     */
    public KafkaConsumerService(@Lazy SchemaRegistryService registryService,
                                @Value("${schema.subject}") String schemaSubject) throws Exception {
        this.registryService = registryService;
        this.schemaSubject = schemaSubject;
        this.schema = loadSchema();
    }

    /**
     * Lädt das Schema aus der Schema Registry.
     *
     * @return das JSON-Schema als {@link JsonSchema}
     * @throws Exception falls ein Fehler beim Abrufen oder Parsen des Schemas auftritt
     */
    private JsonSchema loadSchema() throws Exception {
        return registryService.getLatestSchema(schemaSubject);
    }

    /**
     * Validiert eine gegebene JSON-Nachricht gegen das Schema.
     *
     * @param message die zu validierende JSON-Nachricht als String
     * @return {@code true}, wenn die Nachricht dem Schema entspricht, ansonsten {@code false}
     */
    public boolean validateAgainstSchema(String message) {
        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            Set<ValidationMessage> errors = schema.validate(jsonNode);
            return errors.isEmpty();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Kafka-Listener, der Nachrichten aus dem Topic empfängt.
     * <p>
     * Nur Nachrichten, die das Schema erfüllen, werden gespeichert.
     *
     * @param message die vom Kafka-Topic empfangene Nachricht
     */
    @KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.group}")
    public void consume(String message) {
        if (validateAgainstSchema(message)) {
            validMessages.add(message);
        }
    }

    /**
     * Gibt die Liste der gültigen Nachrichten zurück.
     *
     * @return eine Liste mit gültigen JSON-Nachrichten
     */
    public List<String> getValidMessages() {
        return validMessages;
    }
}
