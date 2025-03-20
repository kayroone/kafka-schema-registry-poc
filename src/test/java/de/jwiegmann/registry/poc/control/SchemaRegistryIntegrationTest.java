package de.jwiegmann.registry.poc.control;

import de.jwiegmann.registry.poc.control.dto.MyKafkaMessage;
import de.jwiegmann.registry.poc.control.testcontainers.TestBase;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integrationstests für die Schema Registry.
 * <p>
 * Diese Tests verifizieren, dass:
 * <ul>
 *     <li>Ein bestimmtes Subject in der Schema Registry registriert ist.</li>
 *     <li>Das neueste Schema korrekt geladen werden kann und den erwarteten Titel enthält.</li>
 *     <li>Ein neues Schema auf Kompatibilität geprüft werden kann.</li>
 *     <li>Ein neues Schema manuell als neue Version registriert werden kann.</li>
 * </ul>
 */
@Slf4j
public class SchemaRegistryIntegrationTest extends TestBase {

    /**
     * Der Subject-Name, unter dem das Schema des DTOs {@link MyKafkaMessage} in der Schema Registry erwartet wird.
     */
    private static final String SUBJECT_NAME = MyKafkaMessage.class.getName();

    /**
     * Der Schema Registry Client zum Zugriff auf die Schema Registry.
     */
    private static SchemaRegistryClient schemaRegistryClient;

    /**
     * Initialisiert den SchemaRegistryClient vor allen Tests.
     */
    @BeforeAll
    public static void setUp() {
        schemaRegistryClient = new CachedSchemaRegistryClient(
                SCHEMA_REGISTRY_CONTAINER.getLocalSchemaRegistryUrl(), 10);
    }

    /**
     * Prüft, ob das erwartete Subject {@link #SUBJECT_NAME} in der Schema Registry vorhanden ist.
     *
     * @throws IOException         wenn ein IO-Fehler auftritt
     * @throws RestClientException wenn die Anfrage an die Registry fehlschlägt
     */
    @Test
    public void shouldContainExpectedSubject() throws IOException, RestClientException {
        Collection<String> allSubjects = schemaRegistryClient.getAllSubjects();
        log.info("Subjects found in Registry: {}", allSubjects);

        assertTrue(allSubjects.contains(SUBJECT_NAME), "Subject not found in Schema Registry");
    }

    /**
     * Lädt das neueste Schema des Subjects und prüft, ob es ein JSON Schema ist und den korrekten Titel enthält.
     *
     * @throws IOException         wenn ein IO-Fehler auftritt
     * @throws RestClientException wenn die Anfrage an die Registry fehlschlägt
     */
    @Test
    public void shouldFetchLatestSchemaAndCheckTitle() throws IOException, RestClientException {
        var metadata = schemaRegistryClient.getLatestSchemaMetadata(SUBJECT_NAME);

        assertNotNull(metadata, "No metadata found for subject");
        log.info("Latest Schema Metadata: Version={}, ID={}", metadata.getVersion(), metadata.getId());

        ParsedSchema schema = schemaRegistryClient.getSchemaById(metadata.getId());

        assertInstanceOf(JsonSchema.class, schema, "Schema is not a JSON Schema");

        JsonSchema jsonSchema = (JsonSchema) schema;
        log.info("Fetched JSON Schema: {}", jsonSchema.canonicalString());

        assertTrue(jsonSchema.canonicalString().contains("\"title\":\"de.jwiegmann.registry.poc.control.dto.MyKafkaMessage\""),
                "Schema title is incorrect");
    }

    /**
     * Prüft, ob ein neues JSON Schema mit dem bestehenden Schema des Subjects kompatibel ist.
     * <p>
     * Der Kompatibilitätscheck erfolgt gegen das neueste registrierte Schema.
     *
     * @throws IOException         wenn ein IO-Fehler auftritt
     * @throws RestClientException wenn die Anfrage an die Registry fehlschlägt
     */
    @Test
    public void shouldValidateSchemaCompatibility() throws IOException, RestClientException {
        String newSchemaStr = """
                {
                  "$schema": "http://json-schema.org/draft-07/schema#",
                  "title": "de.jwiegmann.registry.poc.control.dto.MyKafkaMessage",
                  "type": "object",
                  "properties": {
                    "id": { "type": ["string", "null"] },
                    "message": { "type": ["string", "null"] },
                    "version": { "type": "integer", "minimum": 1 }
                  },
                  "required": ["version"]
                }
                """;

        JsonSchema newSchema = new JsonSchema(newSchemaStr);

        boolean isCompatible = schemaRegistryClient.testCompatibility(SUBJECT_NAME, newSchema);
        log.info("Is new schema compatible? {}", isCompatible);

        assertTrue(isCompatible, "New schema is not backward compatible!");
    }

    /**
     * Registriert manuell ein neues Schema als neue Version unter dem bestehenden Subject {@link #SUBJECT_NAME}.
     * <p>
     * Das neue Schema erweitert das alte um ein zusätzliches Feld {@code status}.
     *
     * @throws IOException         wenn ein IO-Fehler auftritt
     * @throws RestClientException wenn die Anfrage an die Registry fehlschlägt
     */
    @Test
    public void shouldRegisterNewVersionManually() throws IOException, RestClientException {
        String updatedSchemaStr = """
                {
                  "$schema": "http://json-schema.org/draft-07/schema#",
                  "title": "de.jwiegmann.registry.poc.control.dto.MyKafkaMessage",
                  "type": "object",
                  "properties": {
                    "id": { "type": ["string", "null"] },
                    "message": { "type": ["string", "null"] },
                    "version": { "type": "integer", "minimum": 1 },
                    "status": { "type": "string" }
                  },
                  "required": ["version"]
                }
                """;

        JsonSchema updatedSchema = new JsonSchema(updatedSchemaStr);

        int newVersionId = schemaRegistryClient.register(SUBJECT_NAME, updatedSchema);
        log.info("New schema registered with ID: {}", newVersionId);

        assertTrue(newVersionId > 1, "Failed to register new schema version");
    }
}
