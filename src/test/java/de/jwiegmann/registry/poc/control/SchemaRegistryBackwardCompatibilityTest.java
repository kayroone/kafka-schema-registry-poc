package de.jwiegmann.registry.poc.control;

import de.jwiegmann.registry.poc.control.testcontainers.SchemaRegistryCompatibilityTestBase;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integrationstest zur Überprüfung der BACKWARD-Kompatibilität in der Confluent Schema Registry.
 *
 * <p>BACKWARD-Kompatibilität bedeutet, dass neue Schemas mit Daten funktionieren müssen,
 * die mit älteren Versionen des Schemas produziert wurden. Dies ist typischerweise relevant
 * für Consumer, die auch ältere Nachrichten korrekt verarbeiten können sollen.</p>
 *
 * <p>In diesem Test wird ein neues Schema registriert, das ein optionales Feld hinzufügt.
 * Solche Änderungen gelten in der Regel als backward-kompatibel.</p>
 *
 * @see <a href="https://docs.confluent.io/platform/current/schema-registry/compatibility.html">Confluent: Compatibility Rules</a>
 */
public class SchemaRegistryBackwardCompatibilityTest extends SchemaRegistryCompatibilityTestBase {

    @Override
    protected String getCompatibilityLevel() {
        return "BACKWARD";
    }

    @Test
    public void shouldBeBackwardCompatibleWhenOptionalFieldAdded() throws Exception {
        String backwardCompatibleSchemaStr = """
                    {
                      "$schema": "http://json-schema.org/draft-07/schema#",
                      "title": "de.jwiegmann.registry.poc.control.dto.MyKafkaMessage",
                      "type": "object",
                      "additionalProperties": false,
                      "properties": {
                        "id": {
                          "oneOf": [
                            { "type": "null", "title": "Not included" },
                            { "type": "string" }
                          ]
                        },
                        "message": {
                          "oneOf": [
                            { "type": "null", "title": "Not included" },
                            { "type": "string" }
                          ]
                        },
                        "version": {
                          "type": "integer",
                          "minimum": 1
                        },
                        "status": {
                          "type": "string"
                        }
                      },
                      "required": ["version"]
                    }
                """;

        JsonSchema newSchema = new JsonSchema(backwardCompatibleSchemaStr);

        boolean isCompatible = schemaRegistryClient.testCompatibility(SUBJECT_NAME, newSchema);
        assertTrue(isCompatible, "Schema is NOT backward compatible – but it should be");
    }
}
