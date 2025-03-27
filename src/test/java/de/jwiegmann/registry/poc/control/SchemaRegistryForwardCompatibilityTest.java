package de.jwiegmann.registry.poc.control;

import de.jwiegmann.registry.poc.control.testcontainers.SchemaRegistryCompatibilityTestBase;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Integrationstest zur Überprüfung der FORWARD-Kompatibilität in der Confluent Schema Registry.
 *
 * <p>FORWARD-Kompatibilität bedeutet, dass Daten, die mit älteren Schemas produziert wurden,
 * auch mit neuen Versionen des Schemas verarbeitet werden können. Dies ist typischerweise relevant
 * für Producer, die mit neuen Schemas schreiben, während Consumer noch ältere Versionen erwarten.</p>
 *
 * <p>In diesem Test wird ein neues Schema registriert, das ein zusätzliches <b>required</b>-Feld einführt.
 * Solche Änderungen sind in der Regel <b>nicht</b> forward-kompatibel, da ältere Daten dieses Feld nicht enthalten würden.</p>
 *
 * @see <a href="https://docs.confluent.io/platform/current/schema-registry/compatibility.html">Confluent: Compatibility Rules</a>
 */
public class SchemaRegistryForwardCompatibilityTest extends SchemaRegistryCompatibilityTestBase {

    @Override
    protected String getCompatibilityLevel() {
        return "FORWARD";
    }

    @Test
    public void shouldFailForwardCompatibilityWhenRequiredFieldAdded() throws Exception {
        String forwardIncompatibleSchemaStr = """
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
                      "required": ["version", "status"]
                    }
                """;

        JsonSchema newSchema = new JsonSchema(forwardIncompatibleSchemaStr);

        boolean isCompatible = schemaRegistryClient.testCompatibility(SUBJECT_NAME, newSchema);
        assertFalse(isCompatible, "Schema is forward compatible – but it should NOT be");
    }
}
