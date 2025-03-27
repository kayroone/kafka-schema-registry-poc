package de.jwiegmann.registry.poc.control.testcontainers;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class SchemaRegistryCompatibilityTestBase extends TestBase {

    protected static SchemaRegistryClient schemaRegistryClient;

    protected abstract String getCompatibilityLevel();

    @BeforeAll
    public static void setUpClient() {
        schemaRegistryClient = new CachedSchemaRegistryClient(
                SCHEMA_REGISTRY_CONTAINER.getLocalSchemaRegistryUrl(), 10);
    }

    @BeforeAll
    public void setCompatibilityLevel() throws IOException, RestClientException {
        String level = getCompatibilityLevel();
        schemaRegistryClient.updateCompatibility(SUBJECT_NAME, level);
    }
}
