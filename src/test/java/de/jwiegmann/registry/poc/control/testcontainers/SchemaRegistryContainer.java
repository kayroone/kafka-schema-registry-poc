package de.jwiegmann.registry.poc.control.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import static de.jwiegmann.registry.poc.control.testcontainers.TestBase.CONFLUENT_PLATFORM_VERSION;

public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {

    public static final String SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry";
    public static final int SCHEMA_REGISTRY_PORT = 8081;

    public SchemaRegistryContainer() {
        this(CONFLUENT_PLATFORM_VERSION);
    }

    public SchemaRegistryContainer(String version) {
        super(SCHEMA_REGISTRY_IMAGE + ":" + version);
        waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

        // Port 8081 wird dynamisch gemappt für parallele Testausführung
        withExposedPorts(SCHEMA_REGISTRY_PORT);
    }

    public SchemaRegistryContainer withKafka(KafkaContainer kafka) {
        return withKafka(kafka.getNetwork(), kafka.getNetworkAliases().get(0) + ":9092");
    }

    private SchemaRegistryContainer withKafka(Network network, String bootstrapServers) {
        withNetwork(network);
        withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry");
        withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081");
        withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + bootstrapServers);
        return self();
    }

    /**
     * Gibt die Schema Registry URL für den lokalen Zugriff zurück.
     */
    public String getLocalSchemaRegistryUrl() {
        return "http://localhost:" + getMappedPort(SCHEMA_REGISTRY_PORT);
    }
}