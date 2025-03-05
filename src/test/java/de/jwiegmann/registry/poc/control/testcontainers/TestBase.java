package de.jwiegmann.registry.poc.control.testcontainers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.http.*;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.StreamUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base class for integration tests using Testcontainers.
 * <p>
 * Hier werden Kafka- und Schema Registry-Container gestartet, Properties
 * dynamisch registriert, das Topic erstellt und ein JSON-Schema in der Registry registriert.
 */
@Testcontainers
public class TestBase {

    /**
     * Version der Confluent Platform.
     */
    public static final String CONFLUENT_PLATFORM_VERSION = "7.9.0";
    private static final Network KAFKA_NETWORK = Network.newNetwork();
    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka")
            .withTag(CONFLUENT_PLATFORM_VERSION);

    /**
     * KafkaContainer, der für die Tests verwendet wird.
     */
    @Container
    public static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(KAFKA_IMAGE)
            .withNetwork(KAFKA_NETWORK)
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");

    /**
     * SchemaRegistryContainer, der für die Tests verwendet wird.
     */
    public static final SchemaRegistryContainer SCHEMA_REGISTRY_CONTAINER =
            new SchemaRegistryContainer(CONFLUENT_PLATFORM_VERSION);

    private static final RestTemplate restTemplate = new RestTemplate();
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Registriert dynamisch Properties, die von Spring Boot genutzt werden.
     *
     * @param registry the dynamic property registry
     */
    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA_CONTAINER::getBootstrapServers);
        registry.add("schema.registry.url", () ->
                "http://" + SCHEMA_REGISTRY_CONTAINER.getHost() + ":" + SCHEMA_REGISTRY_CONTAINER.getFirstMappedPort());
    }

    /**
     * Startet die Container, erstellt das Kafka-Topic und registriert das JSON-Schema in der Schema Registry.
     *
     * @throws Exception if any error occurs during container startup or schema registration
     */
    @BeforeAll
    public static void startContainersAndSetUpSchema() throws Exception {
        // Starte Schema Registry-Container und verknüpfe ihn mit dem Kafka-Container
        SCHEMA_REGISTRY_CONTAINER.withKafka(KAFKA_CONTAINER).start();
        // Erstelle das Topic "my-topic" (mit 1 Partition und Replikationsfaktor 1)
        createTopic("my-topic");
        // Registriere das JSON-Schema in der Schema Registry
        setUpSchemaRegistry();
    }

    /**
     * Registriert das JSON-Schema in der Schema Registry für das Subject "my-topic-value".
     * Falls das Schema bereits existiert, wird keine erneute Registrierung vorgenommen.
     *
     * @throws Exception if an error occurs during schema registration
     */
    public static void setUpSchemaRegistry() throws Exception {
        String schemaJson = loadSchemaFromFile();

        String host = SCHEMA_REGISTRY_CONTAINER.getHost();
        Integer port = SCHEMA_REGISTRY_CONTAINER.getMappedPort(8081);
        String subject = "my-topic-value";

        String urlLatest = "http://" + host + ":" + port + "/subjects/" + subject + "/versions/latest";
        String urlRegister = "http://" + host + ":" + port + "/subjects/" + subject + "/versions";

        try {
            ResponseEntity<JsonNode> response = restTemplate.getForEntity(urlLatest, JsonNode.class);
            if (response.getStatusCode() == HttpStatus.OK) {
                System.out.println("Schema for subject '" + subject + "' already exists: " + response.getBody());
                return;
            }
        } catch (HttpClientErrorException e) {
            if (!e.getStatusCode().equals(HttpStatus.NOT_FOUND)) {
                throw e;
            }
        }

        ObjectNode request = mapper.createObjectNode();
        request.put("schemaType", "JSON");
        request.put("schema", schemaJson);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(request.toString(), headers);

        ResponseEntity<JsonNode> registerResponse = restTemplate.postForEntity(urlRegister, entity, JsonNode.class);
        assertTrue(registerResponse.getStatusCode().is2xxSuccessful(), "Schema could not be registered in the Registry");
        System.out.println("Schema successfully registered: " + registerResponse.getBody());
    }

    /**
     * Erstellt ein Kafka-Topic mit fest definierten Einstellungen.
     * Das Topic wird mit 1 Partition und einem Replikationsfaktor von 1 erstellt.
     *
     * @param topic the name of the topic to create
     * @throws Exception if topic creation fails
     */
    private static void createTopic(final String topic) throws Exception {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        try (AdminClient adminClient = AdminClient.create(config)) {
            NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            System.out.println("Topic " + topic + " created successfully.");
        }
    }

    /**
     * Lädt das JSON-Schema aus einer Datei im Ressourcenverzeichnis.
     * Die Datei sollte unter "/schema.json" im Klassenpfad verfügbar sein.
     *
     * @return the JSON schema as a String
     * @throws Exception if reading the file fails
     */
    private static String loadSchemaFromFile() throws Exception {
        try (InputStream is = TestBase.class.getResourceAsStream("/schema.json")) {
            return StreamUtils.copyToString(is, StandardCharsets.UTF_8);
        }
    }
}
