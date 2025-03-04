package de.jwiegmann.registry.poc.control;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.StreamUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = de.jwiegmann.registry.poc.KafkaSchemaRegistryPocApplication.class)
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class KafkaConsumerIntegrationTest {

    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"))
            .withExposedPorts(9093);

    @Container
    public static GenericContainer<?> schemaRegistry = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:latest"))
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9094")
            .withStartupTimeout(Duration.ofMinutes(2));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> {
            if (!kafka.isRunning()) {
                kafka.start();
            }
            return kafka.getBootstrapServers();
        });
        registry.add("schema.registry.url", () -> {
            if (!schemaRegistry.isRunning()) {
                schemaRegistry.start();
            }
            return "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
        });
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaConsumerService consumerService;

    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper mapper = new ObjectMapper();

    private String loadSchemaFromFile() throws Exception {
        try (InputStream is = getClass().getResourceAsStream("/schema.json")) {
            return StreamUtils.copyToString(is, StandardCharsets.UTF_8);
        }
    }

    private final String subject = "my-topic-value";

    @BeforeAll
    public void setUpSchemaRegistry() throws Exception {
        String schemaJson = loadSchemaFromFile();

        String host = schemaRegistry.getHost();
        Integer port = schemaRegistry.getMappedPort(8081);

        String urlLatest = "http://" + host + ":" + port + "/subjects/" + subject + "/versions/latest";
        String urlRegister = "http://" + host + ":" + port + "/subjects/" + subject + "/versions";

        try {
            ResponseEntity<JsonNode> response = restTemplate.getForEntity(urlLatest, JsonNode.class);
            if (response.getStatusCode() == HttpStatus.OK) {
                System.out.println("Schema für Subject '" + subject + "' existiert bereits: " + response.getBody());
                return;
            }
        } catch (HttpClientErrorException e) {
            if (!e.getStatusCode().equals(HttpStatus.NOT_FOUND)) {
                throw e;
            }
        }

        ObjectNode request = mapper.createObjectNode();
        request.put("schema", schemaJson);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(request.toString(), headers);

        ResponseEntity<JsonNode> registerResponse = restTemplate.postForEntity(urlRegister, entity, JsonNode.class);
        assertTrue(registerResponse.getStatusCode().is2xxSuccessful(), "Schema konnte nicht in der Registry registriert werden");
        System.out.println("Schema erfolgreich registriert: " + registerResponse.getBody());
    }

    @Test
    public void testValidMessage() throws Exception {
        String validMessage = "{\"id\":1,\"username\":\"testuser\",\"email\":\"test@example.com\"}";
        kafkaTemplate.send("my-topic", validMessage);
        Thread.sleep(2000);
        assertTrue(consumerService.getValidMessages().contains(validMessage));
    }

    @Test
    public void testInvalidMessage() throws Exception {
        String invalidMessage = "{\"id\":1,\"username\":\"testuser\"}";
        kafkaTemplate.send("my-topic", invalidMessage);
        Thread.sleep(2000);
        Assertions.assertFalse(consumerService.getValidMessages().contains(invalidMessage));
    }
}
