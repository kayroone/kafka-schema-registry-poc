package de.jwiegmann.registry.poc.control;

import de.jwiegmann.registry.poc.control.testcontainers.TestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = de.jwiegmann.registry.poc.KafkaSchemaRegistryPocApplication.class)
public class KafkaConsumerIntegrationTest extends TestBase {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaConsumerService consumerService;

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
