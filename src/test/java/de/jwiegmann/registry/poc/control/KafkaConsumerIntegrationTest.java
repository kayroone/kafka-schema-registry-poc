package de.jwiegmann.registry.poc.control;

import de.jwiegmann.registry.poc.control.dto.MyKafkaMessage;
import de.jwiegmann.registry.poc.control.testcontainers.TestBase;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = de.jwiegmann.registry.poc.KafkaSchemaRegistryPocApplication.class)
@DirtiesContext
@Slf4j
public class KafkaConsumerIntegrationTest extends TestBase {

    @Autowired
    private KafkaTemplate<String, MyKafkaMessage> kafkaTemplate;

    @Autowired
    private KafkaConsumerService consumerService;

    @Test
    public void testValidMessage() {
        MyKafkaMessage validMessage = new MyKafkaMessage("1", "Dies ist eine gültige Nachricht", 1);

        kafkaTemplate.send("my-topic", validMessage);

        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    List<MyKafkaMessage> messages = consumerService.getValidMessages();
                    assertThat(messages)
                            .usingRecursiveFieldByFieldElementComparator()
                            .contains(validMessage);
                });
    }

    @Test
    public void testInvalidMessage() {
        // Beispiel für eine ungültige Nachricht: version negativ
        MyKafkaMessage invalidMessage = new MyKafkaMessage("2", "Ungültige Nachricht", -1);

        kafkaTemplate.send("my-topic", invalidMessage);

        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    List<MyKafkaMessage> messages = consumerService.getValidMessages();
                    assertThat(messages)
                            .usingRecursiveFieldByFieldElementComparator()
                            .doesNotContain(invalidMessage);
                });
    }
}
