package de.jwiegmann.registry.poc;

import de.jwiegmann.registry.poc.control.dto.MyKafkaMessage;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
@Slf4j
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${schema.id}")
    private String schemaId;

    // ============================================================================
    // PRODUCER CONFIGURATION
    // ============================================================================

    @Bean
    public ProducerFactory<String, MyKafkaMessage> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put("schema.registry.url", schemaRegistryUrl);

        // Serializer des Kafka Message Keys
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Serializer des Kafka Message Values
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);

        // Automatische clientseitige Schema-Generierung und Registrierung aus
        configProps.put("auto.register.schemas", false);

        // Immer das neuste Schema eines Subjects verwenden
        configProps.put("use.latest.version", true);

        // Nicht prüfen, ob das Schema mit dem letzten Schema des Subjects kompatibel ist
        configProps.put("latest.compatibility.strict", false);

        // Ein Datenmodell gehört immer exakt zu einem Subject in der Registry
        configProps.put("value.subject.name.strategy", RecordNameStrategy.class);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, MyKafkaMessage> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // ============================================================================
    // CONSUMER CONFIGURATION
    // ============================================================================

    @Bean
    public ConsumerFactory<String, MyKafkaMessage> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put("schema.registry.url", schemaRegistryUrl);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");

        // Holt immer alle Messages ab, wenn noch kein Offset vorhanden ist
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // KafkaDeserializer für Key, Value und Error
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaJsonSchemaDeserializer.class);

        // Welcher Error Handler wird aufgerufen, wenn eine Schema-Validierung failed
        configProps.put(ErrorHandlingDeserializer.VALUE_FUNCTION, SchemaValidationErrorHandler.class);

        // Festlegen auf welches DTO der Kafka Message Value automatisch gemappt werden soll
        configProps.put("specific.json.reader", true);
        configProps.put("json.value.type", de.jwiegmann.registry.poc.control.dto.MyKafkaMessage.class.getName());

        // Sicherstellen, dass bei einer nicht erfolgreichen Validierung eine Exception geworfen wird
        configProps.put("json.fail.invalid.schema", true);

        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MyKafkaMessage> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, MyKafkaMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}


