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
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Value Serializer: JSON Schema Serializer
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);

        // Schema Registry URL
        configProps.put("schema.registry.url", schemaRegistryUrl);

        // Optional: Automatische Schema-Generierung aus DTO UND Registrierung in der Registry - nur für lokale Entwicklung!
        configProps.put("auto.register.schemas", false);
        configProps.put("use.latest.version", true);
        configProps.put("latest.compatibility.strict", false);

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
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // KafkaDeserializer für Key, Value und Error
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaJsonSchemaDeserializer.class);

        configProps.put(ErrorHandlingDeserializer.VALUE_FUNCTION, SchemaValidationErrorHandler.class);

        // Schema Registry URL
        configProps.put("schema.registry.url", schemaRegistryUrl);

        // Damit MyKafkaMessage gemappt wird
        configProps.put("json.value.type", de.jwiegmann.registry.poc.control.dto.MyKafkaMessage.class.getName());

        configProps.put("json.fail.invalid.schema", true);
        configProps.put("specific.json.reader", true);

        //configProps.put("use.latest.version", true);
        //configProps.put("latest.compatibility.strict", false);
        //configProps.put("value.subject.name.strategy", RecordNameStrategy.class);

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


