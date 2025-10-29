package de.jwiegmann.registry.poc.control;

import de.jwiegmann.registry.poc.control.dto.MyKafkaMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
@Slf4j
public class KafkaConsumerService {

    // Speichert die erfolgreich empfangenen & validierten Nachrichten
    private final List<MyKafkaMessage> validMessages = new CopyOnWriteArrayList<>();

    /**
     * Kafka Listener, der Nachrichten konsumiert und automatisch gegen das JSON-Schema validiert.
     * Das Schema wird aus der Schema-Registry geladen, und die Validierung übernimmt der Deserializer.
     *
     * @param message Das bereits validierte und deserialisierte Nachrichtenobjekt.
     */
    @KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.group}")
    public void consume(final MyKafkaMessage message) {
        log.info("Empfangene & gültige Nachricht: {}", message);
        validMessages.add(message);
    }

    /**
     * Gibt die Liste der gültigen empfangenen Nachrichten zurück.
     *
     * @return Liste der validierten Nachrichten.
     */
    public List<MyKafkaMessage> getValidMessages() {
        return validMessages;
    }

    /**
     * Leert die Liste der empfangenen Nachrichten.
     * Nützlich für Test-Isolation.
     */
    public void clearMessages() {
        validMessages.clear();
    }
}
