package de.jwiegmann.registry.poc;

import de.jwiegmann.registry.poc.control.dto.MyKafkaMessage;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.kafka.support.serializer.FailedDeserializationInfo;

import java.util.function.Function;

@Slf4j
@NoArgsConstructor
public class SchemaValidationErrorHandler implements Function<FailedDeserializationInfo, MyKafkaMessage> {

    private static final String SEPARATOR = StringUtils.repeat("═", 80);

    @Override
    public MyKafkaMessage apply(FailedDeserializationInfo info) {

        Throwable rootCause = getRootCause(info.getException());
        String reason = (rootCause != null) ? rootCause.getMessage() : "Unknown reason";

        log.error("\n\n" +
                        "🛑🛑🛑 {} 🛑🛑🛑\n\n" +
                        "SCHEMA VALIDATION FAILED\n\n" +
                        "Topic      : {}\n" +
                        "Raw Payload: {}\n" +
                        "Exception  : {}\n" +
                        "Reason     : {}\n\n" +
                        "🛑🛑🛑 {} 🛑🛑🛑\n",
                SEPARATOR,
                info.getTopic(),
                formatBytes(info.getData()),
                info.getException(),
                reason,
                SEPARATOR
        );

        return null; // Returning null means the record is skipped, no listener invocation
    }

    private String formatBytes(byte[] data) {
        if (data == null) {
            return "[no payload data]";
        }

        try {
            String result = new String(data);
            if (result.isBlank()) {
                return "[empty payload]";
            }
            return result;
        } catch (Exception e) {
            return "[unreadable payload bytes]";
        }
    }

    private Throwable getRootCause(Throwable throwable) {
        Throwable cause = throwable;
        while (cause != null && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }
}