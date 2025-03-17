package de.jwiegmann.registry.poc.control.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MyKafkaMessage {

    private final String id;
    private final String message;
    private final int version;

    @JsonCreator
    public MyKafkaMessage(
            @JsonProperty("id") String id,
            @JsonProperty("message") String message,
            @JsonProperty("version") int version
    ) {
        this.id = id;
        this.message = message;
        this.version = version;
    }

    public String getId() {
        return id;
    }

    public String getMessage() {
        return message;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "MyKafkaMessage{" +
                "id='" + id + '\'' +
                ", message='" + message + '\'' +
                ", version=" + version +
                '}';
    }
}

