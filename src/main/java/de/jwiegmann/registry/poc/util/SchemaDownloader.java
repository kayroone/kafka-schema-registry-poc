package de.jwiegmann.registry.poc.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;

@Slf4j
@Component
public class SchemaDownloader {

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${schema.registry.subject}")
    private String subject;

    @Value("${schema.target.folder:src/main/resources/static/components}")
    private String targetFolder;

    @Value("${schema.target.filename:MyKafkaMessage.json}")
    private String targetFilename;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RestTemplate restTemplate = new RestTemplate();

    @EventListener(ApplicationReadyEvent.class)
    public void downloadSchema() {
        log.info("Starte REST-Download für Subject: {}", subject);

        try {
            String url = String.format("%s/subjects/%s/versions/latest", schemaRegistryUrl, subject);
            String response = restTemplate.getForObject(url, String.class);

            JsonNode responseNode = objectMapper.readTree(response);
            String schemaString = responseNode.get("schema").asText();
            JsonNode schemaJson = objectMapper.readTree(schemaString);

            File targetDir = new File(targetFolder);
            if (!targetDir.exists()) {
                Files.createDirectories(targetDir.toPath());
            }

            File schemaFile = new File(targetDir, targetFilename);
            try (FileWriter writer = new FileWriter(schemaFile, false)) {
                writer.write(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(schemaJson));
            }

            log.info("Schema gespeichert unter: {}", schemaFile.getAbsolutePath());

        } catch (Exception e) {
            log.error("Fehler beim Abrufen/Speichern des Schemas!", e);
        }
    }
}
