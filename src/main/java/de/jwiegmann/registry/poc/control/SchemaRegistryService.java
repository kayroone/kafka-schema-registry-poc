package de.jwiegmann.registry.poc.control;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

/**
 * Service zum Abrufen von JSON-Schemas aus der Confluent Schema Registry.
 * <p>
 * Dieser Service verwendet einen {@link RestTemplate}, um das aktuell registrierte Schema
 * für ein gegebenes Subject von der Schema Registry zu laden und in ein {@link JsonSchema} zu parsen.
 */
@Service
public class SchemaRegistryService {

    /**
     * RestTemplate zur Kommunikation mit der Schema Registry.
     */
    private final RestTemplate restTemplate = new RestTemplate();

    /**
     * Die URL der Schema Registry, wird aus den Anwendungseigenschaften geladen.
     */
    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    /**
     * Ruft das aktuell registrierte Schema für das angegebene Subject ab.
     * <p>
     * Hierbei wird eine GET-Anfrage an den Endpunkt der Schema Registry gesendet,
     * um das neueste Schema zu erhalten. Anschließend wird das Schema in ein {@link JsonSchema}
     * geparst, welches für die Validierung von Nachrichten genutzt werden kann.
     *
     * @param subject das Subject, unter dem das Schema registriert ist (z.B. "my-topic-value")
     * @return das {@link JsonSchema} des aktuell registrierten Schemas
     * @throws Exception falls beim Abruf oder Parsing des Schemas ein Fehler auftritt
     */
    public JsonSchema getLatestSchema(final String subject) throws Exception {
        String url = schemaRegistryUrl + "/subjects/" + subject + "/versions/latest";
        JsonNode response = restTemplate.getForObject(url, JsonNode.class);
        String schemaString = response.get("schema").asText();
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
        return factory.getSchema(schemaString);
    }
}
