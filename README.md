# Kafka Schema Registry PoC

Dieses Proof-of-Concept demonstriert, wie man eine Spring Boot Applikation erstellt, die Kafka-Nachrichten konsumiert,
diese gegen ein JSON-Schema validiert und das Schema dynamisch aus der Confluent Schema Registry abruft. Zusätzlich
kommen Testcontainers zum Einsatz, um Kafka und die Schema Registry in Integrationstests bereitzustellen.

## Features

- **Kafka Integration:**  
  Nachrichten werden aus einem Kafka-Topic konsumiert.

- **Schema Validation:**  
  Eingehende Nachrichten werden gegen ein JSON-Schema validiert, das in der Schema Registry hinterlegt ist.

- **Dynamische Schema Registry:**  
  Das Schema wird zur Laufzeit über einen REST-Call aus der Confluent Schema Registry abgerufen.

- **Testcontainers:**  
  Automatisierter Start von Kafka- und Schema Registry-Containern im Integrationstest.

- **Kafka Admin:**  
  Erstellung von Topics über den Kafka-AdminClient im Setup.

## Architektur

- **Spring Boot Applikation:**  
  Nutzt Spring Kafka, um Nachrichten aus dem Topic zu konsumieren, und validiert diese mithilfe des JSON Schema
  Validators.

- **Schema Registry Service:**  
  Holt das neueste JSON Schema aus der Schema Registry (unter Verwendung des `schema.registry.url` und des Subjects, z.
  B. `my-topic-value`).

- **Test Base:**  
  Eine gemeinsame Testbasis, die Kafka- und Schema Registry-Container startet, Properties dynamisch setzt, Topics
  erstellt und das Schema in der Registry registriert.

## Voraussetzungen

- **Java 17 oder höher:**  
  Das Projekt verwendet moderne Java-Versionen (z. B. Java 23).

- **Maven:**  
  Zum Bauen und Testen des Projekts.

- **Docker:**  
  Für den Einsatz von Testcontainers (Kafka und Schema Registry werden in Docker-Containern gestartet).

## Konfiguration

Die Konfiguration erfolgt größtenteils über die `application.yaml` (oder `application.properties`):

- **Kafka:**  
  Der Schlüssel `spring.kafka.bootstrap-servers` wird dynamisch per Testcontainers in den Integrationstests gesetzt.

- **Schema Registry:**  
  `schema.registry.url` wird ebenfalls dynamisch gesetzt. Das JSON Schema wird unter dem Subject (z. B.
  `my-topic-value`) registriert.

- **Schema Subject & Kafka Einstellungen:**  
  Beispiel:
  ```yaml
  schema:
    subject: my-topic-value
  kafka:
    topic: my-topic
    group: test-group

## Funktionsweise

- **Schema Registrierung:**  
  Die Methode `setUpSchemaRegistry()` liest das Schema aus `src/test/resources/schema.json` und registriert es in der
  Schema Registry. Der Request enthält explizit `"schemaType": "JSON"`, damit die Registry weiß, dass es sich um ein
  JSON Schema handelt.

- **Nachrichtenkonsum & Validierung:**  
  Der `KafkaConsumerService` empfängt Nachrichten von `my-topic` und validiert sie mithilfe des abgerufenen JSON
  Schemas. Nur gültige Nachrichten werden intern gespeichert und können über die Methode `getValidMessages()` abgefragt
  werden.
