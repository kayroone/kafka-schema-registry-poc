package de.jwiegmann.registry.poc.control;

import de.jwiegmann.registry.poc.control.testcontainers.TestBase;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@TestPropertySource(properties = "server.port=8080")
public class AsyncApiTest extends TestBase {

    private final RestTemplate restTemplate = new RestTemplate();

    @Test
    public void printAsyncApiYamlToConsole() {
        String url = "http://localhost:8080/async_api.yaml";

        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return restTemplate.getForObject(url, String.class) != null;
            } catch (Exception e) {
                return false;
            }
        });

        String asyncApiYaml = restTemplate.getForObject(url, String.class);

        System.out.println("\n===== AsyncAPI YAML =====\n");
        System.out.println(asyncApiYaml);
        System.out.println("\n=========================\n");
    }
}