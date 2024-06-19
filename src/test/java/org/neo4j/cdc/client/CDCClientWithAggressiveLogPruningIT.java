package org.neo4j.cdc.client;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.cdc.client.model.ChangeIdentifier;
import org.neo4j.cdc.client.selector.EntitySelector;
import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.ClientException;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

@Testcontainers
public class CDCClientWithAggressiveLogPruningIT {
    @Container
    private static final Neo4jContainer<?> neo4j = new Neo4jContainer<>("neo4j:5-enterprise")
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withNeo4jConfig("db.tx_log.rotation.retention_policy", "3 files")
            .withNeo4jConfig("db.tx_log.rotation.size", String.format("%d", 128 * 1024))
            .withAdminPassword("passw0rd");

    private static Driver driver;

    @BeforeAll
    static void setup() {
        driver = GraphDatabase.driver(neo4j.getBoltUrl(), AuthTokens.basic("neo4j", "passw0rd"));
    }

    @AfterAll
    static void cleanup() {
        driver.close();
    }

    @BeforeEach
    void reset() {
        try (var session = driver.session(SessionConfig.forDatabase("system"))) {
            session.run(
                            "CREATE OR REPLACE DATABASE $db OPTIONS {txLogEnrichment: $mode} WAIT",
                            Map.of("db", "neo4j", "mode", "FULL"))
                    .consume();
        }
    }

    @Test
    void shouldFailWithInvalidChangeIdentifierAfterLogPruning() {
        var current = current();

        // make large changes that will cause at least 3 tx log file rotation
        for (var i = 0; i < 1000; i++) {
            try (var session = driver.session(SessionConfig.forDatabase("neo4j"))) {
                session.run(
                        "CREATE (n:Test) SET n.id = $id, n.data = $data",
                        Map.of("id", i, "data", RandomStringUtils.random(1024, true, true)));
            }
        }

        // checkpoint
        try (var session = driver.session(SessionConfig.forDatabase("neo4j"))) {
            session.run("CALL db.checkpoint()").consume();
        }

        StepVerifier.create(new CDCClient(driver, () -> SessionConfig.forDatabase("neo4j")).query(current))
                .expectErrorSatisfies(t -> assertThat(t)
                        .isInstanceOf(ClientException.class)
                        .hasMessage("Unable to find the transaction entry for the given change identifier"))
                .verify();
    }

    @Test
    void shouldNotFailWithInvalidChangeIdentifierAfterLogPruningWhenStreaming()
            throws ExecutionException, InterruptedException {
        var executor = Executors.newSingleThreadExecutor();
        var current = current();

        var generateChanges = new AtomicBoolean(true);
        var changes = executor.submit(() -> {
            var counter = 0;

            while (generateChanges.get()) {
                // make changes
                for (var j = 0; j < 10; j++) {
                    try (var session = driver.session(SessionConfig.forDatabase("neo4j"))) {
                        session.run(
                                "CREATE (n:Test) SET n.id = $id, n.data = $data",
                                Map.of("id", counter * 100 + j, "data", RandomStringUtils.random(1024, true, true)),
                                TransactionConfig.builder()
                                        .withMetadata(Map.of("app", "hr"))
                                        .build());
                    }
                }

                // checkpoint
                try (var session = driver.session(SessionConfig.forDatabase("neo4j"))) {
                    session.run("CALL db.checkpoint()").consume();
                }

                counter++;
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            StepVerifier.create(new CDCClient(
                                    driver,
                                    () -> SessionConfig.forDatabase("neo4j"),
                                    EntitySelector.builder()
                                            .withTxMetadata(Map.of("app", "no-match"))
                                            .build())
                            .stream(current))
                    .expectTimeout(Duration.ofMinutes(1))
                    .verify();
        } finally {
            generateChanges.set(false);
            changes.get();
        }
    }

    private ChangeIdentifier current() {
        try (var session = driver.session(SessionConfig.forDatabase("neo4j"))) {
            return new ChangeIdentifier(
                    session.run("CALL cdc.current()").single().get(0).asString());
        }
    }
}
