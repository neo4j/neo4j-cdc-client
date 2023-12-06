/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cdc.client;

import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;

/**
 * Neo4j 5.15+ introduced a breaking change in node ande relationship keys structure. This suite verifies if
 * CDC Client is backward compatible with 5.14 and earlier.
 */
public class CDCClient514IT extends CDCClientIT {

    private static final String NEO4J_VERSION = "5.14";

    @SuppressWarnings("resource")
    @Container
    private static final Neo4jContainer<?> neo4j = new Neo4jContainer<>("neo4j:" + NEO4J_VERSION + "-enterprise")
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
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

    @Override
    Driver driver() {
        return driver;
    }

    @Override
    Neo4jContainer<?> neo4j() {
        return neo4j;
    }

    @Override
    Map<String, Object> defaultExpectedAdditionalEntries() {
        return Collections.emptyMap();
    }
}
