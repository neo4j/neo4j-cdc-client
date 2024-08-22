/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cdc.client.selector;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.neo4j.cdc.client.model.*;

class SelectorTest {

    @Test
    void entitySelectorMatchesEntityEvents() {
        List.of(nodeCreateEvent(), relationshipCreateEvent()).forEach(event -> {
            assertThat(EntitySelector.builder().build().matches(event)).isTrue();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.CREATE)
                            .build()
                            .matches(event))
                    .isTrue();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.UPDATE)
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withChangesTo(Set.of("name"))
                            .build()
                            .matches(event))
                    .isTrue();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.CREATE)
                            .withChangesTo(Set.of("name"))
                            .build()
                            .matches(event))
                    .isTrue();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.UPDATE)
                            .withChangesTo(Set.of("name"))
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withChangesTo(Set.of("name", "id"))
                            .build()
                            .matches(event))
                    .isTrue();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.CREATE)
                            .withChangesTo(Set.of("name", "id"))
                            .build()
                            .matches(event))
                    .isTrue();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.UPDATE)
                            .withChangesTo(Set.of("name", "id"))
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withChangesTo(Set.of("dob"))
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.CREATE)
                            .withChangesTo(Set.of("dob"))
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.UPDATE)
                            .withChangesTo(Set.of("dob"))
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.CREATE)
                            .withChangesTo(Set.of("name", "id", "dob"))
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.UPDATE)
                            .withChangesTo(Set.of("name", "id", "dob"))
                            .build()
                            .matches(event))
                    .isFalse();
        });

        List.of(nodeDeleteEvent(), relationshipDeleteEvent()).forEach(event -> {
            assertThat(EntitySelector.builder().build().matches(event)).isTrue();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.DELETE)
                            .build()
                            .matches(event))
                    .isTrue();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.UPDATE)
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withChangesTo(Set.of("name"))
                            .build()
                            .matches(event))
                    .isTrue();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.DELETE)
                            .withChangesTo(Set.of("name"))
                            .build()
                            .matches(event))
                    .isTrue();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.UPDATE)
                            .withChangesTo(Set.of("name"))
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withChangesTo(Set.of("name", "id"))
                            .build()
                            .matches(event))
                    .isTrue();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.DELETE)
                            .withChangesTo(Set.of("name", "id"))
                            .build()
                            .matches(event))
                    .isTrue();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.UPDATE)
                            .withChangesTo(Set.of("name", "id"))
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withChangesTo(Set.of("dob"))
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.DELETE)
                            .withChangesTo(Set.of("dob"))
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.UPDATE)
                            .withChangesTo(Set.of("dob"))
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.DELETE)
                            .withChangesTo(Set.of("name", "id", "dob"))
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.UPDATE)
                            .withChangesTo(Set.of("name", "id", "dob"))
                            .build()
                            .matches(event))
                    .isFalse();
        });

        List.of(nodeUpdateEvent(), relationshipUpdateEvent()).forEach(event -> {
            assertThat(EntitySelector.builder().build().matches(event)).isTrue();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.UPDATE)
                            .build()
                            .matches(event))
                    .isTrue();
            assertThat(EntitySelector.builder()
                            .withChangesTo(Set.of("name"))
                            .build()
                            .matches(event))
                    .isTrue();
            assertThat(EntitySelector.builder()
                            .withChangesTo(Set.of("surname"))
                            .build()
                            .matches(event))
                    .isTrue();
            assertThat(EntitySelector.builder()
                            .withChangesTo(Set.of("dob"))
                            .build()
                            .matches(event))
                    .isTrue();
            assertThat(EntitySelector.builder()
                            .withChangesTo(Set.of("name", "surname", "dob"))
                            .build()
                            .matches(event))
                    .isTrue();
            assertThat(EntitySelector.builder()
                            .withChangesTo(Set.of("name"))
                            .withTxMetadata(Map.of("app", "neo4j-browser"))
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withChangesTo(Set.of("name"))
                            .withAuthenticatedUser("neo4j")
                            .build()
                            .matches(event))
                    .isTrue();
            assertThat(EntitySelector.builder()
                            .withChangesTo(Set.of("name"))
                            .withExecutingUser("test")
                            .build()
                            .matches(event))
                    .isTrue();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.CREATE)
                            .withChangesTo(Set.of("name"))
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withChangesTo(Set.of("id"))
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withChangesTo(Set.of("name", "id"))
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.UPDATE)
                            .withChangesTo(Set.of("dob"))
                            .build()
                            .matches(event))
                    .isTrue();
            assertThat(EntitySelector.builder()
                            .withOperation(EntityOperation.UPDATE)
                            .withChangesTo(Set.of("name", "id", "dob"))
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withChangesTo(Set.of("name"))
                            .withTxMetadata(Map.of("app", "cypher-shell"))
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withChangesTo(Set.of("name"))
                            .withAuthenticatedUser("unknown")
                            .build()
                            .matches(event))
                    .isFalse();
            assertThat(EntitySelector.builder()
                            .withChangesTo(Set.of("name"))
                            .withExecutingUser("unknown")
                            .build()
                            .matches(event))
                    .isFalse();
        });
    }

    @Test
    void nodeSelectorMatchesNodeEvents() {
        var event = nodeCreateEvent();

        assertThat(NodeSelector.builder().build().matches(event)).isTrue();
        assertThat(NodeSelector.builder()
                        .withOperation(EntityOperation.CREATE)
                        .build()
                        .matches(event))
                .isTrue();
        assertThat(NodeSelector.builder()
                        .withOperation(EntityOperation.DELETE)
                        .build()
                        .matches(event))
                .isFalse();
        assertThat(NodeSelector.builder()
                        .withOperation(EntityOperation.CREATE)
                        .withChangesTo(Set.of("name"))
                        .build()
                        .matches(event))
                .isTrue();
        assertThat(NodeSelector.builder()
                        .withOperation(EntityOperation.CREATE)
                        .withChangesTo(Set.of("name", "id"))
                        .build()
                        .matches(event))
                .isTrue();
        assertThat(NodeSelector.builder()
                        .withOperation(EntityOperation.CREATE)
                        .withChangesTo(Set.of("name", "id", "dob"))
                        .build()
                        .matches(event))
                .isFalse();
        assertThat(NodeSelector.builder().withLabels(Set.of("Person")).build().matches(event))
                .isTrue();
        assertThat(NodeSelector.builder()
                        .withLabels(Set.of("Person", "Employee"))
                        .build()
                        .matches(event))
                .isTrue();
        assertThat(NodeSelector.builder()
                        .withLabels(Set.of("Employee", "Person"))
                        .build()
                        .matches(event))
                .isTrue();
        assertThat(NodeSelector.builder().withLabels(Set.of("Company")).build().matches(event))
                .isFalse();
        assertThat(NodeSelector.builder()
                        .withLabels(Set.of("Person", "Company"))
                        .build()
                        .matches(event))
                .isFalse();
        assertThat(NodeSelector.builder()
                        .withOperation(EntityOperation.CREATE)
                        .withLabels(Set.of("Employee", "Person"))
                        .build()
                        .matches(event))
                .isTrue();
        assertThat(NodeSelector.builder()
                        .withOperation(EntityOperation.UPDATE)
                        .withLabels(Set.of("Employee", "Person"))
                        .build()
                        .matches(event))
                .isFalse();
        assertThat(NodeSelector.builder().withKey(Map.of("id", 1L)).build().matches(event))
                .isTrue();
        assertThat(NodeSelector.builder()
                        .withKey(Map.of("id", 1L, "dob", "1990"))
                        .build()
                        .matches(event))
                .isFalse();
        assertThat(NodeSelector.builder()
                        .withKey(Map.of("id", 1L, "role", "manager"))
                        .build()
                        .matches(event))
                .isTrue();
        assertThat(NodeSelector.builder()
                        .withLabels(Set.of("Employee"))
                        .withKey(Map.of("id", 1L, "role", "manager"))
                        .build()
                        .matches(event))
                .isTrue();
        assertThat(NodeSelector.builder()
                        .withLabels(Set.of("Employee", "Person"))
                        .withKey(Map.of("id", 1L, "role", "manager"))
                        .build()
                        .matches(event))
                .isTrue();
        assertThat(NodeSelector.builder()
                        .withLabels(Set.of("Person"))
                        .withKey(Map.of("id", 1L, "role", "manager"))
                        .build()
                        .matches(event))
                .isTrue();
        assertThat(NodeSelector.builder()
                        .withLabels(Set.of("Person", "Manager"))
                        .withKey(Map.of("id", 1L, "role", "manager"))
                        .build()
                        .matches(event))
                .isFalse();
        assertThat(NodeSelector.builder()
                        .withKey(Map.of("id", 1L, "name", "acme corp", "prop", false))
                        .build()
                        .matches(event))
                .isFalse();

        assertThat(NodeSelector.builder().build().matches(relationshipCreateEvent()))
                .isFalse();
    }

    @Test
    void relationshipSelectorMatches() {
        var createEvent = relationshipCreateEvent();

        assertThat(RelationshipSelector.builder().build().matches(createEvent)).isTrue();
        assertThat(RelationshipSelector.builder()
                        .withOperation(EntityOperation.CREATE)
                        .build()
                        .matches(createEvent))
                .isTrue();
        assertThat(RelationshipSelector.builder()
                        .withOperation(EntityOperation.DELETE)
                        .build()
                        .matches(createEvent))
                .isFalse();

        assertThat(RelationshipSelector.builder().withType("WORKS_FOR").build().matches(createEvent))
                .isTrue();
        assertThat(RelationshipSelector.builder().withType("KNOWS").build().matches(createEvent))
                .isFalse();
        assertThat(RelationshipSelector.builder()
                        .withType("WORKS_FOR")
                        .withStart(RelationshipNodeSelector.builder()
                                .withLabels(Set.of("Person"))
                                .build())
                        .build()
                        .matches(createEvent))
                .isTrue();
        assertThat(RelationshipSelector.builder()
                        .withType("WORKS_FOR")
                        .withEnd(RelationshipNodeSelector.builder()
                                .withLabels(Set.of("Company"))
                                .build())
                        .build()
                        .matches(createEvent))
                .isTrue();
        assertThat(RelationshipSelector.builder()
                        .withType("WORKS_FOR")
                        .withStart(RelationshipNodeSelector.builder()
                                .withLabels(Set.of("Person"))
                                .build())
                        .withEnd(RelationshipNodeSelector.builder()
                                .withLabels(Set.of("Company"))
                                .build())
                        .build()
                        .matches(createEvent))
                .isTrue();
        assertThat(RelationshipSelector.builder()
                        .withType("WORKS_FOR")
                        .withStart(RelationshipNodeSelector.builder()
                                .withLabels(Set.of("Person"))
                                .withKey(Map.of("id", 1L))
                                .build())
                        .withEnd(RelationshipNodeSelector.builder()
                                .withLabels(Set.of("Company"))
                                .withKey(Map.of("id", 5L))
                                .build())
                        .build()
                        .matches(createEvent))
                .isTrue();
        assertThat(RelationshipSelector.builder()
                        .withType("WORKS_FOR")
                        .withStart(RelationshipNodeSelector.builder()
                                .withLabels(Set.of("Person"))
                                .withKey(Map.of("id", 1L))
                                .build())
                        .withEnd(RelationshipNodeSelector.builder()
                                .withLabels(Set.of("Company"))
                                .withKey(Map.of("id", 5L))
                                .build())
                        .withKey(Map.of("year", 1990L))
                        .build()
                        .matches(createEvent))
                .isTrue();

        assertThat(RelationshipSelector.builder()
                        .withType("WORKS_FOR")
                        .withStart(RelationshipNodeSelector.builder()
                                .withLabels(Set.of("Person", "Employee"))
                                .withKey(Map.of("id", 1L))
                                .build())
                        .withEnd(RelationshipNodeSelector.builder()
                                .withLabels(Set.of("Company"))
                                .withKey(Map.of("id", 5L))
                                .build())
                        .withKey(Map.of("year", 1990L))
                        .build()
                        .matches(createEvent))
                .isFalse();
        assertThat(RelationshipSelector.builder()
                        .withType("WORKS_FOR")
                        .withStart(RelationshipNodeSelector.builder()
                                .withLabels(Set.of("Person"))
                                .withKey(Map.of("id", 1L))
                                .build())
                        .withEnd(RelationshipNodeSelector.builder()
                                .withLabels(Set.of("Company", "Corportation"))
                                .withKey(Map.of("id", 5L))
                                .build())
                        .withKey(Map.of("year", 1990L))
                        .build()
                        .matches(createEvent))
                .isFalse();
        assertThat(RelationshipSelector.builder()
                        .withType("WORKS_FOR")
                        .withStart(RelationshipNodeSelector.builder()
                                .withLabels(Set.of("Person"))
                                .withKey(Map.of("id", true))
                                .build())
                        .withEnd(RelationshipNodeSelector.builder()
                                .withLabels(Set.of("Company"))
                                .withKey(Map.of("id", 5L))
                                .build())
                        .withKey(Map.of("year", 1990L))
                        .build()
                        .matches(createEvent))
                .isFalse();
        assertThat(RelationshipSelector.builder()
                        .withType("WORKS_FOR")
                        .withStart(RelationshipNodeSelector.builder()
                                .withLabels(Set.of("Person"))
                                .withKey(Map.of("id", 1L))
                                .build())
                        .withEnd(RelationshipNodeSelector.builder()
                                .withLabels(Set.of("Company"))
                                .withKey(Map.of("id", "5"))
                                .build())
                        .withKey(Map.of("year", 1990L))
                        .build()
                        .matches(createEvent))
                .isFalse();
        assertThat(RelationshipSelector.builder()
                        .withType("WORKS_FOR")
                        .withStart(RelationshipNodeSelector.builder()
                                .withLabels(Set.of("Person"))
                                .withKey(Map.of("id", 1L))
                                .build())
                        .withEnd(RelationshipNodeSelector.builder()
                                .withLabels(Set.of("Company"))
                                .withKey(Map.of("id", "5"))
                                .build())
                        .withKey(Map.of("year", "1990"))
                        .build()
                        .matches(createEvent))
                .isFalse();

        assertThat(RelationshipSelector.builder().build().matches(nodeCreateEvent()))
                .isFalse();
    }

    @Test
    void metadataSelectorMatches() {
        List.of(nodeCreateEvent(), nodeUpdateEvent(), nodeDeleteEvent()).forEach(event -> {
            assertThat(EntitySelector.builder()
                            .withAuthenticatedUser("neo4j")
                            .build()
                            .matches(event))
                    .isTrue();

            assertThat(EntitySelector.builder()
                            .withExecutingUser("test")
                            .build()
                            .matches(event))
                    .isTrue();

            assertThat(EntitySelector.builder()
                            .withAuthenticatedUser("neo4j")
                            .withExecutingUser("test")
                            .build()
                            .matches(event))
                    .isTrue();

            assertThat(EntitySelector.builder()
                            .withTxMetadata(Map.of("app.name", "my-super-app"))
                            .build()
                            .matches(event))
                    .isTrue();

            assertThat(EntitySelector.builder()
                            .withTxMetadata(Map.of("app.name", "my-super-app", "app.version", "1.0"))
                            .build()
                            .matches(event))
                    .isTrue();

            assertThat(EntitySelector.builder()
                            .withAuthenticatedUser("neo4j")
                            .withExecutingUser("test")
                            .withTxMetadata(Map.of("app.name", "my-super-app", "app.version", "1.0"))
                            .build()
                            .matches(event))
                    .isTrue();

            assertThat(EntitySelector.builder()
                            .withTxMetadata(
                                    Map.of("app.name", "my-super-app", "app.version", "1.0", "app.user", "unknown"))
                            .build()
                            .matches(event))
                    .isFalse();

            assertThat(EntitySelector.builder()
                            .withAuthenticatedUser("neo4j")
                            .withExecutingUser("test")
                            .withTxMetadata(Map.of("app.name", "my-super-app", "app.version", "2.0"))
                            .build()
                            .matches(event))
                    .isFalse();

            assertThat(EntitySelector.builder()
                            .withAuthenticatedUser("neo4j")
                            .withExecutingUser("neo4j")
                            .withTxMetadata(Map.of("app.name", "my-super-app", "app.version", "1.0"))
                            .build()
                            .matches(event))
                    .isFalse();

            assertThat(EntitySelector.builder()
                            .withAuthenticatedUser("neo4j")
                            .withExecutingUser("test")
                            .withTxMetadata(Map.of(
                                    "app.name",
                                    "my-super-app",
                                    "app.version",
                                    "1.0",
                                    "app.user",
                                    "test",
                                    "app.code",
                                    "no-code"))
                            .build()
                            .matches(event))
                    .isFalse();
        });
    }

    @Test
    void applyFiltersShouldArrangeProperties() {
        List.of(nodeCreateEvent(), relationshipCreateEvent()).forEach(event -> {
            assertThat(EntitySelector.builder().build().applyProperties(event))
                    .extracting("event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("id", "name", "surname");
            assertThat(EntitySelector.builder()
                            .includingProperties(Set.of("*"))
                            .build()
                            .applyProperties(event))
                    .extracting("event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("id", "name", "surname");
            assertThat(EntitySelector.builder()
                            .includingProperties(Set.of("id"))
                            .build()
                            .applyProperties(event))
                    .extracting("event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("id");
            assertThat(EntitySelector.builder()
                            .includingProperties(Set.of("id", "name"))
                            .build()
                            .applyProperties(event))
                    .extracting("event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("id", "name");
            assertThat(EntitySelector.builder()
                            .excludingProperties(Set.of("id"))
                            .build()
                            .applyProperties(event))
                    .extracting("event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("name", "surname")
                    .doesNotContainKey("id");
            assertThat(EntitySelector.builder()
                            .excludingProperties(Set.of("id", "name"))
                            .build()
                            .applyProperties(event))
                    .extracting("event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("surname")
                    .doesNotContainKeys("id", "name");
        });

        List.of(nodeDeleteEvent(), relationshipDeleteEvent()).forEach(event -> {
            assertThat(EntitySelector.builder().build().applyProperties(event))
                    .extracting("event.before.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("id", "name", "surname");
            assertThat(EntitySelector.builder()
                            .includingProperties(Set.of("*"))
                            .build()
                            .applyProperties(event))
                    .extracting("event.before.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("id", "name", "surname");
            assertThat(EntitySelector.builder()
                            .includingProperties(Set.of("id"))
                            .build()
                            .applyProperties(event))
                    .extracting("event.before.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("id");
            assertThat(EntitySelector.builder()
                            .includingProperties(Set.of("id", "name"))
                            .build()
                            .applyProperties(event))
                    .extracting("event.before.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("id", "name");
            assertThat(EntitySelector.builder()
                            .excludingProperties(Set.of("id"))
                            .build()
                            .applyProperties(event))
                    .extracting("event.before.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("name", "surname")
                    .doesNotContainKey("id");
            assertThat(EntitySelector.builder()
                            .excludingProperties(Set.of("id", "name"))
                            .build()
                            .applyProperties(event))
                    .extracting("event.before.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("surname")
                    .doesNotContainKeys("id", "name");
        });

        List.of(nodeUpdateEvent(), relationshipUpdateEvent()).forEach(event -> {
            assertThat(EntitySelector.builder().build().applyProperties(event))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.before.properties",
                                    InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("id", "name", "surname"))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("id", "name", "dob"));
            assertThat(EntitySelector.builder()
                            .includingProperties(Set.of("*"))
                            .build()
                            .applyProperties(event))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.before.properties",
                                    InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("id", "name", "surname"))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("id", "name", "dob"));
            assertThat(EntitySelector.builder()
                            .includingProperties(Set.of("id"))
                            .build()
                            .applyProperties(event))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.before.properties",
                                    InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("id"))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("id"));
            assertThat(EntitySelector.builder()
                            .includingProperties(Set.of("id", "name"))
                            .build()
                            .applyProperties(event))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.before.properties",
                                    InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("id", "name"))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("id", "name"));
            assertThat(EntitySelector.builder()
                            .excludingProperties(Set.of("id"))
                            .build()
                            .applyProperties(event))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.before.properties",
                                    InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("name", "surname")
                            .doesNotContainKey("id"))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("name", "dob")
                            .doesNotContainKey("id"));
            assertThat(EntitySelector.builder()
                            .excludingProperties(Set.of("id", "name"))
                            .build()
                            .applyProperties(event))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.before.properties",
                                    InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("surname")
                            .doesNotContainKeys("id", "name"))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("dob")
                            .doesNotContainKeys("id", "name"));
        });
    }

    @NotNull
    private static ChangeEvent nodeCreateEvent() {
        return new ChangeEvent(
                new ChangeIdentifier("id-1"),
                0L,
                0,
                new Metadata(
                        "neo4j",
                        "test",
                        "server-1",
                        "db",
                        CaptureMode.DIFF,
                        "bolt",
                        "127.0.0.1:50000",
                        "127.0.0.1:7687",
                        ZonedDateTime.now().minusSeconds(5),
                        ZonedDateTime.now(),
                        Map.of("app.name", "my-super-app", "app.version", "1.0"),
                        emptyMap()),
                new NodeEvent(
                        "db:1",
                        EntityOperation.CREATE,
                        List.of("Person", "Employee"),
                        Map.of(
                                "Person",
                                List.of(Map.of("id", 1L), Map.of("name", "John")),
                                "Employee",
                                List.of(Map.of("id", 1L, "role", "manager"))),
                        null,
                        new NodeState(
                                List.of("Person", "Employee"), Map.of("id", 1L, "name", "John", "surname", "Doe"))));
    }

    @NotNull
    private static ChangeEvent nodeDeleteEvent() {
        return new ChangeEvent(
                new ChangeIdentifier("id-1"),
                0L,
                0,
                new Metadata(
                        "neo4j",
                        "test",
                        "server-1",
                        "db",
                        CaptureMode.DIFF,
                        "bolt",
                        "127.0.0.1:50000",
                        "127.0.0.1:7687",
                        ZonedDateTime.now().minusSeconds(5),
                        ZonedDateTime.now(),
                        Map.of("app.name", "my-super-app", "app.version", "1.0", "app.user", "test"),
                        emptyMap()),
                new NodeEvent(
                        "db:1",
                        EntityOperation.DELETE,
                        List.of("Person", "Employee"),
                        Map.of(
                                "Person",
                                List.of(Map.of("id", 1L), Map.of("name", "John")),
                                "Employee",
                                List.of(Map.of("id", 1L, "role", "manager"))),
                        new NodeState(
                                List.of("Person", "Employee"), Map.of("id", 1L, "name", "John", "surname", "Doe")),
                        null));
    }

    @NotNull
    private static ChangeEvent nodeUpdateEvent() {
        return new ChangeEvent(
                new ChangeIdentifier("id-1"),
                0L,
                0,
                new Metadata(
                        "neo4j",
                        "test",
                        "server-1",
                        "db",
                        CaptureMode.DIFF,
                        "bolt",
                        "127.0.0.1:50000",
                        "127.0.0.1:7687",
                        ZonedDateTime.now().minusSeconds(5),
                        ZonedDateTime.now(),
                        Map.of("app.name", "my-super-app", "app.version", "1.0", "app.user", "test"),
                        emptyMap()),
                new NodeEvent(
                        "db:1",
                        EntityOperation.UPDATE,
                        List.of("Person", "Employee"),
                        Map.of(
                                "Person",
                                List.of(Map.of("id", 1L), Map.of("name", "John")),
                                "Employee",
                                List.of(Map.of("id", 1L, "role", "manager"))),
                        new NodeState(
                                List.of("Person", "Employee"), Map.of("id", 1L, "name", "John", "surname", "Doe")),
                        new NodeState(
                                List.of("Person", "Employee"),
                                Map.of("id", 1L, "name", "Jack", "dob", LocalDate.of(1990, 1, 1)))));
    }

    @NotNull
    private static ChangeEvent relationshipCreateEvent() {
        return new ChangeEvent(
                new ChangeIdentifier("id-2"),
                0L,
                1,
                new Metadata(
                        "neo4j",
                        "test",
                        "server-1",
                        "db",
                        CaptureMode.DIFF,
                        "bolt",
                        "127.0.0.1:50000",
                        "127.0.0.1:7687",
                        ZonedDateTime.now().minusSeconds(5),
                        ZonedDateTime.now(),
                        Map.of("app.name", "my-super-app", "app.version", "1.0"),
                        emptyMap()),
                new RelationshipEvent(
                        "db:2",
                        "WORKS_FOR",
                        new Node("db:1", List.of("Person"), Map.of("Person", List.of(Map.of("id", 1L)))),
                        new Node("db:2", List.of("Company"), Map.of("Company", List.of(Map.of("id", 5L)))),
                        List.of(Map.of("year", 1990L), Map.of("name", "John")),
                        EntityOperation.CREATE,
                        null,
                        new RelationshipState(Map.of("id", 1L, "name", "John", "surname", "Doe"))));
    }

    @NotNull
    private static ChangeEvent relationshipDeleteEvent() {
        return new ChangeEvent(
                new ChangeIdentifier("id-2"),
                0L,
                1,
                new Metadata(
                        "neo4j",
                        "test",
                        "server-1",
                        "db",
                        CaptureMode.DIFF,
                        "bolt",
                        "127.0.0.1:50000",
                        "127.0.0.1:7687",
                        ZonedDateTime.now().minusSeconds(5),
                        ZonedDateTime.now(),
                        Map.of("app.name", "my-super-app", "app.version", "1.0", "app.user", "another"),
                        emptyMap()),
                new RelationshipEvent(
                        "db:2",
                        "WORKS_FOR",
                        new Node("db:1", List.of("Person"), Map.of("Person", List.of(Map.of("id", 1L)))),
                        new Node("db:2", List.of("Company"), Map.of("Company", List.of(Map.of("id", 5L)))),
                        List.of(Map.of("year", 1990L), Map.of("name", "John")),
                        EntityOperation.DELETE,
                        new RelationshipState(Map.of("id", 1L, "name", "John", "surname", "Doe")),
                        null));
    }

    @NotNull
    private static ChangeEvent relationshipUpdateEvent() {
        return new ChangeEvent(
                new ChangeIdentifier("id-2"),
                0L,
                1,
                new Metadata(
                        "neo4j",
                        "test",
                        "server-1",
                        "db",
                        CaptureMode.DIFF,
                        "bolt",
                        "127.0.0.1:50000",
                        "127.0.0.1:7687",
                        ZonedDateTime.now().minusSeconds(5),
                        ZonedDateTime.now(),
                        Map.of("app.name", "my-super-app", "app.version", "1.0", "app.user", "another"),
                        emptyMap()),
                new RelationshipEvent(
                        "db:2",
                        "WORKS_FOR",
                        new Node("db:1", List.of("Person"), Map.of("Person", List.of(Map.of("id", 1L)))),
                        new Node("db:2", List.of("Company"), Map.of("Company", List.of(Map.of("id", 5L)))),
                        List.of(Map.of("year", 1990L), Map.of("name", "John")),
                        EntityOperation.UPDATE,
                        new RelationshipState(Map.of("id", 1L, "name", "John", "surname", "Doe")),
                        new RelationshipState(Map.of("id", 1L, "name", "Jack", "dob", LocalDate.of(1990, 1, 1)))));
    }
}
