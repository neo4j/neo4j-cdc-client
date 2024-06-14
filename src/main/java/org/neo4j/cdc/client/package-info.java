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
/**
 * An internal client implementation for Neo4j Change Data Capture feature.
 * <p>
 * <strong>This is an INTERNAL library designed to be used inside official Neo4j connectors and no guarantees
 * are made regarding API stability and support.</strong>
 *
 * <p>
 * A minimal instance of a CDC Client for database <strong>my_db</strong> can be created using;
 *
 * <pre>{@code
 *     new CDCClient(driver, () -> SessionConfig.forDatabase("my_db"));
 * }</pre>
 *
 * <p>
 * If you want to include some selectors so that you can filter change events, you can also provide selectors during
 * construction.
 *
 * <p>
 * For example, the following code block will create a CDC Client that only returns {@code Create} and {@code Update}
 * change events for both nodes and relationships.
 * <pre>{@code
 *     new CDCClient(driver, () -> SessionConfig.forDatabase("my_db"),
 *          EntitySelector.builder().withOperation(EntityOperation.CREATE).build(),
 *          EntitySelector.builder().withOperation(EntityOperation.UPDATE).build());
 * }</pre>
 *
 * Another example that will return changes for nodes with label {@code Person} and for relationships with type
 * {@code KNOWS} between {@code Person} nodes is as follows;
 * <pre>{@code
 *     new CDCClient(driver, () -> SessionConfig.forDatabase("my_db"),
 *          NodeSelector.builder().withLabels(Set.of("Person")).build(),
 *          RelationshipSelector.builder().withType("KNOWS")
 *              .withStart(RelationshipNodeSelector.builder().withLabels(Set.of("Person")).build())
 *              .withEnd(RelationshipNodeSelector.builder().withLabels(Set.of("Person")).build())
 *              .build());
 * }</pre>
 *
 */
package org.neo4j.cdc.client;
