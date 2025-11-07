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
package org.neo4j.cdc.client;

import java.util.function.Consumer;
import org.neo4j.cdc.client.model.ChangeEvent;
import org.neo4j.cdc.client.model.ChangeIdentifier;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * <code>CDCService</code> enables callers to track changes happening in a Neo4j database.
 */
public interface CDCService {

    /**
     * Returns the change identifier for the earliest available change.
     *
     * @return change identifier
     */
    Mono<ChangeIdentifier> earliest();

    /**
     * Returns the change identifier for the last committed transaction.
     *
     * @return change identifier
     */
    Mono<ChangeIdentifier> current();

    /**
     * Returns the changes that happened to the database after the given change identifier.
     * The returned Flux completes when we reach the end of change stream.
     *
     * @param from change identifier to query changes from.
     * @return change events
     */
    Flux<ChangeEvent> query(ChangeIdentifier from);

    /**
     * Returns the changes that happened to the database after the given change identifier.
     * The returned Flux completes when we reach the end of change stream.
     *
     * @param from change identifier to query changes from.
     * @param lastKnownChangeIdentifierWhenNoResults a consumer that will be called with the last seen change identifier when no results are found.
     * @return change events
     */
    Flux<ChangeEvent> query(ChangeIdentifier from, Consumer<ChangeIdentifier> lastKnownChangeIdentifierWhenNoResults);

    /**
     * Returns the changes that happened to the database after the given change identifier.
     * The returned Flux does not complete, and continues querying for new changes until the
     * Flux subscription is closed.
     * <p>
     * <i>Change Data Capture feature currently does not support streaming, and this method
     * mimics streaming through polling.</i>
     *
     * @param from change identifier to query changes from.
     * @return change events
     */
    Flux<ChangeEvent> stream(ChangeIdentifier from);
}
