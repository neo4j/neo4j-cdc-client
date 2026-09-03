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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.jspecify.annotations.NonNull;
import org.neo4j.cdc.client.model.ChangeEvent;
import org.neo4j.cdc.client.model.ChangeIdentifier;
import org.neo4j.cdc.client.selector.Selector;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.reactivestreams.ReactiveResult;
import org.neo4j.driver.reactivestreams.ReactiveSession;
import org.neo4j.driver.reactivestreams.ReactiveTransactionCallback;
import org.neo4j.driver.types.MapAccessor;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Default {@link CDCService} implementation.
 */
public class CDCClient implements CDCService {
    private final Logger log = LoggerFactory.getLogger(CDCClient.class);

    private static final String CDC_EARLIEST_STATEMENT = "call db.cdc.earliest()";
    private static final String CDC_CURRENT_STATEMENT = "call db.cdc.current()";
    private static final String CDC_QUERY_STATEMENT = "call db.cdc.query($from, $selectors)";
    private final Driver driver;
    private final List<Selector> selectors;
    private final SessionConfigSupplier sessionConfigSupplier;
    private final TransactionConfigSupplier transactionConfigSupplier;
    private final Duration streamingPollInterval;

    private final String currentStatement;

    /**
     * Construct an instance from a driver and an optional list of selectors.
     *
     * @param driver Driver instance to use
     * @param selectors List of selectors to query changes for
     *
     * @see Selector
     */
    public CDCClient(Driver driver, Selector... selectors) {
        this(driver, Duration.ofSeconds(1), selectors);
    }

    /**
     * Construct an instance from a driver, a poll interval and an optional list of selectors.
     *
     * @param driver Driver instance to use
     * @param streamingPollInterval Polling interval to mimic streaming when using @link{stream} method
     * @param selectors List of selectors to query changes for
     *
     * @see Selector
     */
    public CDCClient(Driver driver, Duration streamingPollInterval, Selector... selectors) {
        this(
                driver,
                () -> SessionConfig.builder().build(),
                () -> TransactionConfig.builder().build(),
                streamingPollInterval,
                null,
                selectors);
    }

    /**
     * Construct an instance from a driver, a session config supplier and an optional list of selectors.
     *
     * @param driver Driver instance to use
     * @param sessionConfigSupplier a supplier to customise session configuration
     * @param selectors List of selectors to query changes for
     *
     * @see Selector
     */
    public CDCClient(Driver driver, SessionConfigSupplier sessionConfigSupplier, Selector... selectors) {
        this(driver, sessionConfigSupplier, Duration.ofSeconds(1), selectors);
    }

    /**
     * Construct an instance from a driver, a session config supplier, a poll interval and an optional list of selectors.
     *
     * @param driver Driver instance to use
     * @param sessionConfigSupplier a supplier to customise session configuration
     * @param streamingPollInterval Polling interval to mimic streaming when using @link{stream} method
     * @param selectors List of selectors to query changes for
     *
     * @see Selector
     */
    public CDCClient(
            Driver driver,
            SessionConfigSupplier sessionConfigSupplier,
            Duration streamingPollInterval,
            Selector... selectors) {
        this(
                driver,
                sessionConfigSupplier,
                () -> TransactionConfig.builder().build(),
                streamingPollInterval,
                null,
                selectors);
    }

    /**
     * Construct an instance from a driver, a session config supplier, a transaction config supplier, a poll interval and an optional list of selectors.
     *
     * @param driver Driver instance to use
     * @param sessionConfigSupplier a supplier to customise session configuration
     * @param transactionConfigSupplier a supplier to customise transaction configuration
     * @param streamingPollInterval Polling interval to mimic streaming when using @link{stream} method
     * @param selectors List of selectors to query changes for
     *
     * @see Selector
     */
    public CDCClient(
            Driver driver,
            SessionConfigSupplier sessionConfigSupplier,
            TransactionConfigSupplier transactionConfigSupplier,
            Duration streamingPollInterval,
            Selector... selectors) {
        this(driver, sessionConfigSupplier, transactionConfigSupplier, streamingPollInterval, null, selectors);
    }

    /**
     * Construct an instance that runs {@code db.cdc.current} under an explicit Cypher version.
     *
     * <p>{@code db.cdc.current} only yields {@code txCommitTime} under Cypher 25, and an unprefixed
     * statement runs under the database's default language. Passing {@code "25"} here forces it
     * regardless of that default.
     *
     * <p><strong>The caller must verify the server supports that Cypher version.</strong> The
     * prefix is a syntax error otherwise, and because {@code db.cdc.current} runs first in the
     * query transaction, that would fail every CDC query rather than degrading gracefully.
     *
     * @param driver Driver instance to use
     * @param sessionConfigSupplier a supplier to customise session configuration
     * @param transactionConfigSupplier a supplier to customise transaction configuration
     * @param streamingPollInterval Polling interval to mimic streaming when using @link{stream} method
     * @param cypherVersion Cypher version to prefix {@code db.cdc.current} with, e.g. {@code "25"};
     *     {@code null} or blank leaves the statement unprefixed
     * @param selectors List of selectors to query changes for
     *
     * @see Selector
     */
    public CDCClient(
            Driver driver,
            SessionConfigSupplier sessionConfigSupplier,
            TransactionConfigSupplier transactionConfigSupplier,
            Duration streamingPollInterval,
            String cypherVersion,
            Selector... selectors) {
        this.driver = Objects.requireNonNull(driver);
        this.sessionConfigSupplier = sessionConfigSupplier;
        this.transactionConfigSupplier = transactionConfigSupplier;
        this.streamingPollInterval = Objects.requireNonNull(streamingPollInterval);
        this.selectors = selectors == null ? List.of() : Arrays.asList(selectors);
        this.currentStatement = buildCurrentStatement(cypherVersion);
    }

    /**
     * Builds the {@code db.cdc.current} statement, pinned to an explicit Cypher version when one is
     * given. Visible for testing.
     *
     * @param cypherVersion Cypher version to pin to, e.g. {@code "25"}; {@code null} or blank
     *     leaves the statement unprefixed so it runs under the database's default language
     * @return the statement to execute
     */
    static String buildCurrentStatement(String cypherVersion) {
        return (cypherVersion == null || cypherVersion.isBlank())
                ? CDC_CURRENT_STATEMENT
                : "CYPHER " + cypherVersion + " " + CDC_CURRENT_STATEMENT;
    }

    @Override
    public Mono<ChangeIdentifier> earliest() {
        return queryForChangeIdentifier(CDC_EARLIEST_STATEMENT, "db.cdc.earliest");
    }

    @Override
    public Mono<ChangeIdentifier> current() {
        return queryForChangeIdentifier(currentStatement, "db.cdc.current");
    }

    @Override
    public Flux<ChangeEvent> query(ChangeIdentifier from) {
        return query(from, changeId -> {
            // no-op
        });
    }

    @Override
    public Flux<ChangeEvent> query(
            ChangeIdentifier from, Consumer<ChangeIdentifier> lastKnownChangeIdentifierWhenNoResults) {
        var sessionConfig = sessionConfigSupplier.sessionConfig();

        return Flux.usingWhen(
                        Mono.fromSupplier(() -> driver.session(ReactiveSession.class, sessionConfig)),
                        (ReactiveSession session) -> {
                            if (sessionConfig.defaultAccessMode() == AccessMode.WRITE) {
                                return Flux.from(session.executeWrite(
                                        queryChangesWork(from, lastKnownChangeIdentifierWhenNoResults),
                                        transactionConfigSupplier.transactionConfig()));
                            } else {
                                return Flux.from(session.executeRead(
                                        queryChangesWork(from, lastKnownChangeIdentifierWhenNoResults),
                                        transactionConfigSupplier.transactionConfig()));
                            }
                        },
                        ReactiveSession::close)
                .map(this::applyPropertyFilters)
                .doOnSubscribe(s -> log.trace("subscribed to cdc query"))
                .doOnComplete(() -> log.trace("subscription to cdc query completed"));
    }

    private @NonNull ReactiveTransactionCallback<Publisher<ChangeEvent>> queryChangesWork(
            ChangeIdentifier from, Consumer<ChangeIdentifier> lastKnownChangeIdentifierWhenNoResults) {
        return tx -> {
            var current = Mono.from(tx.run(currentStatement))
                    .flatMap(result -> Mono.from(result.records()))
                    .map(MapAccessor::asMap)
                    .map(ResultMapper::parseChangeIdentifier);

            var params = Map.of(
                    "from",
                    from.getId(),
                    "selectors",
                    selectors.stream().map(Selector::asMap).collect(Collectors.toList()));

            return current.flatMapMany(changeId -> {
                log.trace("running db.cdc.query using parameters {}", params);
                return Flux.from(tx.run(CDC_QUERY_STATEMENT, params))
                        .flatMap(ReactiveResult::records)
                        .map(MapAccessor::asMap)
                        .map(ResultMapper::parseChangeEvent)
                        .switchIfEmpty(Flux.defer(() -> {
                            log.info("no new changes, reporting last seen change id as {}", changeId);
                            lastKnownChangeIdentifierWhenNoResults.accept(changeId);
                            return Flux.empty();
                        }));
            });
        };
    }

    public Flux<ChangeEvent> stream(ChangeIdentifier from) {
        var sessionConfig = sessionConfigSupplier.sessionConfig();
        var cursor = new AtomicReference<>(from);

        var query = Flux.usingWhen(
                Mono.fromSupplier(() -> driver.session(ReactiveSession.class, sessionConfig)),
                (ReactiveSession session) -> {
                    if (sessionConfig.defaultAccessMode() == AccessMode.WRITE) {
                        return Flux.from(session.executeWrite(
                                streamChangesWork(cursor), transactionConfigSupplier.transactionConfig()));
                    } else {
                        return Flux.from(session.executeRead(
                                streamChangesWork(cursor), transactionConfigSupplier.transactionConfig()));
                    }
                },
                ReactiveSession::close);

        return Flux.concat(query, Mono.delay(streamingPollInterval).mapNotNull(x -> null))
                .map(this::applyPropertyFilters)
                .doOnNext(e -> cursor.set(e.getId()))
                .repeat()
                .doOnSubscribe(s -> log.trace("subscribed to cdc stream"))
                .doOnComplete(() -> log.trace("subscription to cdc stream completed"));
    }

    private @NonNull ReactiveTransactionCallback<Publisher<ChangeEvent>> streamChangesWork(
            AtomicReference<ChangeIdentifier> cursor) {
        return tx -> {
            var current = Mono.from(tx.run(currentStatement))
                    .flatMap(result -> Mono.from(result.records()))
                    .map(MapAccessor::asMap)
                    .map(ResultMapper::parseChangeIdentifier);

            var params = Map.of(
                    "from",
                    cursor.get().getId(),
                    "selectors",
                    selectors.stream().map(Selector::asMap).collect(Collectors.toList()));

            return current.flatMapMany(changeId -> {
                log.trace("running db.cdc.query using parameters {}", params);
                return Flux.from(tx.run(CDC_QUERY_STATEMENT, params))
                        .flatMap(ReactiveResult::records)
                        .map(MapAccessor::asMap)
                        .map(ResultMapper::parseChangeEvent)
                        .switchIfEmpty(Flux.defer(() -> {
                            cursor.set(changeId);
                            return Flux.empty();
                        }));
            });
        };
    }

    private ChangeEvent applyPropertyFilters(ChangeEvent original) {
        if (selectors.isEmpty()) {
            return original;
        }

        for (var selector : selectors) {
            if (selector.matches(original)) {
                return selector.applyProperties(original);
            }
        }

        return original;
    }

    private Mono<ChangeIdentifier> queryForChangeIdentifier(String query, String description) {
        var sessionConfig = sessionConfigSupplier.sessionConfig();
        return Mono.usingWhen(
                        Mono.fromSupplier(() -> driver.session(ReactiveSession.class, sessionConfig)),
                        (ReactiveSession session) -> {
                            if (sessionConfig.defaultAccessMode() == AccessMode.WRITE) {
                                return Mono.from(session.executeWrite(
                                        queryChangeIdentifierWork(query),
                                        transactionConfigSupplier.transactionConfig()));
                            } else {
                                return Mono.from(session.executeRead(
                                        queryChangeIdentifierWork(query),
                                        transactionConfigSupplier.transactionConfig()));
                            }
                        },
                        ReactiveSession::close)
                .doOnSubscribe(s -> log.trace("subscribed to {}", description))
                .doOnSuccess(c -> log.trace("subscription to {} completed with '{}'", description, c))
                .doOnError(t -> log.error("subscription to {} failed", description, t));
    }

    private static @NonNull ReactiveTransactionCallback<Publisher<ChangeIdentifier>> queryChangeIdentifierWork(
            String query) {
        return tx -> Mono.from(tx.run(query))
                .flatMap(result -> Mono.from(result.records()))
                .map(MapAccessor::asMap)
                .map(ResultMapper::parseChangeIdentifier);
    }
}
