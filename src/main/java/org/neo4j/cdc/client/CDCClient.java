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
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransactionWork;
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
        this.driver = Objects.requireNonNull(driver);
        this.sessionConfigSupplier = () -> SessionConfig.builder().build();
        this.transactionConfigSupplier = () -> TransactionConfig.builder().build();
        this.streamingPollInterval = Objects.requireNonNull(streamingPollInterval);
        this.selectors = selectors == null ? List.of() : Arrays.asList(selectors);
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
        this.driver = Objects.requireNonNull(driver);
        this.sessionConfigSupplier = sessionConfigSupplier;
        this.transactionConfigSupplier = () -> TransactionConfig.builder().build();
        this.streamingPollInterval = Objects.requireNonNull(streamingPollInterval);
        this.selectors = selectors == null ? List.of() : Arrays.asList(selectors);
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
        this.driver = Objects.requireNonNull(driver);
        this.sessionConfigSupplier = sessionConfigSupplier;
        this.transactionConfigSupplier = transactionConfigSupplier;
        this.streamingPollInterval = Objects.requireNonNull(streamingPollInterval);
        this.selectors = selectors == null ? List.of() : Arrays.asList(selectors);
    }

    @Override
    public Mono<ChangeIdentifier> earliest() {
        return queryForChangeIdentifier(CDC_EARLIEST_STATEMENT, "db.cdc.earliest");
    }

    @Override
    public Mono<ChangeIdentifier> current() {
        return queryForChangeIdentifier(CDC_CURRENT_STATEMENT, "db.cdc.current");
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
                        Mono.fromSupplier(() -> driver.rxSession(sessionConfig)),
                        (RxSession session) -> {
                            if (sessionConfig.defaultAccessMode() == AccessMode.WRITE) {
                                return Flux.from(session.writeTransaction(
                                        queryChangesWork(from, lastKnownChangeIdentifierWhenNoResults),
                                        transactionConfigSupplier.transactionConfig()));
                            } else {
                                return Flux.from(session.readTransaction(
                                        queryChangesWork(from, lastKnownChangeIdentifierWhenNoResults),
                                        transactionConfigSupplier.transactionConfig()));
                            }
                        },
                        RxSession::close)
                .map(this::applyPropertyFilters)
                .doOnSubscribe(s -> log.trace("subscribed to cdc query"))
                .doOnComplete(() -> log.trace("subscription to cdc query completed"));
    }

    private @NonNull RxTransactionWork<Publisher<ChangeEvent>> queryChangesWork(
            ChangeIdentifier from, Consumer<ChangeIdentifier> lastKnownChangeIdentifierWhenNoResults) {
        return tx -> {
            var current = Mono.from(tx.run("CALL db.cdc.current()").records())
                    .map(MapAccessor::asMap)
                    .map(ResultMapper::parseChangeIdentifier);

            var params = Map.of(
                    "from",
                    from.getId(),
                    "selectors",
                    selectors.stream().map(Selector::asMap).collect(Collectors.toList()));

            log.trace("running db.cdc.query using parameters {}", params);
            RxResult result = tx.run(CDC_QUERY_STATEMENT, params);

            return current.flatMapMany(changeId -> Flux.from(result.records())
                    .map(MapAccessor::asMap)
                    .map(ResultMapper::parseChangeEvent)
                    .switchIfEmpty(Flux.defer(() -> {
                        log.info("no new changes, reporting last seen change id as {}", changeId);
                        lastKnownChangeIdentifierWhenNoResults.accept(changeId);
                        return Flux.empty();
                    })));
        };
    }

    public Flux<ChangeEvent> stream(ChangeIdentifier from) {
        var sessionConfig = sessionConfigSupplier.sessionConfig();
        var cursor = new AtomicReference<>(from);

        var query = Flux.usingWhen(
                Mono.fromSupplier(() -> driver.rxSession(sessionConfig)),
                (RxSession session) -> {
                    if (sessionConfig.defaultAccessMode() == AccessMode.WRITE) {
                        return Flux.from(session.writeTransaction(
                                streamChangesWork(cursor), transactionConfigSupplier.transactionConfig()));
                    } else {
                        return Flux.from(session.readTransaction(
                                streamChangesWork(cursor), transactionConfigSupplier.transactionConfig()));
                    }
                },
                RxSession::close);

        return Flux.concat(query, Mono.delay(streamingPollInterval).mapNotNull(x -> null))
                .map(this::applyPropertyFilters)
                .doOnNext(e -> cursor.set(e.getId()))
                .repeat()
                .doOnSubscribe(s -> log.trace("subscribed to cdc stream"))
                .doOnComplete(() -> log.trace("subscription to cdc stream completed"));
    }

    private @NonNull RxTransactionWork<Publisher<ChangeEvent>> streamChangesWork(
            AtomicReference<ChangeIdentifier> cursor) {
        return tx -> {
            var current = Mono.from(tx.run("CALL db.cdc.current()").records())
                    .map(MapAccessor::asMap)
                    .map(ResultMapper::parseChangeIdentifier);

            var params = Map.of(
                    "from",
                    cursor.get().getId(),
                    "selectors",
                    selectors.stream().map(Selector::asMap).collect(Collectors.toList()));

            log.trace("running db.cdc.query using parameters {}", params);
            RxResult result = tx.run(CDC_QUERY_STATEMENT, params);

            return current.flatMapMany(changeId -> Flux.from(result.records())
                    .map(MapAccessor::asMap)
                    .map(ResultMapper::parseChangeEvent)
                    .switchIfEmpty(Flux.defer(() -> {
                        cursor.set(changeId);
                        return Flux.empty();
                    })));
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
                        Mono.fromSupplier(() -> driver.rxSession(sessionConfig)),
                        (RxSession session) -> {
                            if (sessionConfig.defaultAccessMode() == AccessMode.WRITE) {
                                return Mono.from(session.writeTransaction(
                                        queryChangeIdentifierWork(query),
                                        transactionConfigSupplier.transactionConfig()));
                            } else {
                                return Mono.from(session.readTransaction(
                                        queryChangeIdentifierWork(query),
                                        transactionConfigSupplier.transactionConfig()));
                            }
                        },
                        RxSession::close)
                .doOnSubscribe(s -> log.trace("subscribed to {}", description))
                .doOnSuccess(c -> log.trace("subscription to {} completed with '{}'", description, c))
                .doOnError(t -> log.error("subscription to {} failed", description, t));
    }

    private static @NonNull RxTransactionWork<Publisher<ChangeIdentifier>> queryChangeIdentifierWork(String query) {
        return tx -> {
            RxResult result = tx.run(query);
            return Mono.from(result.records()).map(MapAccessor::asMap).map(ResultMapper::parseChangeIdentifier);
        };
    }
}
