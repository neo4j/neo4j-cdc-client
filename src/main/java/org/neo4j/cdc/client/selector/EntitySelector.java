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
import static java.util.Collections.emptySet;

import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.collections4.MapUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.cdc.client.model.*;

public class EntitySelector implements Selector {

    public static final String METADATA_KEY_AUTHENTICATED_USER = "authenticatedUser";
    public static final String METADATA_KEY_EXECUTING_USER = "executingUser";
    public static final String METADATA_KEY_TX_METADATA = "txMetadata";

    @Nullable
    private final EntityOperation change;

    @NotNull
    private final Set<String> changesTo;

    @NotNull
    private final Set<String> includeProperties;

    @NotNull
    private final Set<String> excludeProperties;

    @NotNull
    private final Map<String, Object> metadata;

    public EntitySelector() {
        this(null);
    }

    public EntitySelector(@Nullable EntityOperation change) {
        this(change, emptySet(), emptyMap());
    }

    public EntitySelector(@Nullable EntityOperation change, @NotNull Set<String> changesTo) {
        this(change, changesTo, emptySet(), emptySet(), emptyMap());
    }

    public EntitySelector(
            @Nullable EntityOperation change, @NotNull Set<String> changesTo, @NotNull Map<String, Object> metadata) {
        this(change, changesTo, emptySet(), emptySet(), metadata);
    }

    public EntitySelector(
            @Nullable EntityOperation change,
            @NotNull Set<String> changesTo,
            @NotNull Set<String> includeProperties,
            @NotNull Set<String> excludeProperties,
            @NotNull Map<String, Object> metadata) {
        this.change = change;
        this.changesTo = Objects.requireNonNull(changesTo);
        this.includeProperties = Objects.requireNonNull(includeProperties);
        this.excludeProperties = Objects.requireNonNull(excludeProperties);
        this.metadata = metadata;
    }

    public @Nullable EntityOperation getChange() {
        return change;
    }

    public @NotNull Set<String> getChangesTo() {
        return changesTo;
    }

    public @NotNull Set<String> getIncludeProperties() {
        return includeProperties;
    }

    public @NotNull Set<String> getExcludeProperties() {
        return excludeProperties;
    }

    public @NotNull Map<String, Object> getMetadata() {
        return metadata;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean matches(ChangeEvent e) {
        if (!(e.getEvent() instanceof EntityEvent<?>)) {
            return false;
        }

        var event = (EntityEvent<State>) e.getEvent();
        if (change != null && event.getOperation() != change) {
            return false;
        }

        if (!changesTo.isEmpty()) {
            switch (event.getOperation()) {
                case CREATE:
                    if (!changesTo.stream()
                            .allMatch(p -> event.getAfter().getProperties().containsKey(p))) {
                        return false;
                    }

                    break;
                case DELETE:
                    if (!changesTo.stream()
                            .allMatch(p -> event.getBefore().getProperties().containsKey(p))) {
                        return false;
                    }

                    break;
                case UPDATE:
                    var allUpdated = changesTo.stream().allMatch(prop -> {
                        if (!event.getBefore().getProperties().containsKey(prop)
                                && !event.getAfter().getProperties().containsKey(prop)) {
                            return false;
                        }

                        var oldValue = event.getBefore().getProperties().get(prop);
                        var newValue = event.getAfter().getProperties().get(prop);
                        return !Objects.equals(oldValue, newValue);
                    });

                    if (!allUpdated) {
                        return false;
                    }

                    break;
            }
        }
        if (!metadata.isEmpty()) {
            Object authenticatedUser = metadata.get(METADATA_KEY_AUTHENTICATED_USER);
            if (authenticatedUser != null
                    && !e.getMetadata().getAuthenticatedUser().equals(authenticatedUser)) {
                return false;
            }
            Object executingUser = metadata.get(METADATA_KEY_EXECUTING_USER);
            if (executingUser != null && !e.getMetadata().getExecutingUser().equals(executingUser)) {
                return false;
            }
            var txMetadata = MapUtils.getMap(metadata, METADATA_KEY_TX_METADATA, emptyMap()).entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> {
                                if (entry.getKey() instanceof String) {
                                    return (String) entry.getKey();
                                }

                                throw new IllegalArgumentException(String.format(
                                        "expected map key to be a String but got '%s'.",
                                        entry.getKey().getClass().getSimpleName()));
                            },
                            entry -> (Object) entry.getValue()));
            if (!e.getMetadata().getTxMetadata().entrySet().containsAll(txMetadata.entrySet())) {
                return false;
            }
        }

        return true;
    }

    @Override
    public ChangeEvent applyProperties(ChangeEvent e) {
        // there is nothing to d
        if (includeProperties.isEmpty() && excludeProperties.isEmpty()) {
            return e;
        }

        switch (e.getEvent().getEventType()) {
            case NODE: {
                var nodeEvent = (NodeEvent) e.getEvent();
                var beforeState = nodeEvent.getBefore();
                if (beforeState != null) {
                    beforeState = new NodeState(beforeState.getLabels(), filterProps(beforeState.getProperties()));
                }

                var afterState = nodeEvent.getAfter();
                if (afterState != null) {
                    afterState = new NodeState(afterState.getLabels(), filterProps(afterState.getProperties()));
                }

                return new ChangeEvent(
                        e.getId(),
                        e.getTxId(),
                        e.getSeq(),
                        e.getMetadata(),
                        new NodeEvent(
                                nodeEvent.getElementId(),
                                nodeEvent.getOperation(),
                                nodeEvent.getLabels(),
                                nodeEvent.getKeys(),
                                beforeState,
                                afterState));
            }
            case RELATIONSHIP: {
                var relationshipEvent = (RelationshipEvent) e.getEvent();
                var beforeState = relationshipEvent.getBefore();
                if (beforeState != null) {
                    beforeState = new RelationshipState(filterProps(beforeState.getProperties()));
                }

                var afterState = relationshipEvent.getAfter();
                if (afterState != null) {
                    afterState = new RelationshipState(filterProps(afterState.getProperties()));
                }

                return new ChangeEvent(
                        e.getId(),
                        e.getTxId(),
                        e.getSeq(),
                        e.getMetadata(),
                        new RelationshipEvent(
                                relationshipEvent.getElementId(),
                                relationshipEvent.getType(),
                                relationshipEvent.getStart(),
                                relationshipEvent.getEnd(),
                                relationshipEvent.getKeys(),
                                relationshipEvent.getOperation(),
                                beforeState,
                                afterState));
            }
        }

        return e;
    }

    @NotNull
    private Map<String, Object> filterProps(Map<String, Object> props) {
        return props.entrySet().stream()
                .filter(entry -> {
                    if (excludeProperties.contains(entry.getKey())) {
                        return false;
                    }

                    return includeProperties.isEmpty()
                            || includeProperties.contains("*")
                            || includeProperties.contains(entry.getKey());
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Map<String, Object> asMap() {
        var result = new HashMap<String, Object>();

        result.put("select", "e");
        if (change != null) {
            result.put("operation", change.shorthand);
        }
        if (!changesTo.isEmpty()) {
            result.put("changesTo", changesTo);
        }
        if (metadata.containsKey(METADATA_KEY_AUTHENTICATED_USER)) {
            result.put("authenticatedUser", metadata.get(METADATA_KEY_AUTHENTICATED_USER));
        }
        if (metadata.containsKey(METADATA_KEY_EXECUTING_USER)) {
            result.put("executingUser", metadata.get(METADATA_KEY_EXECUTING_USER));
        }
        if (metadata.containsKey(METADATA_KEY_TX_METADATA)) {
            result.put("txMetadata", metadata.get(METADATA_KEY_TX_METADATA));
        }

        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EntitySelector that = (EntitySelector) o;

        if (change != that.change) return false;
        if (!changesTo.equals(that.changesTo)) return false;
        if (!includeProperties.equals(that.includeProperties)) return false;
        if (!metadata.equals(that.metadata)) return false;
        return excludeProperties.equals(that.excludeProperties);
    }

    @Override
    public int hashCode() {
        int result = change != null ? change.hashCode() : 0;
        result = 31 * result + changesTo.hashCode();
        result = 31 * result + includeProperties.hashCode();
        result = 31 * result + excludeProperties.hashCode();
        result = 31 * result + metadata.hashCode();
        return result;
    }
}
