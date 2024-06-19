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

import static java.util.Collections.emptySet;

import java.util.*;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.cdc.client.model.*;

/**
 * Provides a means to filter changes regardless of the entity type.
 */
public class EntitySelector implements Selector {

    @Nullable
    private final EntityOperation operation;

    @NotNull
    private final Set<String> changesTo;

    @NotNull
    private final Set<String> includeProperties;

    @NotNull
    private final Set<String> excludeProperties;

    @Nullable
    private final String executingUser;

    @Nullable
    private final String authenticatedUser;

    @NotNull
    private final Map<String, Object> txMetadata;

    protected EntitySelector(
            @Nullable EntityOperation operation,
            @NotNull Set<String> changesTo,
            @Nullable String executingUser,
            @Nullable String authenticatedUser,
            @NotNull Map<String, Object> txMetadata,
            @NotNull Set<String> includeProperties,
            @NotNull Set<String> excludeProperties) {
        this.operation = operation;
        this.changesTo = Objects.requireNonNull(changesTo);
        this.includeProperties = Objects.requireNonNull(includeProperties);
        this.excludeProperties = Objects.requireNonNull(excludeProperties);
        this.executingUser = executingUser;
        this.authenticatedUser = authenticatedUser;
        this.txMetadata = txMetadata;
    }

    /**
     * Operation type performed on the entity.
     *
     * @return operation type
     */
    public @Nullable EntityOperation getOperation() {
        return operation;
    }

    /**
     * Set of property names that have been added/updated or removed on the entity as a result of the change.
     *
     * @return set of properties
     */
    public @NotNull Set<String> getChangesTo() {
        return changesTo;
    }

    /**
     * Executing user that performed the change.
     *
     * @return executing user
     */
    public @Nullable String getExecutingUser() {
        return executingUser;
    }

    /**
     * Authenticated user that performed the change.
     *
     * @return authenticated user
     */
    public @Nullable String getAuthenticatedUser() {
        return authenticatedUser;
    }

    /**
     * Transaction metadata to match on the underlying transaction of the change.
     *
     * @return transaction metadata
     */
    public @NotNull Map<String, Object> getTxMetadata() {
        return txMetadata;
    }

    /**
     * Set of properties to include in the returned change events.
     *
     * @return set of properties
     */
    public @NotNull Set<String> getIncludeProperties() {
        return includeProperties;
    }

    /**
     * Set of properties to exclude from the returned change events.
     *
     * @return set of properties
     */
    public @NotNull Set<String> getExcludeProperties() {
        return excludeProperties;
    }

    /**
     * Checks if a given change event matches this selector.
     *
     * @param e change event to check
     * @return whether the event matches or not
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean matches(ChangeEvent e) {
        if (!(e.getEvent() instanceof EntityEvent<?>)) {
            return false;
        }

        var event = (EntityEvent<State>) e.getEvent();
        if (operation != null && event.getOperation() != operation) {
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

        if (executingUser != null && !e.getMetadata().getExecutingUser().equals(executingUser)) {
            return false;
        }

        if (authenticatedUser != null && !e.getMetadata().getAuthenticatedUser().equals(authenticatedUser)) {
            return false;
        }

        if (!e.getMetadata().getTxMetadata().entrySet().containsAll(txMetadata.entrySet())) {
            return false;
        }

        return true;
    }

    /**
     * Performs desired filtering of property names based on includeProperties and excludeProperties.
     *
     * @param e change event to apply filtering on
     * @return a new change event with filters applied
     */
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

    /**
     * Returns a map representation of this selector to be sent over to server.
     *
     * @return map representation
     */
    @Override
    public Map<String, Object> asMap() {
        var result = new HashMap<String, Object>();

        result.put("select", "e");
        if (operation != null) {
            result.put("operation", operation.shorthand);
        }
        if (!changesTo.isEmpty()) {
            result.put("changesTo", changesTo);
        }
        if (authenticatedUser != null) {
            result.put("authenticatedUser", authenticatedUser);
        }
        if (executingUser != null) {
            result.put("executingUser", executingUser);
        }
        if (!txMetadata.isEmpty()) {
            result.put("txMetadata", txMetadata);
        }

        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EntitySelector that = (EntitySelector) o;
        return operation == that.operation
                && changesTo.equals(that.changesTo)
                && includeProperties.equals(that.includeProperties)
                && excludeProperties.equals(that.excludeProperties)
                && Objects.equals(executingUser, that.executingUser)
                && Objects.equals(authenticatedUser, that.authenticatedUser)
                && txMetadata.equals(that.txMetadata);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(operation);
        result = 31 * result + changesTo.hashCode();
        result = 31 * result + includeProperties.hashCode();
        result = 31 * result + excludeProperties.hashCode();
        result = 31 * result + Objects.hashCode(executingUser);
        result = 31 * result + Objects.hashCode(authenticatedUser);
        result = 31 * result + txMetadata.hashCode();
        return result;
    }

    /**
     * Returns a builder instance for {@link EntitySelector}.
     *
     * @return builder instance
     */
    public static Builder<?, ?> builder() {
        return new Builder<>();
    }

    @SuppressWarnings("unchecked")
    public static class Builder<T extends Builder<?, ?>, S extends EntitySelector> {
        protected EntityOperation operation;
        protected Set<String> changesTo;
        protected Set<String> includeProperties;
        protected Set<String> excludeProperties;
        protected String executingUser;
        protected String authenticatedUser;
        protected Map<String, Object> txMetadata;

        protected Builder() {
            this.operation = null;
            this.changesTo = emptySet();
            this.includeProperties = emptySet();
            this.excludeProperties = emptySet();
            this.executingUser = null;
            this.authenticatedUser = null;
            this.txMetadata = null;
        }

        /**
         * Set an operation type filter on the selector to be built.
         *
         * @param operation operation type
         * @return builder
         */
        public T withOperation(EntityOperation operation) {
            this.operation = operation;
            return (T) this;
        }

        /**
         * Set a filter of added/updated or removed properties on the selector to be built.
         *
         * @param changesTo set of property names
         * @return builder
         */
        public T withChangesTo(Set<String> changesTo) {
            this.changesTo = changesTo;
            return (T) this;
        }

        /**
         * Set a filter of executing user on the selector to be built.
         *
         * @param executingUser executing user
         * @return builder
         */
        public T withExecutingUser(String executingUser) {
            this.executingUser = executingUser;
            return (T) this;
        }
        /**
         * Set a filter of authenticated user on the selector to be built.
         *
         * @param authenticatedUser authenticated user
         * @return builder
         */
        public T withAuthenticatedUser(String authenticatedUser) {
            this.authenticatedUser = authenticatedUser;
            return (T) this;
        }

        /**
         * Set a filter of transaction metadata on the selector to be built.
         *
         * @param txMetadata transaction metadata
         * @return builder
         */
        public T withTxMetadata(Map<String, Object> txMetadata) {
            this.txMetadata = txMetadata;
            return (T) this;
        }

        /**
         * Set which properties to include in the returned change events.
         *
         * @param includeProperties set of property names
         * @return builder
         */
        public T includingProperties(Set<String> includeProperties) {
            this.includeProperties = includeProperties;
            return (T) this;
        }

        /**
         * Set which properties to exclude from the returned change events.
         *
         * @param excludeProperties set of property names
         * @return builder
         */
        public T excludingProperties(Set<String> excludeProperties) {
            this.excludeProperties = excludeProperties;
            return (T) this;
        }

        /**
         * Build the desired selector.
         *
         * @return selector
         */
        public S build() {
            return (S) new EntitySelector(
                    operation,
                    Objects.requireNonNullElseGet(changesTo, Collections::emptySet),
                    executingUser,
                    authenticatedUser,
                    Objects.requireNonNullElseGet(txMetadata, Collections::emptyMap),
                    Objects.requireNonNullElseGet(includeProperties, Collections::emptySet),
                    Objects.requireNonNullElseGet(excludeProperties, Collections::emptySet));
        }
    }
}
