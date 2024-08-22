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

import java.util.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.cdc.client.model.*;

/**
 * Provides a means to filter changes for nodes.
 */
public class RelationshipSelector extends EntitySelector {
    @Nullable
    private final String type;

    @NotNull
    private final RelationshipNodeSelector start;

    @NotNull
    private final RelationshipNodeSelector end;

    @NotNull
    private final Map<String, Object> key;

    /**
     * Construct a relationship selector for a specific operation, a list of affected properties and a relationship
     * type, a start node, an end node, a key, specifying which properties to include or exclude in the returned change
     * event and a metadata.
     *
     * @param change operation type
     * @param changesTo list of properties that were all changed
     * @param type relationship type
     * @param start selector for start node
     * @param end selector for end node
     * @param key key properties to match on changed node
     * @param executingUser executing user that performed changes
     * @param authenticatedUser authenticated user that performed changes
     * @param txMetadata tx metadata to match
     * @param includeProperties list of properties to include in the returned change event
     * @param excludeProperties list of properties to exclude in the returned change event
     */
    private RelationshipSelector(
            @Nullable EntityOperation change,
            @NotNull Set<String> changesTo,
            @Nullable String type,
            @NotNull RelationshipNodeSelector start,
            @NotNull RelationshipNodeSelector end,
            @NotNull Map<String, Object> key,
            @Nullable String executingUser,
            @Nullable String authenticatedUser,
            @NotNull Map<String, Object> txMetadata,
            @NotNull Set<String> includeProperties,
            @NotNull Set<String> excludeProperties) {
        super(change, changesTo, executingUser, authenticatedUser, txMetadata, includeProperties, excludeProperties);

        this.type = type;
        this.start = Objects.requireNonNull(start);
        this.end = Objects.requireNonNull(end);
        this.key = Objects.requireNonNull(key);
    }

    /**
     * Relationship type of the changed relationship.
     *
     * @return relationship type
     */
    public @Nullable String getType() {
        return this.type;
    }

    /**
     * Selector for the start node of the changed relationship.
     *
     * @return relationship node selector
     */
    public @NotNull RelationshipNodeSelector getStart() {
        return this.start;
    }

    /**
     * Selector for the end node of the changed relationship.
     *
     * @return relationship node selector
     */
    public @NotNull RelationshipNodeSelector getEnd() {
        return this.end;
    }

    /**
     * Map of property names and values that identifies the relationship.
     * All the property names needs to be part of a Key constraint and values need to match.
     *
     * @return map of property name and values
     */
    public @NotNull Map<String, Object> getKey() {
        return this.key;
    }

    @Override
    public boolean matches(ChangeEvent e) {
        if (!(e.getEvent() instanceof RelationshipEvent)) {
            return false;
        }

        if (!super.matches(e)) {
            return false;
        }

        RelationshipEvent relationshipEvent = (RelationshipEvent) e.getEvent();
        if (type != null && !relationshipEvent.getType().equals(type)) {
            return false;
        }

        if (start.getLabels().stream()
                        .anyMatch(l -> !relationshipEvent.getStart().getLabels().contains(l))
                || (!start.getKey().isEmpty()
                        && relationshipEvent.getStart().getKeys().values().stream()
                                .flatMap(List::stream)
                                .noneMatch(start.getKey()::equals))) {
            return false;
        }

        if (end.getLabels().stream()
                        .anyMatch(l -> !relationshipEvent.getEnd().getLabels().contains(l))
                || (!end.getKey().isEmpty()
                        && relationshipEvent.getEnd().getKeys().values().stream()
                                .flatMap(List::stream)
                                .noneMatch(end.getKey()::equals))) {
            return false;
        }

        if (!key.isEmpty() && !relationshipEvent.getKeys().contains(key)) {
            return false;
        }

        return true;
    }

    @Override
    public Map<String, Object> asMap() {
        var result = new HashMap<>(super.asMap());

        result.put("select", "r");
        if (type != null) {
            result.put("type", type);
        }
        if (!start.isEmpty()) {
            result.put("start", start.asMap());
        }
        if (!end.isEmpty()) {
            result.put("end", end.asMap());
        }
        if (!key.isEmpty()) {
            result.put("key", key);
        }

        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        RelationshipSelector that = (RelationshipSelector) o;

        if (!Objects.equals(type, that.type)) return false;
        if (!start.equals(that.start)) return false;
        if (!end.equals(that.end)) return false;
        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + start.hashCode();
        result = 31 * result + end.hashCode();
        result = 31 * result + key.hashCode();
        return result;
    }

    /**
     * Returns a builder instance for {@link RelationshipSelector}.
     *
     * @return builder instance
     */
    public static RelationshipSelectorBuilder builder() {
        return new RelationshipSelectorBuilder();
    }

    public static class RelationshipSelectorBuilder extends Builder<RelationshipSelectorBuilder, RelationshipSelector> {
        private String type;
        private RelationshipNodeSelector start;
        private RelationshipNodeSelector end;
        private Map<String, Object> key;

        private RelationshipSelectorBuilder() {}

        /**
         * Set a filter on relationship type on the selector to be built.
         *
         * @param type relationship type
         * @return builder
         */
        public RelationshipSelectorBuilder withType(String type) {
            this.type = type;
            return this;
        }

        /**
         * Set a filter on relationship start node on the selector to be built.
         *
         * @param start relationship node selector
         * @return builder
         */
        public RelationshipSelectorBuilder withStart(RelationshipNodeSelector start) {
            this.start = start;
            return this;
        }

        /**
         * Set a filter on relationship end node on the selector to be built.
         *
         * @param end relationship node selector
         * @return builder
         */
        public RelationshipSelectorBuilder withEnd(RelationshipNodeSelector end) {
            this.end = end;
            return this;
        }

        /**
         * Set a filter of key properties on the selector to be built.
         * All the property names needs to be part of a Key constraint and values need to match.
         *
         * @param key map of property names and values
         * @return builder
         */
        public RelationshipSelectorBuilder withKey(Map<String, Object> key) {
            this.key = key;
            return this;
        }

        @Override
        public RelationshipSelector build() {
            return new RelationshipSelector(
                    operation,
                    Objects.requireNonNullElseGet(changesTo, Collections::emptySet),
                    type,
                    Objects.requireNonNullElseGet(
                            start, () -> RelationshipNodeSelector.builder().build()),
                    Objects.requireNonNullElseGet(
                            end, () -> RelationshipNodeSelector.builder().build()),
                    Objects.requireNonNullElseGet(key, Collections::emptyMap),
                    executingUser,
                    authenticatedUser,
                    Objects.requireNonNullElseGet(txMetadata, Collections::emptyMap),
                    Objects.requireNonNullElseGet(includeProperties, Collections::emptySet),
                    Objects.requireNonNullElseGet(excludeProperties, Collections::emptySet));
        }
    }
}
