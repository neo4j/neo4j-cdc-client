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
import org.neo4j.cdc.client.model.ChangeEvent;
import org.neo4j.cdc.client.model.EntityOperation;
import org.neo4j.cdc.client.model.NodeEvent;

/**
 * Provides a means to filter changes for nodes.
 */
public class NodeSelector extends EntitySelector {
    @NotNull
    private final Set<String> labels;

    @NotNull
    private final Map<String, Object> key;

    private NodeSelector(
            @Nullable EntityOperation change,
            @NotNull Set<String> changesTo,
            @NotNull Set<String> labels,
            @NotNull Map<String, Object> key,
            @Nullable String executingUser,
            @Nullable String authenticatedUser,
            @NotNull Map<String, Object> txMetadata,
            @NotNull Set<String> includeProperties,
            @NotNull Set<String> excludeProperties) {
        super(change, changesTo, executingUser, authenticatedUser, txMetadata, includeProperties, excludeProperties);

        this.labels = Objects.requireNonNull(labels);
        this.key = Objects.requireNonNull(key);
    }

    /**
     * Set of labels that needs to be present on the node.
     *
     * @return set of properties
     */
    public @NotNull Set<String> getLabels() {
        return labels;
    }

    /**
     * Map of property names and values that identifies the node.
     * All the property names needs to be part of a Key constraint and values need to match.
     *
     * @return map of property name and values
     */
    public @NotNull Map<String, Object> getKey() {
        return key;
    }

    @Override
    public boolean matches(ChangeEvent e) {
        if (!(e.getEvent() instanceof NodeEvent)) {
            return false;
        }

        if (!super.matches(e)) {
            return false;
        }

        NodeEvent nodeEvent = (NodeEvent) e.getEvent();
        if (labels.stream().anyMatch(l -> !nodeEvent.getLabels().contains(l))) {
            return false;
        }

        if (!key.isEmpty()
                && nodeEvent.getKeys().values().stream().flatMap(List::stream).noneMatch(key::equals)) {
            return false;
        }

        return true;
    }

    @Override
    public Map<String, Object> asMap() {
        var result = new HashMap<>(super.asMap());

        result.put("select", "n");
        if (!labels.isEmpty()) {
            result.put("labels", labels);
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

        NodeSelector that = (NodeSelector) o;

        if (!labels.equals(that.labels)) return false;
        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + labels.hashCode();
        result = 31 * result + key.hashCode();
        return result;
    }

    /**
     * Returns a builder instance for {@link NodeSelector}.
     *
     * @return builder instance
     */
    public static NodeSelectorBuilder builder() {
        return new NodeSelectorBuilder();
    }

    public static class NodeSelectorBuilder extends Builder<NodeSelectorBuilder, NodeSelector> {
        private Set<String> labels;
        private Map<String, Object> key;

        private NodeSelectorBuilder() {}

        /**
         * Set a filter of labels on the selector to be built.
         * All the labels need to be present on the changed node for a match.
         *
         * @param labels set of labels
         * @return builder
         */
        public NodeSelectorBuilder withLabels(Set<String> labels) {
            this.labels = labels;
            return this;
        }

        /**
         * Set a filter of key properties on the selector to be built.
         * All the property names needs to be part of a Key constraint and values need to match.
         *
         * @param key map of property names and values
         * @return builder
         */
        public NodeSelectorBuilder withKey(Map<String, Object> key) {
            this.key = key;
            return this;
        }

        @Override
        public NodeSelector build() {
            return new NodeSelector(
                    operation,
                    Objects.requireNonNullElseGet(changesTo, Collections::emptySet),
                    Objects.requireNonNullElseGet(labels, Collections::emptySet),
                    Objects.requireNonNullElseGet(key, Collections::emptyMap),
                    executingUser,
                    authenticatedUser,
                    Objects.requireNonNullElseGet(txMetadata, Collections::emptyMap),
                    Objects.requireNonNullElseGet(includeProperties, Collections::emptySet),
                    Objects.requireNonNullElseGet(excludeProperties, Collections::emptySet));
        }
    }
}
