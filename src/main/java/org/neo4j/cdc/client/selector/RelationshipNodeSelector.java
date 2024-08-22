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

/**
 * Describes a selector for relationship start and end nodes.
 */
public class RelationshipNodeSelector {
    @NotNull
    private final Set<String> labels;

    @NotNull
    private final Map<String, Object> key;

    private RelationshipNodeSelector(@NotNull Set<String> labels, @NotNull Map<String, Object> key) {
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

    /**
     * Returns a map representation of this selector to be sent over to server.
     *
     * @return map representation
     */
    public Map<String, Object> asMap() {
        var result = new HashMap<String, Object>();

        if (!labels.isEmpty()) {
            result.put("labels", labels);
        }
        if (!key.isEmpty()) {
            result.put("key", key);
        }

        return result;
    }

    /**
     * Checks whether any filter is set on this selector.
     *
     * @return true if either labels or key is specified
     */
    public boolean isEmpty() {
        return labels.isEmpty() && key.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RelationshipNodeSelector that = (RelationshipNodeSelector) o;

        if (!labels.equals(that.labels)) return false;
        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        int result = labels.hashCode();
        result = 31 * result + key.hashCode();
        return result;
    }

    /**
     * Returns a builder instance for {@link RelationshipNodeSelector}.
     *
     * @return builder instance
     */
    public static RelationshipNodeSelectorBuilder builder() {
        return new RelationshipNodeSelectorBuilder();
    }

    public static class RelationshipNodeSelectorBuilder {
        private Set<String> labels;
        private Map<String, Object> key;

        private RelationshipNodeSelectorBuilder() {}

        /**
         * Set a filter of labels on the selector to be built.
         * All the labels need to be present on the node for a match.
         *
         * @param labels set of labels
         * @return builder
         */
        public RelationshipNodeSelectorBuilder withLabels(Set<String> labels) {
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
        public RelationshipNodeSelectorBuilder withKey(Map<String, Object> key) {
            this.key = key;
            return this;
        }

        /**
         * Build the desired selector.
         *
         * @return selector
         */
        public RelationshipNodeSelector build() {
            return new RelationshipNodeSelector(
                    Objects.requireNonNullElseGet(labels, Collections::emptySet),
                    Objects.requireNonNullElseGet(key, Collections::emptyMap));
        }
    }
}
