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
package org.neo4j.cdc.client.model;

import java.util.Objects;

/**
 * Describes a change event related to a node or a relationship.
 *
 * @param <T> type of state information
 * @see NodeState
 * @see RelationshipState
 */
public abstract class EntityEvent<T extends State> implements Event {

    private final String elementId;
    private final EventType eventType;
    private final T before;
    private final T after;
    private final EntityOperation operation;

    protected EntityEvent(String elementId, EventType eventType, EntityOperation operation, T before, T after) {
        this.elementId = Objects.requireNonNull(elementId);
        this.eventType = Objects.requireNonNull(eventType);
        this.operation = Objects.requireNonNull(operation);
        this.before = before;
        this.after = after;
    }

    /**
     * The elementId of the changed entity (node or relationship).
     *
     * @return element id
     */
    public String getElementId() {
        return this.elementId;
    }

    /**
     * Type of the changed entity.
     *
     * @return event type
     */
    public EventType getEventType() {
        return this.eventType;
    }

    /**
     * Type of the operation.
     *
     * @return operation type
     */
    public EntityOperation getOperation() {
        return this.operation;
    }

    /**
     * The state of the entity before the change.
     *
     * @return state
     */
    public T getBefore() {
        return this.before;
    }

    /**
     * The state of the entity after the change.
     *
     * @return state
     */
    public T getAfter() {
        return this.after;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EntityEvent<?> that = (EntityEvent<?>) o;

        if (!elementId.equals(that.elementId)) return false;
        if (eventType != that.eventType) return false;
        if (!Objects.equals(before, that.before)) return false;
        if (!Objects.equals(after, that.after)) return false;
        return operation == that.operation;
    }

    @Override
    public int hashCode() {
        int result = elementId.hashCode();
        result = 31 * result + eventType.hashCode();
        result = 31 * result + (before != null ? before.hashCode() : 0);
        result = 31 * result + (after != null ? after.hashCode() : 0);
        result = 31 * result + operation.hashCode();
        return result;
    }
}
