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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.collections4.MapUtils;

/**
 * Describes a change event related to a relationship.
 */
public class RelationshipEvent extends EntityEvent<RelationshipState> {

    private final Node start;
    private final Node end;
    private final String type;
    private final List<Map<String, Object>> keys;

    public RelationshipEvent(
            String elementId,
            String type,
            Node start,
            Node end,
            List<Map<String, Object>> keys,
            EntityOperation operation,
            RelationshipState before,
            RelationshipState after) {
        super(elementId, EventType.RELATIONSHIP, operation, before, after);

        this.start = Objects.requireNonNull(start);
        this.end = Objects.requireNonNull(end);
        this.type = Objects.requireNonNull(type);
        this.keys = keys;
    }

    /**
     * Start node of the relationship.
     *
     * @return start node
     */
    public Node getStart() {
        return this.start;
    }

    /**
     * End node of the relationship.
     *
     * @return end node
     */
    public Node getEnd() {
        return this.end;
    }

    /**
     * Type of the relationship.
     *
     * @return relationship type
     */
    public String getType() {
        return this.type;
    }

    /**
     * 	The keys identifying the changed entity.
     * 	This requires key constraints defined on the changed relationships.
     *
     * @return list of key properties
     */
    public List<Map<String, Object>> getKeys() {
        return this.keys;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        RelationshipEvent that = (RelationshipEvent) o;

        if (!start.equals(that.start)) return false;
        if (!end.equals(that.end)) return false;
        if (!type.equals(that.type)) return false;
        return Objects.equals(keys, that.keys);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + start.hashCode();
        result = 31 * result + end.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + (keys != null ? keys.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format(
                "RelationshipEvent{elementId=%s, start=%s, end=%s, type='%s', keys=%s, operation=%s, before=%s, after=%s}",
                getElementId(), start, end, type, keys, getOperation(), getBefore(), getAfter());
    }

    public static RelationshipEvent fromMap(Map<?, ?> map) {
        var cypherMap = ModelUtils.checkedMap(Objects.requireNonNull(map), String.class, Object.class);

        var elementId = MapUtils.getString(cypherMap, "elementId");
        var operation = EntityOperation.fromShorthand(MapUtils.getString(cypherMap, "operation"));
        var type = MapUtils.getString(cypherMap, "type");
        var start = Node.fromMap(ModelUtils.getMap(cypherMap, "start", String.class, Object.class));
        var end = Node.fromMap(ModelUtils.getMap(cypherMap, "end", String.class, Object.class));
        var key = ModelUtils.getRelationshipKeys(cypherMap);

        var state = ModelUtils.checkedMap(
                Objects.requireNonNull(MapUtils.getMap(cypherMap, "state")), String.class, Object.class);
        var before = RelationshipState.fromMap(MapUtils.getMap(state, "before"));
        var after = RelationshipState.fromMap(MapUtils.getMap(state, "after"));

        return new RelationshipEvent(elementId, type, start, end, key, operation, before, after);
    }
}
