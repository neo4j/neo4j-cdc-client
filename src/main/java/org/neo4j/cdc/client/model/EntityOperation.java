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
 * Type of operation performed on an entity.
 */
public enum EntityOperation {
    /**
     * Entity is created.
     */
    CREATE("c"),
    /**
     * Entity is updated.
     */
    UPDATE("u"),
    /**
     * Entity is deleted.
     */
    DELETE("d");

    public final String shorthand;

    EntityOperation(String shorthand) {
        this.shorthand = Objects.requireNonNull(shorthand);
    }

    public static EntityOperation fromShorthand(String shorthand) {
        if (CREATE.shorthand.equalsIgnoreCase(shorthand)) {
            return CREATE;
        } else if (UPDATE.shorthand.equalsIgnoreCase(shorthand)) {
            return UPDATE;
        } else if (DELETE.shorthand.equalsIgnoreCase(shorthand)) {
            return DELETE;
        } else {
            throw new IllegalArgumentException(String.format("unknown event type: %s", shorthand));
        }
    }
}
