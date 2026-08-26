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

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.collections4.MapUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Change identifier that identifies a change record.
 */
public class ChangeIdentifier {
    private static final String ID_FIELD = "id";
    private static final String TX_COMMIT_TIME_FIELD = "txCommitTime";

    private final String id;
    private final ZonedDateTime txCommitTime;

    public ChangeIdentifier(@NotNull String id) {
        this(id, null);
    }

    public ChangeIdentifier(@NotNull String id, ZonedDateTime txCommitTime) {
        this.id = Objects.requireNonNull(id);
        this.txCommitTime = txCommitTime;
    }

    /**
     * Builds a change identifier from a {@code db.cdc.current} or {@code db.cdc.earliest} record.
     *
     * <p>{@code txCommitTime} is yielded by {@code db.cdc.current} on new enough servers only, and is never
     * yielded by {@code db.cdc.earliest}. When it is absent, {@link #getTxCommitTime()} returns {@code null}.
     *
     * @param message record returned by the procedure
     * @return change identifier
     */
    public static ChangeIdentifier fromMap(Map<String, Object> message) {
        return new ChangeIdentifier(
                MapUtils.getString(message, ID_FIELD), ModelUtils.getZonedDateTime(message, TX_COMMIT_TIME_FIELD));
    }

    /**
     * Identifier as a string value.
     *
     * @return identifier
     */
    public String getId() {
        return this.id;
    }

    /**
     * Commit time of the transaction this change belongs to.
     *
     * @return transaction commit time, or {@code null} if the server does not surface it
     */
    public ZonedDateTime getTxCommitTime() {
        return this.txCommitTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ChangeIdentifier that = (ChangeIdentifier) o;

        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return String.format("ChangeIdentifier{id=%s, txCommitTime=%s}", id, txCommitTime);
    }
}
