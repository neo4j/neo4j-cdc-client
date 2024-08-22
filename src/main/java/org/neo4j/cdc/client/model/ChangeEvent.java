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
 * Change event.
 */
public class ChangeEvent {

    private final ChangeIdentifier id;
    private final Long txId;
    private final Integer seq;
    private final Metadata metadata;
    private final Event event;

    public ChangeEvent(ChangeIdentifier id, Long txId, Integer seq, Metadata metadata, Event event) {
        this.id = Objects.requireNonNull(id);
        this.txId = Objects.requireNonNull(txId);
        this.seq = Objects.requireNonNull(seq);
        this.metadata = Objects.requireNonNull(metadata);
        this.event = Objects.requireNonNull(event);
    }

    /**
     * A unique change identifier that identifies this change record.
     * It can be used to query changes from this change onward.
     *
     * @return change identifier
     */
    public ChangeIdentifier getId() {
        return this.id;
    }

    /**
     * A number identifying which transaction the change happened in, unique in combination with seq.
     * Transaction identifiers are not continuous (some transactions, such as system and schema commands,
     * are not recorded in change data capture and cause gaps in the transaction identifiers).
     *
     * @return transaction id
     */
    public Long getTxId() {
        return this.txId;
    }

    /**
     * A number used for ordering changes that happened in the same transaction.
     * The order of changes observed in the output does not necessarily correspond to the order in which
     * changes were applied during the transaction.
     *
     * @return sequence
     */
    public Integer getSeq() {
        return this.seq;
    }

    /**
     * Other useful information about the transaction.
     *
     * @return metadata
     */
    public Metadata getMetadata() {
        return this.metadata;
    }

    /**
     * Details about the actual data change.
     *
     * @return event
     */
    public Event getEvent() {
        return this.event;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ChangeEvent that = (ChangeEvent) o;

        if (!id.equals(that.id)) return false;
        if (!txId.equals(that.txId)) return false;
        if (!seq.equals(that.seq)) return false;
        if (!metadata.equals(that.metadata)) return false;
        return event.equals(that.event);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + txId.hashCode();
        result = 31 * result + seq.hashCode();
        result = 31 * result + metadata.hashCode();
        result = 31 * result + event.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format(
                "ChangeEvent{id=%s, txId=%s, seq=%s, metadata=%s, event=%s}", id, txId, seq, metadata, event);
    }
}
