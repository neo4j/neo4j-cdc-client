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
package org.neo4j.cdc.client;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.cdc.client.model.*;

public class ResultMapperTest {

    private static final String CHANGE_IDENTIFIER_VALUE = "AlvVSy-4s0yaia31SXmHHz8AAAAAAAAACgAAAAAAAAAA";

    @Test
    void shouldParseChangeIdentifier() {
        String changeIdentifierValue = CHANGE_IDENTIFIER_VALUE;
        Map<String, Object> message = new HashMap<>();
        message.put("id", changeIdentifierValue);
        ChangeIdentifier result = ResultMapper.parseChangeIdentifier(message);
        assertEquals(changeIdentifierValue, result.getId());
        assertNull(result.getTxId());
        assertNull(result.getTxStartTime());
        assertNull(result.getTxCommitTime());
    }

    @Test
    void shouldParseChangeIdentifierWithTransactionDetails() {
        String changeIdentifierValue = CHANGE_IDENTIFIER_VALUE;
        var txStartTime = ZonedDateTime.parse("2023-08-17T09:14:35.636Z");
        var txCommitTime = ZonedDateTime.parse("2023-08-17T09:14:35.666Z");

        Map<String, Object> message = new HashMap<>();
        message.put("id", changeIdentifierValue);
        message.put("txId", 3L);
        message.put("txStartTime", txStartTime);
        message.put("txCommitTime", txCommitTime);

        ChangeIdentifier result = ResultMapper.parseChangeIdentifier(message);
        assertEquals(changeIdentifierValue, result.getId());
        assertEquals(3L, result.getTxId());
        assertEquals(txStartTime, result.getTxStartTime());
        assertEquals(txCommitTime, result.getTxCommitTime());
    }

    @Test
    void shouldParseChangeIdentifierWithTransactionDetailsAsStrings() {
        Map<String, Object> message = new HashMap<>();
        message.put("id", CHANGE_IDENTIFIER_VALUE);
        message.put("txId", 3L);
        message.put("txStartTime", "2023-08-17T09:14:35.636000000Z");
        message.put("txCommitTime", "2023-08-17T09:14:35.666000000Z");

        ChangeIdentifier result = ResultMapper.parseChangeIdentifier(message);
        assertEquals(ZonedDateTime.parse("2023-08-17T09:14:35.636Z"), result.getTxStartTime());
        assertEquals(ZonedDateTime.parse("2023-08-17T09:14:35.666Z"), result.getTxCommitTime());
    }

    @Test
    void shouldChangeIdentifierEqualityOnlyDependOnId() {
        String changeIdentifierValue = CHANGE_IDENTIFIER_VALUE;
        var withoutDetails = new ChangeIdentifier(changeIdentifierValue);
        var withDetails = new ChangeIdentifier(
                changeIdentifierValue,
                3L,
                ZonedDateTime.parse("2023-08-17T09:14:35.636Z"),
                ZonedDateTime.parse("2023-08-17T09:14:35.666Z"));

        assertEquals(withoutDetails, withDetails);
        assertEquals(withoutDetails.hashCode(), withDetails.hashCode());
    }

    @Test
    void shouldParseCompleteChangeNodeEventRecord() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("executingUser", "neo4j");
        metadata.put("connectionClient", "172.17.0.1:44484");
        metadata.put("authenticatedUser", "neo4j");
        metadata.put("captureMode", "FULL");
        metadata.put("serverId", "60b75468");
        metadata.put("connectionType", "bolt");
        metadata.put("connectionServer", "172.17.0.2:7687");
        metadata.put("txStartTime", "2023-08-17T09:14:35.636000000Z");
        metadata.put("txCommitTime", "2023-08-17T09:14:35.666000000Z");

        Map<String, Object> properties = new HashMap<>();
        properties.put("name", "someone");
        properties.put("real_name", "Some real name");
        Map<String, Object> afterState = new HashMap<>();
        afterState.put("properties", properties);
        afterState.put("labels", Collections.singletonList("User"));
        Map<String, Object> state = new HashMap<>();
        state.put("before", null);
        state.put("after", afterState);

        Map<String, Object> event = new HashMap<>();
        event.put("elementId", "4:5bd54b2f-b8b3-4c9a-89ad-f54979871f3f:0");
        event.put("keys", emptyMap());
        event.put("eventType", "n");
        event.put("state", state);
        event.put("operation", "c");
        event.put("labels", Collections.singletonList("User"));

        Map<String, Object> message = new HashMap<>();
        message.put("id", CHANGE_IDENTIFIER_VALUE);
        message.put("txId", 3L);
        message.put("seq", 1L);
        message.put("metadata", metadata);
        message.put("event", event);

        ChangeEvent changeEvent = ResultMapper.parseChangeEvent(message);
        assertEquals(CHANGE_IDENTIFIER_VALUE, changeEvent.getId().getId());
        assertEquals(3L, changeEvent.getTxId());
        assertEquals(1, changeEvent.getSeq());

        checkMetadata(changeEvent.getMetadata());
        Event changeEventEvent = changeEvent.getEvent();
        assertInstanceOf(NodeEvent.class, changeEventEvent);
        NodeEvent nodeEvent = (NodeEvent) changeEventEvent;
        assertEquals("4:5bd54b2f-b8b3-4c9a-89ad-f54979871f3f:0", nodeEvent.getElementId());
        assertEquals(nodeEvent.getKeys(), emptyMap());
        assertEquals(EventType.NODE, nodeEvent.getEventType());
        assertEquals("User", nodeEvent.getLabels().get(0));
        assertEquals(EntityOperation.CREATE, nodeEvent.getOperation());
        assertNull(nodeEvent.getBefore());
        assertEquals("someone", nodeEvent.getAfter().getProperties().get("name"));
        assertEquals("Some real name", nodeEvent.getAfter().getProperties().get("real_name"));
        assertEquals("User", nodeEvent.getAfter().getLabels().get(0));
    }

    @Test
    void shouldParseCompleteChangeRelationshipEventRecord() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("executingUser", "neo4j");
        metadata.put("connectionClient", "172.17.0.1:44484");
        metadata.put("authenticatedUser", "neo4j");
        metadata.put("captureMode", "FULL");
        metadata.put("serverId", "60b75468");
        metadata.put("connectionType", "bolt");
        metadata.put("connectionServer", "172.17.0.2:7687");
        metadata.put("txStartTime", "2023-08-17T09:14:35.636000000Z");
        metadata.put("txCommitTime", "2023-08-17T09:14:35.666000000Z");

        Map<String, Object> properties = new HashMap<>();
        properties.put("roles", "Jack Swigert");
        Map<String, Object> afterState = new HashMap<>();
        afterState.put("properties", properties);
        Map<String, Object> state = new HashMap<>();
        state.put("before", null);
        state.put("after", afterState);

        Map<String, Object> start = new HashMap<>();
        start.put("elementId", "4:6a4af4ff-da3a-49e7-ae71-2c0ac3c1fc1a:0");
        start.put("labels", Collections.singletonList("PERSON"));
        start.put("keys", Collections.emptyMap());

        Map<String, Object> end = new HashMap<>();
        end.put("elementId", "4:6a4af4ff-da3a-49e7-ae71-2c0ac3c1fc1a:1");
        end.put("labels", Collections.singletonList("MOVIE"));
        end.put("keys", Collections.emptyMap());

        Map<String, Object> event = new HashMap<>();
        event.put("elementId", "5:6a4af4ff-da3a-49e7-ae71-2c0ac3c1fc1a:0");
        event.put("start", start);
        event.put("end", end);
        event.put("key", emptyMap());
        event.put("eventType", "r");
        event.put("state", state);
        event.put("operation", "c");
        event.put("type", "ACTED_IN");

        Map<String, Object> message = new HashMap<>();
        message.put("id", CHANGE_IDENTIFIER_VALUE);
        message.put("txId", 4L);
        message.put("seq", 2L);
        message.put("metadata", metadata);
        message.put("event", event);

        ChangeEvent changeEvent = ResultMapper.parseChangeEvent(message);
        assertEquals(CHANGE_IDENTIFIER_VALUE, changeEvent.getId().getId());
        assertEquals(4L, changeEvent.getTxId());
        assertEquals(2, changeEvent.getSeq());
        checkMetadata(changeEvent.getMetadata());
        Event changeEventEvent = changeEvent.getEvent();
        assertInstanceOf(RelationshipEvent.class, changeEventEvent);
        RelationshipEvent relationshipEvent = (RelationshipEvent) changeEventEvent;
        assertEquals("5:6a4af4ff-da3a-49e7-ae71-2c0ac3c1fc1a:0", relationshipEvent.getElementId());
        assertEquals("ACTED_IN", relationshipEvent.getType());
        assertEquals(EntityOperation.CREATE, relationshipEvent.getOperation());
        assertEquals(EventType.RELATIONSHIP, relationshipEvent.getEventType());
        assertNull(relationshipEvent.getBefore());
        assertEquals("Jack Swigert", relationshipEvent.getAfter().getProperties().get("roles"));

        Node startElement = relationshipEvent.getStart();
        assertEquals("4:6a4af4ff-da3a-49e7-ae71-2c0ac3c1fc1a:0", startElement.getElementId());
        assertEquals(startElement.getKeys(), emptyMap());
        assertEquals("PERSON", startElement.getLabels().get(0));

        Node endElement = relationshipEvent.getEnd();
        assertEquals("4:6a4af4ff-da3a-49e7-ae71-2c0ac3c1fc1a:1", endElement.getElementId());
        assertEquals(endElement.getKeys(), emptyMap());
        assertEquals("MOVIE", endElement.getLabels().get(0));
    }

    private void checkMetadata(Metadata metadata) {
        assertEquals("neo4j", metadata.getExecutingUser());
        assertEquals("172.17.0.1:44484", metadata.getConnectionClient());
        assertEquals("neo4j", metadata.getAuthenticatedUser());
        assertEquals(CaptureMode.FULL, metadata.getCaptureMode());
        assertEquals("60b75468", metadata.getServerId());
        assertEquals("bolt", metadata.getConnectionType());
        assertEquals("172.17.0.2:7687", metadata.getConnectionServer());
        assertEquals(
                metadata.getTxStartTime(),
                ZonedDateTime.parse(
                        "2023-08-17T09:14:35.636000000Z",
                        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSX")));
        assertEquals(
                metadata.getTxCommitTime(),
                ZonedDateTime.parse(
                        "2023-08-17T09:14:35.666000000Z",
                        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSX")));
    }
}
