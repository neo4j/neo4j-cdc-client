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
package org.neo4j.cdc.client.pattern;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.neo4j.cdc.client.model.EntityOperation;
import org.neo4j.cdc.client.selector.Selector;

public interface Pattern {

    Set<Selector> toSelector();

    void withOperation(EntityOperation operation);

    void withExecutingUser(String user);

    void withAuthenticatedUser(String user);

    void withTxMetadata(Map<String, Object> metadata);

    void withChangesTo(Set<String> changesTo);

    static List<Pattern> parse(String expression) {
        return Visitors.parse(expression);
    }
}
