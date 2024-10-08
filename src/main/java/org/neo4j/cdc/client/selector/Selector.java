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

import java.util.Map;
import org.neo4j.cdc.client.model.ChangeEvent;

/**
 * Describes a selector that provides a means for filtering the returned changes.
 */
public interface Selector {

    Map<String, Object> asMap();

    boolean matches(ChangeEvent e);

    ChangeEvent applyProperties(ChangeEvent e);
}
