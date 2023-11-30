/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.collections4.MapUtils;

class ModelUtils {
    private ModelUtils() {}

    @SuppressWarnings("unchecked")
    static <K, V> Map<K, V> checkedMap(Map<?, ?> input, Class<K> keyType, Class<V> valueType) {
        if (input == null) {
            return null;
        }

        if (input.keySet().stream().anyMatch(k -> !keyType.isInstance(k))) {
            throw new IllegalArgumentException(String.format(
                    "There are keys of unsupported types in the provided map, expected: %s", keyType.getSimpleName()));
        }

        if (input.values().stream().anyMatch(v -> v != null && !valueType.isInstance(v))) {
            throw new IllegalArgumentException(String.format(
                    "There are values of unsupported types in the provided map, expected: %s",
                    valueType.getSimpleName()));
        }

        return (Map<K, V>) input;
    }

    @SuppressWarnings({"SameParameterValue"})
    static <K, V> Map<K, V> getMap(Map<String, Object> input, String key, Class<K> keyType, Class<V> valueType) {
        var value = input.get(key);
        if (value == null) {
            return null;
        }

        if (value instanceof Map) {
            return checkedMap((Map<?, ?>) value, keyType, valueType);
        }

        throw new IllegalArgumentException(String.format(
                "Unsupported type %s, expected Map", value.getClass().getSimpleName()));
    }

    @SuppressWarnings({"unchecked", "SameParameterValue"})
    static <T> List<T> getList(Map<String, Object> input, String key, Class<T> type) {
        var value = MapUtils.getObject(input, key);
        if (value == null) {
            return null;
        }
        if (value instanceof List) {
            if (((List<?>) value).stream().anyMatch(v -> !type.isInstance(v))) {
                throw new IllegalArgumentException("There are elements of unsupported types in the provided list");
            }

            return (List<T>) value;
        }

        throw new IllegalArgumentException(String.format(
                "Unsupported type %s, expected List", value.getClass().getSimpleName()));
    }

    static ZonedDateTime getZonedDateTime(Map<String, Object> map, String key) {
        var value = MapUtils.getObject(map, key);
        if (value == null) {
            return null;
        }
        if (value instanceof TemporalAccessor) {
            return ZonedDateTime.from((TemporalAccessor) value);
        }
        return ZonedDateTime.parse(value.toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSX"));
    }

    static Map<String, List<Map<String, Object>>> getNodesKeys(Map<String, Object> cypherMap) {
        var keysMap = coerseToMap(MapUtils.getObject(cypherMap, "keys"));
        if (keysMap == null) {
            return null;
        }
        if (keysMap.isEmpty()) {
            return Collections.emptyMap();
        }
        var valueType = keysMap.values().iterator().next().getClass();

        // Check if the key structure is pre Neo4j 5.15
        if (Map.class.isAssignableFrom(valueType)) {
            var preNode515keyMap = checkedMap(keysMap, String.class, Map.class);
            return ModelUtils.transformMapValues(
                    preNode515keyMap, e -> List.of(ModelUtils.checkedMap(e, String.class, Object.class)));
        } else {
            var postNode515KeyMap = checkedMap(keysMap, String.class, List.class);
            return ModelUtils.transformMapValues(
                    postNode515KeyMap, e -> coerceToListOfMaps(e, String.class, Object.class));
        }
    }

    static Map<?, ?> coerseToMap(Object input) {
        if (input == null) {
            return null;
        }
        if (!(input instanceof Map)) {
            throw new IllegalArgumentException(String.format(
                    "Unexpected type %s, expected Map", input.getClass().getSimpleName()));
        }
        return (Map<?, ?>) input;
    }

    static <K, V> List<Map<K, V>> coerceToListOfMaps(List<?> input, Class<K> keyType, Class<V> valueType) {
        if (input == null) {
            return null;
        }
        return input.stream()
                .map(e -> {
                    if (e != null && !(e instanceof Map)) {
                        throw new IllegalArgumentException(
                                "There are elements of unsupported types in the provided list, expected Map");
                    }
                    try {
                        return checkedMap((Map<?, ?>) e, keyType, valueType);
                    } catch (RuntimeException ex) {
                        throw new IllegalArgumentException(
                                "There are elements of unsupported types in the provided list", ex);
                    }
                })
                .collect(Collectors.toList());
    }

    static <K, V1, V2> Map<K, V2> transformMapValues(Map<K, V1> input, Function<V1, V2> transform) {
        if (input == null) {
            return null;
        }
        return input.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> transform.apply(e.getValue())));
    }
}
