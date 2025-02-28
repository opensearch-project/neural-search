/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.env.Environment;
import org.opensearch.index.mapper.MapperService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Stack;

/**
 * This class is used to accommodate the common code pieces of parsing, validating and processing the document for multiple
 * pipeline processors.
 */
public class ProcessorDocumentUtils {

    /**
     * Validates a map type value recursively up to a specified depth. Supports Map type, List type and String type.
     * If current sourceValue is Map or List type, recursively validates its values, otherwise validates its value.
     *
     * @param  sourceKey    the key of the source map being validated, the first level is always the "field_map" key.
     * @param  sourceValue  the source map being validated, the first level is always the sourceAndMetadataMap.
     * @param  fieldMap     the configuration map for validation, the first level is always the value of "field_map" in the processor configuration.
     * @param  clusterService cluster service passed from OpenSearch core.
     * @param  environment   environment passed from OpenSearch core.
     * @param  indexName     the maximum allowed depth for recursion
     * @param  allowEmpty   flag to allow empty values in map type validation.
     */
    public static void validateMapTypeValue(
        final String sourceKey,
        final Map<String, Object> sourceValue,
        final Object fieldMap,
        final String indexName,
        final ClusterService clusterService,
        final Environment environment,
        final boolean allowEmpty
    ) {
        validateMapTypeValue(sourceKey, sourceValue, fieldMap, 1, indexName, clusterService, environment, allowEmpty);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static void validateMapTypeValue(
        final String sourceKey,
        final Map<String, Object> sourceValue,
        final Object fieldMap,
        final long depth,
        final String indexName,
        final ClusterService clusterService,
        final Environment environment,
        final boolean allowEmpty
    ) {
        if (Objects.isNull(sourceValue)) { // allow map type value to be null.
            return;
        }
        validateDepth(sourceKey, depth, indexName, clusterService, environment);
        if (!(fieldMap instanceof Map)) { // source value is map type means configuration has to be map type
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] configuration doesn't match actual value type, configuration type is: %s, actual value type is: %s",
                    sourceKey,
                    fieldMap.getClass().getName(),
                    sourceValue.getClass().getName()
                )
            );
        }
        // next level validation, only validate the keys in configuration.
        ((Map<String, Object>) fieldMap).forEach((key, nextFieldMap) -> {
            Object nextSourceValue = sourceValue.get(key);
            if (nextSourceValue != null) {
                if (nextSourceValue instanceof List) {
                    validateListTypeValue(
                        key,
                        (List) nextSourceValue,
                        fieldMap,
                        depth + 1,
                        indexName,
                        clusterService,
                        environment,
                        allowEmpty
                    );
                } else if (nextSourceValue instanceof Map) {
                    validateMapTypeValue(
                        key,
                        (Map<String, Object>) nextSourceValue,
                        nextFieldMap,
                        depth + 1,
                        indexName,
                        clusterService,
                        environment,
                        allowEmpty
                    );
                } else if (!(nextSourceValue instanceof String)) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "map type field [%s] is neither string nor nested type, cannot process it", key)
                    );
                } else if (!allowEmpty && StringUtils.isBlank((String) nextSourceValue)) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "map type field [%s] has empty string value, cannot process it", key)
                    );
                }
            }
        });
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static void validateListTypeValue(
        final String sourceKey,
        final List sourceValue,
        final Object fieldMap,
        final long depth,
        final String indexName,
        final ClusterService clusterService,
        final Environment environment,
        final boolean allowEmpty
    ) {
        validateDepth(sourceKey, depth, indexName, clusterService, environment);
        if (CollectionUtils.isEmpty(sourceValue)) {
            return;
        }
        for (Object element : sourceValue) {
            if (Objects.isNull(element)) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "list type field [%s] has null, cannot process it", sourceKey)
                );
            }
            if (element instanceof List) { // nested list case.
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "list type field [%s] is nested list type, cannot process it", sourceKey)
                );
            } else if (element instanceof Map) {
                validateMapTypeValue(
                    sourceKey,
                    (Map<String, Object>) element,
                    ((Map) fieldMap).get(sourceKey),
                    depth + 1,
                    indexName,
                    clusterService,
                    environment,
                    allowEmpty
                );
            } else if (!(element instanceof String)) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "list type field [%s] has non string value, cannot process it", sourceKey)
                );
            } else if (!allowEmpty && StringUtils.isBlank(element.toString())) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "list type field [%s] has empty string, cannot process it", sourceKey)
                );
            }
        }
    }

    private static void validateDepth(
        String sourceKey,
        long depth,
        String indexName,
        ClusterService clusterService,
        Environment environment
    ) {
        Settings settings = Optional.ofNullable(clusterService.state().metadata().index(indexName))
            .map(IndexMetadata::getSettings)
            .orElse(environment.settings());
        long maxDepth = MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.get(settings);
        if (depth > maxDepth) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "map type field [%s] reaches max depth limit, cannot process it", sourceKey)
            );
        }
    }

    /**
     * Unflatten a JSON object represented as a {@code Map<String, Object>}, possibly with dot in field name,
     * into a nested {@code Map<String, Object>}
     * "Object" can be either a {@code Map<String, Object>} or a {@code List<Object>} or simply a String.
     * For example, input is {"a.b": "c"}, output is {"a":{"b": "c"}}.
     * Another example:
     *     input is {"a": [{"b.c": "d"}, {"b.c": "e"}]},
     *     output is {"a": [{"b": {"c": "d"}}, {"b": {"c": "e"}}]}
     * @param originalJsonMap the original JSON object represented as a {@code Map<String, Object>}
     * @return the nested JSON object represented as a nested {@code Map<String, Object>}
     * @throws IllegalArgumentException  if the originalJsonMap is null or has invalid dot usage in field name
     */
    public static Map<String, Object> unflattenJson(Map<String, Object> originalJsonMap) {
        if (originalJsonMap == null) {
            throw new IllegalArgumentException("originalJsonMap cannot be null");
        }
        Map<String, Object> result = new HashMap<>();
        Stack<ProcessJsonObjectItem> stack = new Stack<>();

        // Push initial items to stack
        for (Map.Entry<String, Object> entry : originalJsonMap.entrySet()) {
            stack.push(new ProcessJsonObjectItem(entry.getKey(), entry.getValue(), result));
        }

        // Process items until stack is empty
        while (!stack.isEmpty()) {
            ProcessJsonObjectItem item = stack.pop();
            String key = item.key;
            Object value = item.value;
            Map<String, Object> currentMap = item.targetMap;

            // Handle nested value
            if (value instanceof Map) {
                Map<String, Object> nestedMap = new HashMap<>();
                for (Map.Entry<String, Object> entry : ((Map<String, Object>) value).entrySet()) {
                    stack.push(new ProcessJsonObjectItem(entry.getKey(), entry.getValue(), nestedMap));
                }
                value = nestedMap;
            } else if (value instanceof List) {
                value = handleList((List<Object>) value);
            }

            // If key contains dot, split and create nested structure
            unflattenSingleItem(key, value, currentMap);
        }

        return result;
    }

    private static List<Object> handleList(List<Object> list) {
        List<Object> result = new ArrayList<>();
        Stack<ProcessJsonListItem> stack = new Stack<>();

        // Push initial items to stack
        for (int i = list.size() - 1; i >= 0; i--) {
            stack.push(new ProcessJsonListItem(list.get(i), result));
        }

        // Process items until stack is empty
        while (!stack.isEmpty()) {
            ProcessJsonListItem item = stack.pop();
            Object value = item.value;
            List<Object> targetList = item.targetList;

            if (value instanceof Map) {
                Map<String, Object> nestedMap = new HashMap<>();
                Map<String, Object> sourceMap = (Map<String, Object>) value;
                for (Map.Entry<String, Object> entry : sourceMap.entrySet()) {
                    stack.push(new ProcessJsonListItem(new ProcessJsonObjectItem(entry.getKey(), entry.getValue(), nestedMap), targetList));
                }
                targetList.add(nestedMap);
            } else if (value instanceof List) {
                List<Object> nestedList = new ArrayList<>();
                for (Object listItem : (List<Object>) value) {
                    stack.push(new ProcessJsonListItem(listItem, nestedList));
                }
                targetList.add(nestedList);
            } else if (value instanceof ProcessJsonObjectItem) {
                ProcessJsonObjectItem processJsonObjectItem = (ProcessJsonObjectItem) value;
                Map<String, Object> tempMap = new HashMap<>();
                unflattenSingleItem(processJsonObjectItem.key, processJsonObjectItem.value, tempMap);
                processJsonObjectItem.targetMap.putAll(tempMap);
            } else {
                targetList.add(value);
            }
        }

        return result;
    }

    private static void unflattenSingleItem(String key, Object value, Map<String, Object> result) {
        if (StringUtils.isBlank(key)) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        if (key.contains(".")) {
            // Use split with -1 limit to preserve trailing empty strings
            String[] parts = key.split("\\.", -1);
            Map<String, Object> current = result;

            for (int i = 0; i < parts.length; i++) {
                if (StringUtils.isBlank(parts[i])) {
                    throw new IllegalArgumentException(String.format(Locale.ROOT, "Field name '%s' contains invalid dot usage", key));
                }
                if (i == parts.length - 1) {
                    current.put(parts[i], value);
                    continue;
                }
                current = (Map<String, Object>) current.computeIfAbsent(parts[i], k -> new HashMap<>());
            }
        } else {
            result.put(key, value);
        }
    }

    // Helper classes to maintain state during iteration
    private static class ProcessJsonObjectItem {
        String key;
        Object value;
        Map<String, Object> targetMap;

        ProcessJsonObjectItem(String key, Object value, Map<String, Object> targetMap) {
            this.key = key;
            this.value = value;
            this.targetMap = targetMap;
        }
    }

    private static class ProcessJsonListItem {
        Object value;
        List<Object> targetList;

        ProcessJsonListItem(Object value, List<Object> targetList) {
            this.value = value;
            this.targetList = targetList;
        }
    }

}
