/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.index.mapper.MapperService;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class ProcessorDocumentUtils {

    public static long getMaxDepth(Map<String, Object> sourceAndMetadataMap, ClusterService clusterService, Environment environment) {
        String indexName = sourceAndMetadataMap.get(IndexFieldMapper.NAME).toString();
        IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
        if (indexMetadata != null) {
            Settings settings = indexMetadata.getSettings();
            return MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.get(settings);
        }
        return MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.get(environment.settings());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void validateMapTypeValue(
        final String sourceKey,
        final Map<String, Object> sourceValue,
        final Object fieldMap,
        final int depth,
        final long maxDepth
    ) {
        if (sourceValue == null) return; // allow map type value to be null.
        validateDepth(sourceKey, depth, maxDepth);
        if (!(fieldMap instanceof Map)) { // source value is map type means configuration has to be map type
            throw new IllegalArgumentException(
                String.format(
                    Locale.getDefault(),
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
                    validateListTypeValue(key, (List) nextSourceValue, fieldMap, depth + 1, maxDepth);
                } else if (nextSourceValue instanceof Map) {
                    validateMapTypeValue(key, (Map<String, Object>) nextSourceValue, nextFieldMap, depth + 1, maxDepth);
                } else if (!(nextSourceValue instanceof String)) {
                    throw new IllegalArgumentException("map type field [" + key + "] is neither string nor nested type, cannot process it");
                } else if (StringUtils.isBlank((String) nextSourceValue)) {
                    throw new IllegalArgumentException("map type field [" + key + "] has empty string value, cannot process it");
                }
            }
        });
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static void validateListTypeValue(String sourceKey, List sourceValue, Object fieldMap, int depth, long maxDepth) {
        validateDepth(sourceKey, depth, maxDepth);
        if (sourceValue == null || sourceValue.isEmpty()) return;
        Object firstNonNullElement = sourceValue.stream().filter(Objects::nonNull).findFirst().orElse(null);
        if (firstNonNullElement == null) return;
        for (Object element : sourceValue) {
            if (firstNonNullElement instanceof List) { // nested list case.
                validateListTypeValue(sourceKey, (List) element, fieldMap, depth + 1, maxDepth);
            } else if (firstNonNullElement instanceof Map) {
                validateMapTypeValue(sourceKey, (Map<String, Object>) element, ((Map) fieldMap).get(sourceKey), depth + 1, maxDepth);
            } else if (!(firstNonNullElement instanceof String)) {
                throw new IllegalArgumentException("list type field [" + sourceKey + "] has non string value, cannot process it");
            } else {
                if (element == null) {
                    throw new IllegalArgumentException("list type field [" + sourceKey + "] has null, cannot process it");
                } else if (!(element instanceof String)) {
                    throw new IllegalArgumentException("list type field [" + sourceKey + "] has non string value, cannot process it");
                } else if (StringUtils.isBlank(element.toString())) {
                    throw new IllegalArgumentException("list type field [" + sourceKey + "] has empty string, cannot process it");
                }
            }
        }
    }

    private static void validateDepth(String sourceKey, int depth, long maxDepth) {
        if (depth > maxDepth) {
            throw new IllegalArgumentException("map type field [" + sourceKey + "] reached max depth limit, cannot process it");
        }
    }
}
