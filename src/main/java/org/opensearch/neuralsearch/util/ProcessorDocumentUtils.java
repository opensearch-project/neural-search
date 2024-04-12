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
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.index.mapper.MapperService;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * This class is used to accommodate the common code pieces of parsing, validating and processing the document for multiple
 * pipeline processors.
 */
public class ProcessorDocumentUtils {

    /**
     * This method is used to get the max depth of the index or from system settings.
     *
     * @param  sourceAndMetadataMap   _source and metadata info in document.
     * @param  clusterService         cluster service passed from OpenSearch core.
     * @param  environment            environment passed from OpenSearch core.
     * @return                        max depth of the index or from system settings.
     */
    public static long getMaxDepth(Map<String, Object> sourceAndMetadataMap, ClusterService clusterService, Environment environment) {
        String indexName = sourceAndMetadataMap.get(IndexFieldMapper.NAME).toString();
        IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
        if (indexMetadata != null) {
            Settings settings = indexMetadata.getSettings();
            return MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.get(settings);
        }
        return MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.get(environment.settings());
    }

    /**
     * Validates a map type value recursively up to a specified depth. Supports Map type, List type and String type.
     * If current sourceValue is Map or List type, recursively validates its values, otherwise validates its value.
     *
     * @param  sourceKey    the key of the source map being validated, the first level is always the "field_map" key.
     * @param  sourceValue  the source map being validated, the first level is always the sourceAndMetadataMap.
     * @param  fieldMap     the configuration map for validation, the first level is always the value of "field_map" in the processor configuration.
     * @param  depth        the current depth of recursion
     * @param  maxDepth     the maximum allowed depth for recursion
     * @param  allowEmpty   flag to allow empty values in map type validation.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void validateMapTypeValue(
        final String sourceKey,
        final Map<String, Object> sourceValue,
        final Object fieldMap,
        final long depth,
        final long maxDepth,
        final boolean allowEmpty
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
                    validateListTypeValue(key, (List) nextSourceValue, fieldMap, depth + 1, maxDepth, allowEmpty);
                } else if (nextSourceValue instanceof Map) {
                    validateMapTypeValue(key, (Map<String, Object>) nextSourceValue, nextFieldMap, depth + 1, maxDepth, allowEmpty);
                } else if (!(nextSourceValue instanceof String)) {
                    throw new IllegalArgumentException("map type field [" + key + "] is neither string nor nested type, cannot process it");
                } else if (!allowEmpty && StringUtils.isBlank((String) nextSourceValue)) {
                    throw new IllegalArgumentException("map type field [" + key + "] has empty string value, cannot process it");
                }
            }
        });
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static void validateListTypeValue(
        String sourceKey,
        List sourceValue,
        Object fieldMap,
        long depth,
        long maxDepth,
        boolean allowEmpty
    ) {
        validateDepth(sourceKey, depth, maxDepth);
        if (CollectionUtils.isEmpty(sourceValue)) return;
        for (Object element : sourceValue) {
            if (element == null) {
                throw new IllegalArgumentException("list type field [" + sourceKey + "] has null, cannot process it");
            }
            if (element instanceof List) { // nested list case.
                throw new IllegalArgumentException("list type field [" + sourceKey + "] is nested list type, cannot process it");
            } else if (element instanceof Map) {
                validateMapTypeValue(
                    sourceKey,
                    (Map<String, Object>) element,
                    ((Map) fieldMap).get(sourceKey),
                    depth + 1,
                    maxDepth,
                    allowEmpty
                );
            } else if (!(element instanceof String)) {
                throw new IllegalArgumentException("list type field [" + sourceKey + "] has non string value, cannot process it");
            } else if (!allowEmpty && StringUtils.isBlank(element.toString())) {
                throw new IllegalArgumentException("list type field [" + sourceKey + "] has empty string, cannot process it");
            }
        }
    }

    private static void validateDepth(String sourceKey, long depth, long maxDepth) {
        if (depth > maxDepth) {
            throw new IllegalArgumentException("map type field [" + sourceKey + "] reaches max depth limit, cannot process it");
        }
    }
}
