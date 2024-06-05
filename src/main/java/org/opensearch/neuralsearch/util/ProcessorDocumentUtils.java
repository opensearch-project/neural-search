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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
                    throw new IllegalArgumentException(String.format(Locale.ROOT, "map type field [%s] is neither string nor nested type, cannot process it", key));
                } else if (!allowEmpty && StringUtils.isBlank((String) nextSourceValue)) {
                    throw new IllegalArgumentException(String.format(Locale.ROOT, "map type field [%s] has empty string value, cannot process it", key));
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
                throw new IllegalArgumentException(String.format(Locale.ROOT, "list type field [%s] has null, cannot process it", sourceKey));
            }
            if (element instanceof List) { // nested list case.
                throw new IllegalArgumentException(String.format(Locale.ROOT, "list type field [%s] is nested list type, cannot process it", sourceKey));
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
                throw new IllegalArgumentException(String.format(Locale.ROOT, "list type field [%s] has non string value, cannot process it", sourceKey));
            } else if (!allowEmpty && StringUtils.isBlank(element.toString())) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "list type field [%s] has empty string, cannot process it", sourceKey));
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
            throw new IllegalArgumentException(String.format(Locale.ROOT, "map type field [%s] reaches max depth limit, cannot process it", sourceKey));
        }
    }
}
