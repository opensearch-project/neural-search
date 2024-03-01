/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.env.Environment;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.ingest.IngestDocument;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class ProcessorInputValidator {

    public void validateFieldsValue(
        Map<String, Object> fieldMap,
        Environment environment,
        IngestDocument ingestDocument,
        boolean allowEmpty
    ) {
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        for (Map.Entry<String, Object> embeddingFieldsEntry : fieldMap.entrySet()) {
            Object sourceValue = sourceAndMetadataMap.get(embeddingFieldsEntry.getKey());
            if (sourceValue != null) {
                String sourceKey = embeddingFieldsEntry.getKey();
                Class<?> sourceValueClass = sourceValue.getClass();
                if (List.class.isAssignableFrom(sourceValueClass) || Map.class.isAssignableFrom(sourceValueClass)) {
                    validateNestedTypeValue(sourceKey, sourceValue, environment, allowEmpty, () -> 1);
                } else if (!String.class.isAssignableFrom(sourceValueClass)) {
                    throw new IllegalArgumentException("field [" + sourceKey + "] is neither string nor nested type, cannot process it");
                } else if (!allowEmpty && StringUtils.isBlank(sourceValue.toString())) {
                    throw new IllegalArgumentException("field [" + sourceKey + "] has empty string value, cannot process it");
                }
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void validateNestedTypeValue(
        String sourceKey,
        Object sourceValue,
        Environment environment,
        boolean allowEmpty,
        Supplier<Integer> maxDepthSupplier
    ) {
        int maxDepth = maxDepthSupplier.get();
        if (maxDepth > MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.get(environment.settings())) {
            throw new IllegalArgumentException("map type field [" + sourceKey + "] reached max depth limit, cannot process it");
        } else if ((List.class.isAssignableFrom(sourceValue.getClass()))) {
            validateListTypeValue(sourceKey, sourceValue, environment, allowEmpty, maxDepthSupplier);
        } else if (Map.class.isAssignableFrom(sourceValue.getClass())) {
            ((Map) sourceValue).values()
                .stream()
                .filter(Objects::nonNull)
                .forEach(x -> validateNestedTypeValue(sourceKey, x, environment, allowEmpty, () -> maxDepth + 1));
        } else if (!String.class.isAssignableFrom(sourceValue.getClass())) {
            throw new IllegalArgumentException("map type field [" + sourceKey + "] has non-string type, cannot process it");
        } else if (!allowEmpty && StringUtils.isBlank(sourceValue.toString())) {
            throw new IllegalArgumentException("map type field [" + sourceKey + "] has empty string, cannot process it");
        }
    }

    @SuppressWarnings({ "rawtypes" })
    private void validateListTypeValue(
        String sourceKey,
        Object sourceValue,
        Environment environment,
        boolean allowEmpty,
        Supplier<Integer> maxDepthSupplier
    ) {
        for (Object value : (List) sourceValue) {
            if (value instanceof Map) {
                validateNestedTypeValue(sourceKey, value, environment, allowEmpty, () -> maxDepthSupplier.get() + 1);
            } else if (value == null) {
                throw new IllegalArgumentException("list type field [" + sourceKey + "] has null, cannot process it");
            } else if (value instanceof List) {
                for (Object nestedValue : (List) sourceValue) {
                    if (!(nestedValue instanceof String)) {
                        throw new IllegalArgumentException("list type field [" + sourceKey + "] has non string value, cannot process it");
                    }
                }
            } else if (!(value instanceof String)) {
                throw new IllegalArgumentException("list type field [" + sourceKey + "] has non string value, cannot process it");
            } else if (!allowEmpty && StringUtils.isBlank(value.toString())) {
                throw new IllegalArgumentException("list type field [" + sourceKey + "] has empty string, cannot process it");
            }
        }
    }
}
