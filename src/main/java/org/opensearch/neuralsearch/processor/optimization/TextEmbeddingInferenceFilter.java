/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.optimization;

import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.processor.util.ProcessorUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * TextEmbeddingInferenceFilter optimizes text embedding inference by selectively processing text data.
 * This class extends InferenceFilter to provide efficient text embedding processing by comparing text
 * between existing and new documents. If the text is identical, the corresponding embeddings are copied over,
 * avoiding redundant inference calls and improving performance.
 */
@Log4j2
public class TextEmbeddingInferenceFilter extends InferenceFilter {
    /**
     * Constructs a TextEmbeddingInferenceFilter instance with the specified field map.
     */
    public TextEmbeddingInferenceFilter(Map<String, Object> fieldMap) {
        super(fieldMap);
    }

    /**
     * Filters a single value by checking if the text is identical in both the existing and new document.
     * If the text matches, the corresponding embedding is copied, and null is returned, indicating no further
     * processing is required.
     *
     * @return null if embeddings are reused; the original value otherwise.
     */
    @Override
    public Object filterInferenceValue(
        String embeddingKey,
        Object processValue,
        Map<String, Object> sourceAndMetadataMap,
        Map<String, Object> existingSourceAndMetadataMap,
        int index
    ) {
        String textPath = reversedFieldMap.get(embeddingKey);
        Optional<Object> existingValueOptional = ProcessorUtils.getValueFromSource(existingSourceAndMetadataMap, textPath);
        Optional<Object> embeddingValueOptional = ProcessorUtils.getValueFromSource(existingSourceAndMetadataMap, embeddingKey);
        if (existingValueOptional.isPresent() && embeddingValueOptional.isPresent()) {
            return copyEmbeddingForSingleObject(
                embeddingKey,
                processValue,
                existingValueOptional.get(),
                embeddingValueOptional.get(),
                sourceAndMetadataMap,
                index
            );
        }
        return processValue;
    }

    /**
     * Copy a single value by checking if the text is identical in both the existing and new document.
     * If the text matches, the corresponding embedding is copied, and null is returned, indicating no further
     * processing is required.
     *
     * @return null if embeddings are reused; the processValue otherwise.
     */
    @Override
    public Object copyEmbeddingForSingleObject(
        String embeddingKey,
        Object processValue,
        Object existingValue,
        Object embeddingValue,
        Map<String, Object> sourceAndMetadataMap,
        int index
    ) {
        if (Objects.equals(existingValue, processValue)) {
            ProcessorUtils.setValueToSource(sourceAndMetadataMap, embeddingKey, embeddingValue, index);
            // if successfully copied, return null to be filtered out from process map
            return null;
        }
        // processValue and existingValue are different, return processValue to be included in process map
        return processValue;
    }

    /**
     * Copy values in list by checking if all texts in list are identical in both the existing and new documents.
     * If lists are equal, the corresponding embeddings are copied
     * @return empty list if embeddings are reused; processList otherwise.
     */
    @Override
    public List<Object> copyEmbeddingForListObject(
        String embeddingKey,
        List<Object> processList,
        List<Object> existingList,
        List<Object> embeddingList,
        Map<String, Object> sourceAndMetadataMap
    ) {
        if (Objects.equals(processList, existingList)) {
            ProcessorUtils.setValueToSource(sourceAndMetadataMap, embeddingKey, embeddingList);
            // if successfully copied, return null to be filtered out from process map
            return null;
        }
        // source list and existing list are different, return processList to be included in process map
        return processList;
    }
}
