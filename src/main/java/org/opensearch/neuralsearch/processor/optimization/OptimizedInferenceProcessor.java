/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.optimization;

import lombok.extern.log4j.Log4j2;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.env.Environment;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.InferenceProcessor;
import org.opensearch.neuralsearch.processor.util.ProcessorUtils;
import org.opensearch.neuralsearch.util.ProcessorDocumentUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The abstract class for optimized text processing use cases. On update operation, the optimized inference processor will attempt to
 * optimize inference calls by copying over existing embeddings for the same text
 */

@Log4j2
public abstract class OptimizedInferenceProcessor extends InferenceProcessor {
    public OptimizedInferenceProcessor(
        String tag,
        String description,
        int batchSize,
        String modelId,
        String type,
        String listTypeNestedMapKey,
        Map<String, Object> fieldMap,
        MLCommonsClientAccessor clientAccessor,
        Environment environment,
        ClusterService clusterService
    ) {
        super(tag, description, batchSize, type, listTypeNestedMapKey, modelId, fieldMap, clientAccessor, environment, clusterService);
    }

    public abstract Object processValue(
        String currentPath,
        Object processValue,
        int level,
        Map<String, Object> sourceAndMetadataMap,
        Map<String, Object> existingSourceAndMetadataMap,
        int index
    );

    public abstract List<Object> processValues(
        List<?> processList,
        Optional<Object> sourceList,
        Optional<Object> existingList,
        Optional<Object> embeddingList,
        Map<String, Object> sourceAndMetadataMap,
        Map<String, Object> existingSourceAndMetadataMap,
        String fullEmbeddingKey
    );

    public Map<String, Object> filterProcessMap(
        Map<String, Object> existingSourceAndMetadataMap,
        Map<String, Object> sourceAndMetadataMap,
        Map<String, Object> processMap
    ) {
        return filterProcessMap(existingSourceAndMetadataMap, sourceAndMetadataMap, processMap, "", 0);
    }

    /**
     * Filters and processes a nested map structure by comparing values between existing and new metadata maps.
     *
     * @param existingSourceAndMetadataMap SourceAndMetadataMap of existing Document
     * @param sourceAndMetadataMap SourceAndMetadataMap of ingestDocument Document
     * @param processMap The current processMap
     * @param prevPath The dot-notation path of the parent elements
     * @param prevLevel The current nesting level in the hierarchy
     * @return A filtered map containing only the elements that differ between the existing and new metadata maps
     *
     */
    protected Map<String, Object> filterProcessMap(
        Map<String, Object> existingSourceAndMetadataMap,
        Map<String, Object> sourceAndMetadataMap,
        Map<String, Object> processMap,
        String prevPath,
        int prevLevel
    ) {
        Map<String, Object> filteredProcessMap = new HashMap<>();

        for (Map.Entry<String, Object> entry : processMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            String currentPath = prevPath.isEmpty() ? key : prevPath + "." + key;
            int currLevel = prevLevel + 1;
            if (value instanceof Map<?, ?>) {
                Map<String, Object> filteredInnerMap = filterProcessMap(
                    existingSourceAndMetadataMap,
                    sourceAndMetadataMap,
                    (Map<String, Object>) value,
                    currentPath,
                    currLevel
                );
                if (!filteredInnerMap.isEmpty()) {
                    filteredProcessMap.put(key, filteredInnerMap);
                }
            } else if (value instanceof List) {
                List<Object> processedList = processListValue(
                    currentPath,
                    (List<?>) value,
                    currLevel,
                    sourceAndMetadataMap,
                    existingSourceAndMetadataMap
                );
                if (!processedList.isEmpty()) {
                    filteredProcessMap.put(key, processedList);
                }
            } else {
                Object processedValue = processValue(currentPath, value, currLevel, sourceAndMetadataMap, existingSourceAndMetadataMap, -1);
                if (processedValue != null) {
                    filteredProcessMap.put(key, processedValue);
                }
            }
        }
        return filteredProcessMap;
    }

    /**
     * Processes a list of values by comparing them against source and existing metadata.
     *
     * @param currentPath The current path in dot notation for the list being processed
     * @param processList The list of values to process
     * @param level The current nesting level in the hierarchy
     * @param sourceAndMetadataMap SourceAndMetadataMap of ingestDocument Document
     * @param existingSourceAndMetadataMap SourceAndMetadataMap of existing Document
     * @return A processed list containing non-filtered elements
     */
    protected List<Object> processListValue(
        String currentPath,
        List<?> processList,
        int level,
        Map<String, Object> sourceAndMetadataMap,
        Map<String, Object> existingSourceAndMetadataMap
    ) {
        String textKey = ProcessorUtils.findKeyFromFromValue(ProcessorDocumentUtils.unflattenJson(fieldMap), currentPath, level);
        if (textKey == null) {
            return new ArrayList<>(processList);
        }

        String fullTextKey = ProcessorUtils.computeFullTextKey(currentPath, textKey, level);
        String fullEmbeddingKey = currentPath;
        Optional<Object> sourceList = ProcessorUtils.getValueFromSource(sourceAndMetadataMap, fullTextKey);
        Optional<Object> existingList = ProcessorUtils.getValueFromSource(existingSourceAndMetadataMap, fullTextKey);
        Optional<Object> embeddingList = ProcessorUtils.getValueFromSource(existingSourceAndMetadataMap, fullEmbeddingKey);
        if (sourceList.isPresent() && sourceList.get() instanceof List) {
            return processValues(
                processList,
                sourceList,
                existingList,
                embeddingList,
                sourceAndMetadataMap,
                existingSourceAndMetadataMap,
                fullEmbeddingKey
            );
        } else {
            return processMapValuesInList(processList, currentPath, level, sourceAndMetadataMap, existingSourceAndMetadataMap);

        }
    }

    /**
     * Processes a list containing map values by iterating through each item and processing it individually.
     *
     * @param processList The list of Map items to process
     * @param currentPath The current path in dot notation
     * @param level The current nesting level in the hierarchy
     * @param sourceAndMetadataMap SourceAndMetadataMap of ingestDocument Document
     * @param existingSourceAndMetadataMap SourceAndMetadataMap of existing Document
     * @return A processed list containing non-filtered elements
     */
    private List<Object> processMapValuesInList(
        List<?> processList,
        String currentPath,
        int level,
        Map<String, Object> sourceAndMetadataMap,
        Map<String, Object> existingSourceAndMetadataMap
    ) {
        List<Object> filteredList = new ArrayList<>();
        for (int i = 0; i < processList.size(); i++) {
            Object processedItem = processValue(
                currentPath,
                processList.get(i),
                level,
                sourceAndMetadataMap,
                existingSourceAndMetadataMap,
                i
            );
            if (processedItem != null) {
                filteredList.add(processedItem);
            }
        }
        return filteredList;
    }
}
