/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.neuralsearch.sparse.SparseSettings;
import org.opensearch.neuralsearch.sparse.mapper.SparseVectorFieldType;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Utility class for operations related to sparse fields in neural search indices.
 */
public class SparseFieldUtils {
    /**
     * Retrieves all sparse ANN fields from a given index, including nested fields.
     * For nested fields like "passage_chunk_embedding.sparse_encoding", returns the full path "passage_chunk_embedding.sparse_encoding".
     *
     * @param index The name of the index
     * @param clusterService The cluster service
     * @param maxDepth The maximum depth to traverse in nested fields
     * @return A set of field names that are configured as sparse token fields, or an empty set if none exist
     */
    @SuppressWarnings("unchecked")
    public static Set<String> getSparseAnnFields(String index, ClusterService clusterService, long maxDepth) {
        if (index == null) {
            return Collections.emptySet();
        }
        final IndexMetadata metadata = Optional.ofNullable(clusterService)
            .map(ClusterService::state)
            .map(ClusterState::metadata)
            .map(metadataState -> metadataState.index(index))
            .orElse(null);
        if (metadata == null || !SparseSettings.IS_SPARSE_INDEX_SETTING.get(metadata.getSettings())) {
            return Collections.emptySet();
        }
        MappingMetadata mappingMetadata = metadata.mapping();
        if (mappingMetadata == null || mappingMetadata.sourceAsMap() == null) {
            return Collections.emptySet();
        }
        Object properties = mappingMetadata.sourceAsMap().get("properties");
        if (!(properties instanceof Map)) {
            return Collections.emptySet();
        }
        Set<String> sparseAnnFields = new HashSet<>();
        Map<String, Object> fields = (Map<String, Object>) properties;
        collectSparseAnnFields(fields, "", sparseAnnFields, 1, maxDepth);
        return sparseAnnFields;
    }

    /**
     * Recursively collects sparse ANN fields from the mapping, including nested structures.
     * For nested fields, returns the parent path rather than the full field path.
     *
     * @param fields The current level of field mappings
     * @param parentPath The path to the current level (empty for top-level)
     * @param sparseAnnFields The set to collect sparse ANN field paths
     * @param depth Current recursion depth
     * @param maxDepth Maximum allowed depth
     */
    @SuppressWarnings("unchecked")
    private static void collectSparseAnnFields(
        Map<String, Object> fields,
        String parentPath,
        Set<String> sparseAnnFields,
        int depth,
        long maxDepth
    ) {
        if (depth > maxDepth) {
            throw new IllegalArgumentException(
                String.format("Field [%s] exceeds maximum mapping depth limit of [%d]", parentPath, maxDepth)
            );
        }

        for (Map.Entry<String, Object> field : fields.entrySet()) {
            if (!(field.getValue() instanceof Map)) {
                continue;
            }
            Map<String, Object> fieldMap = (Map<String, Object>) field.getValue();
            Object type = fieldMap.get("type");

            if (Objects.nonNull(type) && SparseVectorFieldType.isSparseVectorType(type.toString())) {
                sparseAnnFields.add(parentPath.isEmpty() ? field.getKey() : parentPath + "." + field.getKey());
            } else {
                Object nestedProperties = fieldMap.get("properties");
                if (nestedProperties instanceof Map) {
                    String currentPath = parentPath.isEmpty() ? field.getKey() : parentPath + "." + field.getKey();
                    collectSparseAnnFields((Map<String, Object>) nestedProperties, currentPath, sparseAnnFields, depth + 1, maxDepth);
                }
            }
        }
    }
}
