/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.lucene.index.FieldInfo;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.neuralsearch.sparse.SparseSettings;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.algorithm.SparseForwardIndex;
import org.opensearch.neuralsearch.sparse.mapper.SparseVectorFieldType;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTERING_BATCH_SIZE_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.ENGINE_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.FORWARD_INDEX_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.QUANTIZATION_CEILING_INGEST_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_CLUSTERING_BATCH_SIZE;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_N_POSTINGS;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_POSTING_MINIMUM_LENGTH;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_POSTING_PRUNE_RATIO;

/**
 * Utility class for operations related to sparse fields in neural search indices.
 */
public class SparseFieldUtils {
    /**
     * Retrieves all sparse ANN fields from a given index, including nested fields.
     * For nested fields like "passage_chunk_embedding.sparse_encoding", returns the full path "passage_chunk_embedding.sparse_encoding".
     * This method automatically retrieves the max depth from the index settings.
     *
     * @param index The name of the index
     * @param clusterService The cluster service
     * @return A set of field names that are configured as sparse token fields, or an empty set if none exist
     */
    public static Set<String> getSparseAnnFields(String index, ClusterService clusterService) {
        long maxDepth = getMaxDepth(index, clusterService);
        return getSparseAnnFields(index, clusterService, maxDepth);
    }

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
     * Retrieves the maximum allowed mapping depth from index settings.
     *
     * @param index The name of the index
     * @param clusterService The cluster service
     * @return The maximum depth limit from index settings
     */
    public static long getMaxDepth(String index, ClusterService clusterService) {
        Settings settings = Optional.ofNullable(clusterService)
            .map(ClusterService::state)
            .map(ClusterState::metadata)
            .map(metadata -> metadata.index(index))
            .map(IndexMetadata::getSettings)
            .orElse(Settings.EMPTY);

        return MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.get(settings);
    }

    /**
     * Extracts the {@link SparseEngine} for a sparse vector field from its {@link FieldInfo} attributes.
     *
     * @param fieldInfo The field info containing the engine attribute
     * @return The {@link SparseEngine} configured for the field, or {@link SparseEngine#DEFAULT} if not found
     */
    public static SparseEngine getSparseEngine(FieldInfo fieldInfo) {
        if (fieldInfo == null) {
            // A segment that holds no document with the field has no FieldInfo for it.
            return SparseEngine.DEFAULT;
        }
        String engine = fieldInfo.getAttribute(ENGINE_FIELD);
        if (engine == null) {
            return SparseEngine.DEFAULT;
        }
        SparseEngine sparseEngine = SparseEngine.fromName(engine);
        return sparseEngine != null ? sparseEngine : SparseEngine.DEFAULT;
    }

    /**
     * Extracts the {@link SparseForwardIndex} for a sparse vector field from its {@link FieldInfo} attributes.
     *
     * @param fieldInfo The field info containing the forward index attribute
     * @return The {@link SparseForwardIndex} configured for the field, or {@link SparseForwardIndex#DEFAULT} if not found
     */
    public static SparseForwardIndex getSparseForwardIndex(FieldInfo fieldInfo) {
        if (fieldInfo == null) {
            // A segment that holds no document with the field has no FieldInfo for it.
            return SparseForwardIndex.DEFAULT;
        }
        SparseForwardIndex forwardIndex = SparseForwardIndex.fromName(fieldInfo.getAttribute(FORWARD_INDEX_FIELD));
        return forwardIndex != null ? forwardIndex : SparseForwardIndex.DEFAULT;
    }

    /**
     * Retrieves how many inverted lists the native engine clusters per batch from the field attributes.
     *
     * Tolerates a missing or unparseable attribute rather than failing the segment: only the native
     * engine writes it, so a field from a Lucene-engine index has none.
     *
     * @param fieldInfo The field info containing the clustering batch size attribute
     * @return The clustering batch size configured for the field, or {@link SparseConstants.Seismic#DEFAULT_CLUSTERING_BATCH_SIZE}
     */
    public static int getClusteringBatchSize(FieldInfo fieldInfo) {
        if (fieldInfo == null) {
            // A segment that holds no document with the field has no FieldInfo for it.
            return DEFAULT_CLUSTERING_BATCH_SIZE;
        }
        return NumberUtils.toInt(fieldInfo.getAttribute(CLUSTERING_BATCH_SIZE_FIELD), DEFAULT_CLUSTERING_BATCH_SIZE);
    }

    /**
     * Retrieves the quantization ceiling for ingest from the field attributes.
     *
     * @param fieldInfo The field info containing the quantization ceiling attribute
     * @return The quantization ceiling value for ingest
     */
    public static float getQuantizationCeilingIngest(FieldInfo fieldInfo) {
        return Float.parseFloat(fieldInfo.attributes().get(QUANTIZATION_CEILING_INGEST_FIELD));
    }

    /**
     * Retrieves the cluster ratio from the field attributes.
     *
     * @param fieldInfo The field info containing the cluster ratio attribute
     * @return The cluster ratio value
     */
    public static float getClusterRatio(FieldInfo fieldInfo) {
        return Float.parseFloat(fieldInfo.attributes().get(CLUSTER_RATIO_FIELD));
    }

    /**
     * Retrieves the summary prune ratio from the field attributes.
     *
     * @param fieldInfo The field info containing the summary prune ratio attribute
     * @return The summary prune ratio value
     */
    public static float getSummaryPruneRatio(FieldInfo fieldInfo) {
        return Float.parseFloat(fieldInfo.attributes().get(SUMMARY_PRUNE_RATIO_FIELD));
    }

    public static int getNPostings(FieldInfo fieldInfo, int maxDoc) {
        int nPostings;
        if (Integer.parseInt(fieldInfo.attributes().get(N_POSTINGS_FIELD)) == DEFAULT_N_POSTINGS) {
            nPostings = Math.max((int) (DEFAULT_POSTING_PRUNE_RATIO * maxDoc), DEFAULT_POSTING_MINIMUM_LENGTH);
        } else {
            nPostings = Integer.parseInt(fieldInfo.attributes().get(N_POSTINGS_FIELD));
        }
        return nPostings;
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
                String.format(Locale.ROOT, "Field [%s] exceeds maximum mapping depth limit of [%d]", parentPath, maxDepth)
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
