/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import lombok.AllArgsConstructor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.knn.index.mapper.KNNVectorFieldMapper;
import org.opensearch.neuralsearch.constants.MappingConstants;
import org.opensearch.neuralsearch.mapper.SemanticFieldMapper;
import lombok.NonNull;
import org.opensearch.neuralsearch.query.dto.NeuralQueryTargetFieldConfig;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.neuralsearch.constants.MappingConstants.DOC;
import static org.opensearch.neuralsearch.constants.MappingConstants.PATH_SEPARATOR;
import static org.opensearch.neuralsearch.constants.MappingConstants.PROPERTIES;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.CHUNKING;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.MODEL_ID;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SEMANTIC_FIELD_SEARCH_ANALYZER;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SEMANTIC_INFO_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.MappingConstants.TYPE;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.DEFAULT_SEMANTIC_INFO_FIELD_NAME_SUFFIX;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SEARCH_MODEL_ID;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.CHUNKS_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.EMBEDDING_FIELD_NAME;
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.SUPPORTED_TARGET_FIELD_TYPES;

/**
 * A util class to help process mapping with semantic field
 */
public class SemanticMappingUtils {
    private static final int MAX_DEPTH_OF_INDEX_MAPPING = 1000;

    @AllArgsConstructor
    private static class CollectSemanticFieldStackEntry {
        Map<String, Object> mapping;
        String path;
        int depth;
    }

    /**
     * It will recursively traverse the mapping to collect the full path to field config map for semantic fields
     * @param currentMapping current mapping
     * @param semanticFieldPathToConfigMap path to field config map for semantic fields
     */
    @SuppressWarnings("unchecked")
    public static void collectSemanticField(
        @NonNull final Map<String, Object> currentMapping,
        @NonNull final Map<String, Map<String, Object>> semanticFieldPathToConfigMap
    ) {
        final Deque<CollectSemanticFieldStackEntry> semanticFieldEntryStack = new ArrayDeque<>();
        semanticFieldEntryStack.push(new CollectSemanticFieldStackEntry(currentMapping, "", 0));

        while (semanticFieldEntryStack.isEmpty() == false) {
            final CollectSemanticFieldStackEntry entry = semanticFieldEntryStack.pop();
            final Map<String, Object> mapping = entry.mapping;
            String parentPath = entry.path;
            int currentDepth = entry.depth;

            if (currentDepth > MAX_DEPTH_OF_INDEX_MAPPING) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Cannot transform the mapping for semantic fields because its depth exceeds the maximum allowed depth %d.",
                        MAX_DEPTH_OF_INDEX_MAPPING
                    )
                );
            }

            for (Map.Entry<String, Object> fieldEntry : mapping.entrySet()) {
                final String fieldName = fieldEntry.getKey();
                final Object fieldConfig = fieldEntry.getValue();
                // Build the full path for the current field
                final String fullPath = parentPath.isEmpty() ? fieldName : String.join(PATH_SEPARATOR, parentPath, fieldName);

                if (fieldConfig instanceof Map) {
                    final Map<String, Object> fieldConfigMap = (Map<String, Object>) fieldConfig;

                    if (isSemanticField(fieldConfigMap)) {
                        semanticFieldPathToConfigMap.put(fullPath, fieldConfigMap);
                    }

                    // If it's an object field, recurse into the sub-fields
                    if (fieldConfigMap.containsKey(MappingConstants.PROPERTIES)) {
                        final Map<String, Object> subMapping = (Map<String, Object>) fieldConfigMap.get(MappingConstants.PROPERTIES);
                        semanticFieldEntryStack.push(new CollectSemanticFieldStackEntry(subMapping, fullPath, currentDepth + 1));
                    }
                } else {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Expect the field config at the path %s should be a map.", fullPath)
                    );
                }
            }
        }
    }

    private static boolean isSemanticField(Map<String, Object> fieldConfigMap) {
        final Object type = fieldConfigMap.get(MappingConstants.TYPE);
        return SemanticFieldMapper.CONTENT_TYPE.equals(type);
    }

    /**
     * In mapping there can be multiple semantic fields with the same model id. This function can help build the model
     * id to the path of semantic fields map. In this way we can know how many unique model ids we have and pull the
     * model info for them.
     * @param semanticFieldPathToConfigMap path to semantic field config map
     * @return model id to paths of semantic fields map
     */
    public static Map<String, List<String>> extractModelIdToFieldPathMap(
        @NonNull final Map<String, Map<String, Object>> semanticFieldPathToConfigMap
    ) {
        final Map<String, List<String>> modelIdToFieldPathMap = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : semanticFieldPathToConfigMap.entrySet()) {
            final String fullPath = entry.getKey();
            final Map<String, Object> fieldConfigMap = entry.getValue();
            final String modelIdStr = getModelId(fieldConfigMap, fullPath);
            if (modelIdToFieldPathMap.containsKey(modelIdStr) == false) {
                modelIdToFieldPathMap.put(modelIdStr, new ArrayList<>());
            }
            modelIdToFieldPathMap.get(modelIdStr).add(fullPath);
        }
        return modelIdToFieldPathMap;
    }

    /**
     * Get the model id from the semantic field config. It will validate the model id to ensure it is a valid string
     * value.
     * @param fieldConfigMap semantic field config
     * @param fullPath full path to the semantic field
     * @return valid model id
     */
    public static String getModelId(@NonNull final Map<String, Object> fieldConfigMap, @NonNull final String fullPath) {
        final String error = validateModelId(fullPath, fieldConfigMap);
        if (error != null) {
            throw new IllegalArgumentException(error);
        }

        return (String) fieldConfigMap.get(MODEL_ID);
    }

    /**
     * Get the full path to the semantic info field
     * @param fieldConfigMap semantic field config
     * @param baseFieldPath The base path to construct the semantic info field full path
     * @param semanticFieldFullPathInMapping full path to access semantic field in the index mapping
     * @return full path to the semantic info field
     */
    public static String getSemanticInfoFieldFullPath(
        @NonNull final Map<String, Object> fieldConfigMap,
        @NonNull final String baseFieldPath,
        @NonNull final String semanticFieldFullPathInMapping
    ) {
        final String error = validateSemanticInfoFieldName(semanticFieldFullPathInMapping, fieldConfigMap);
        if (error != null) {
            throw new IllegalArgumentException(error);
        }
        String semanticInfoFullPath = baseFieldPath + DEFAULT_SEMANTIC_INFO_FIELD_NAME_SUFFIX;
        final String userDefinedSemanticInfoFieldName = fieldConfigMap.containsKey(SEMANTIC_INFO_FIELD_NAME)
            ? (String) fieldConfigMap.get(SEMANTIC_INFO_FIELD_NAME)
            : null;
        if (userDefinedSemanticInfoFieldName != null) {
            final String[] paths = semanticInfoFullPath.split("\\.");
            paths[paths.length - 1] = userDefinedSemanticInfoFieldName;
            semanticInfoFullPath = String.join(PATH_SEPARATOR, paths);
        }
        return semanticInfoFullPath;
    }

    /**
     * Help extract the properties from a mapping
     * @param mapping index mapping
     * @return properties of the mapping
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> getProperties(Map<String, Object> mapping) {
        if (mapping == null) {
            return new HashMap<>();
        }
        // Actions like create index and legacy create/update index template will have the mapping properties under a
        // _doc key. Other actions like update mapping and create/update index template will not have the _doc layer.
        try {
            if (mapping.containsKey(DOC) && mapping.get(DOC) instanceof Map) {
                Map<String, Object> doc = (Map<String, Object>) mapping.get(DOC);
                if (doc.containsKey(PROPERTIES) && doc.get(PROPERTIES) instanceof Map) {
                    return (Map<String, Object>) doc.get(PROPERTIES);
                }
            } else if (mapping.containsKey(PROPERTIES) && mapping.get(PROPERTIES) instanceof Map) {
                return (Map<String, Object>) mapping.get(PROPERTIES);
            }
        } catch (ClassCastException e) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Failed to get the mapping properties" + " to process the semantic field due to %s",
                    e.getMessage()
                ),
                e
            );
        }

        return new HashMap<>();
    }

    /**
     * Validate the semantic field should have a string as the model id
     * @param semanticFieldPath full path to the semantic field
     * @param semanticFieldConfig config of the semantic field
     * @return error message is there is any otherwise return null
     */
    public static String validateModelId(@NonNull final String semanticFieldPath, @NonNull final Map<String, Object> semanticFieldConfig) {
        Object modelId = semanticFieldConfig.get(MODEL_ID);
        if (modelId == null) {
            return String.format(Locale.ROOT, "%s is required for the semantic field at %s", MODEL_ID, semanticFieldPath);
        }

        if (modelId instanceof String == false || ((String) modelId).isEmpty()) {
            return String.format(Locale.ROOT, "%s should be a non-empty string for the semantic field at %s", MODEL_ID, semanticFieldPath);
        }

        return null;
    }

    /**
     * Validate if the semantic info field name exist it should be a non-empty string without dot
     * @param semanticFieldPath full path to the semantic field
     * @param semanticFieldConfig config of the semantic field
     * @return error message is there is any otherwise return null
     */
    public static String validateSemanticInfoFieldName(
        @NonNull final String semanticFieldPath,
        @NonNull final Map<String, Object> semanticFieldConfig
    ) {
        if (semanticFieldConfig.containsKey(SEMANTIC_INFO_FIELD_NAME)) {
            final Object semanticInfoFieldName = semanticFieldConfig.get(SEMANTIC_INFO_FIELD_NAME);
            if (semanticInfoFieldName instanceof String semanticInfoFieldNameStr) {
                if (semanticInfoFieldNameStr.isEmpty()) {
                    return String.format(
                        Locale.ROOT,
                        "%s cannot be an empty string for the semantic field at %s",
                        SEMANTIC_INFO_FIELD_NAME,
                        semanticFieldPath
                    );
                }

                // OpenSearch allows to define a field name with "." in the index mapping and will unflatten it later
                // but in our case it's not necessary to support "." in the custom semantic info field name. So add this
                // validation to block it.
                if (semanticInfoFieldNameStr.contains(".")) {
                    return String.format(
                        Locale.ROOT,
                        "%s should not contain '.' for the semantic field at %s",
                        SEMANTIC_INFO_FIELD_NAME,
                        semanticFieldPath

                    );
                }
            } else {
                return String.format(
                    Locale.ROOT,
                    "%s should be a non-empty string for the semantic field at %s",
                    SEMANTIC_INFO_FIELD_NAME,
                    semanticFieldPath
                );
            }
        }
        // SEMANTIC_INFO_FIELD_NAME is an optional field. If it does not exist we simply return null to show no error.
        return null;
    }

    /**
     * Get the config of the target field from the index mapping by its path.
     * @param mapping index mapping
     * @param path path to the target field
     * @return The config of the target field in the index mapping.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> getFieldConfigByPath(final Map<String, Object> mapping, final String path) {
        final String[] paths = path.split("\\.");
        Map<String, Object> currentMapping = getProperties(mapping);
        for (int i = 0; i < paths.length; i++) {
            if (currentMapping == null) {
                return null;
            }
            final Object temp = currentMapping.get(paths[i]);
            if (temp instanceof Map) {
                currentMapping = (Map<String, Object>) temp;
                // handle the object field in the path
                if (i < paths.length - 1 && currentMapping.containsKey(PROPERTIES)) {
                    currentMapping = (Map<String, Object>) currentMapping.get(PROPERTIES);
                }
            } else {
                return null;
            }
        }
        return currentMapping;
    }

    /**
     * Extract the target field config from the index mapping based on the field name
     * @param fieldName full path to the target field
     * @param targetIndexMetadataList target index metadata
     * @return index to target field config map
     */
    public static Map<String, NeuralQueryTargetFieldConfig> getIndexToTargetFieldConfigMapFromIndexMetadata(
        @NonNull final String fieldName,
        @NonNull final List<IndexMetadata> targetIndexMetadataList
    ) {
        final Map<String, NeuralQueryTargetFieldConfig> indexToTargetFieldConfigMap = new HashMap<>();
        for (IndexMetadata indexMetadata : targetIndexMetadataList) {
            if (indexMetadata == null) {
                continue;
            }
            final MappingMetadata mappingMetadata = indexMetadata.mapping();
            final NeuralQueryTargetFieldConfig.NeuralQueryTargetFieldConfigBuilder targetFieldConfigBuilder = NeuralQueryTargetFieldConfig
                .builder();
            if (mappingMetadata == null) {
                indexToTargetFieldConfigMap.put(
                    indexMetadata.getIndex().toString(),
                    targetFieldConfigBuilder.isUnmappedField(true).build()
                );
                continue;
            }
            final Map<String, Object> mappings = mappingMetadata.sourceAsMap();
            final Map<String, Object> targetFieldConfig = SemanticMappingUtils.getFieldConfigByPath(mappings, fieldName);
            if (targetFieldConfig == null) {
                indexToTargetFieldConfigMap.put(
                    indexMetadata.getIndex().toString(),
                    targetFieldConfigBuilder.isUnmappedField(true).build()
                );
                continue;
            }
            targetFieldConfigBuilder.isUnmappedField(false);
            final Object targetFieldTypeObject = targetFieldConfig.get(TYPE);
            if (!(targetFieldTypeObject instanceof String targetFieldType)) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Failed to process the neural query against the field [%s] because it is an object field.",
                        fieldName
                    )
                );
            }

            if (SemanticFieldMapper.CONTENT_TYPE.equals(targetFieldType)) {
                final Map<String, Object> embeddingFieldConfig = getSemanticEmbeddingFieldConfig(
                    fieldName,
                    targetFieldConfig,
                    mappings,
                    targetFieldConfigBuilder
                );
                final String embeddingFieldType = (String) embeddingFieldConfig.get(TYPE);
                String semanticFieldSearchAnalyzer = null;
                if (targetFieldConfig.containsKey(SEMANTIC_FIELD_SEARCH_ANALYZER)) {
                    semanticFieldSearchAnalyzer = (String) targetFieldConfig.get(SEMANTIC_FIELD_SEARCH_ANALYZER);
                }
                // If we have search model id we should use it otherwise fall back to use the model id.
                String searchModelId = (String) targetFieldConfig.get(MODEL_ID);
                if (targetFieldConfig.containsKey(SEARCH_MODEL_ID)) {
                    searchModelId = (String) targetFieldConfig.get(SEARCH_MODEL_ID);
                }
                targetFieldConfigBuilder.embeddingFieldType(embeddingFieldType);
                targetFieldConfigBuilder.searchModelId(searchModelId);
                targetFieldConfigBuilder.isSemanticField(Boolean.TRUE);
                targetFieldConfigBuilder.semanticFieldSearchAnalyzer(semanticFieldSearchAnalyzer);
            } else if (KNNVectorFieldMapper.CONTENT_TYPE.equals(targetFieldType)) {
                targetFieldConfigBuilder.isSemanticField(Boolean.FALSE);
                targetFieldConfigBuilder.embeddingFieldType(targetFieldType);
            } else {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Failed to process the neural query against the field %s because its type is not supported. It should be one of [%s]",
                        fieldName,
                        String.join(",", SUPPORTED_TARGET_FIELD_TYPES)
                    )
                );
            }
            indexToTargetFieldConfigMap.put(indexMetadata.getIndex().toString(), targetFieldConfigBuilder.build());
        }

        return indexToTargetFieldConfigMap;
    }

    private static Map<String, Object> getSemanticEmbeddingFieldConfig(
        @NonNull final String fieldName,
        @NonNull final Map<String, Object> targetFieldConfig,
        @NonNull final Map<String, Object> mappings,
        @NonNull final NeuralQueryTargetFieldConfig.NeuralQueryTargetFieldConfigBuilder targetFieldConfigBuilder
    ) {
        final String semanticInfoFieldFullPath = getSemanticInfoFieldFullPath(targetFieldConfig, fieldName, fieldName);
        final Boolean chunkingEnabled = isChunkingEnabled(targetFieldConfig, fieldName);
        targetFieldConfigBuilder.chunkingEnabled(chunkingEnabled);
        String embeddingFieldPath;
        if (Boolean.TRUE.equals(chunkingEnabled)) {
            targetFieldConfigBuilder.chunksPath(semanticInfoFieldFullPath + PATH_SEPARATOR + CHUNKS_FIELD_NAME);
            embeddingFieldPath = new StringBuilder().append(semanticInfoFieldFullPath)
                .append(PATH_SEPARATOR)
                .append(CHUNKS_FIELD_NAME)
                .append(PATH_SEPARATOR)
                .append(EMBEDDING_FIELD_NAME)
                .toString();
        } else {
            embeddingFieldPath = semanticInfoFieldFullPath + PATH_SEPARATOR + EMBEDDING_FIELD_NAME;
        }
        targetFieldConfigBuilder.embeddingFieldPath(embeddingFieldPath);
        return SemanticMappingUtils.getFieldConfigByPath(mappings, embeddingFieldPath);
    }

    /**
     * Check if the chunking is enabled in the semantic field config. If the field is not defined then return false as
     * the default value.
     * @param fieldConfigMap The config for a semantic field.
     * @return If the chunking is enabled in the semantic filed config.
     */
    public static Boolean isChunkingEnabled(@NonNull final Map<String, Object> fieldConfigMap, @NonNull final String semanticFieldPath) {
        if (fieldConfigMap.containsKey(CHUNKING)) {
            final Object chunkingEnabledObj = fieldConfigMap.get(CHUNKING);
            if (chunkingEnabledObj instanceof Boolean) {
                return (Boolean) chunkingEnabledObj;
            } else {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "%s should be a boolean for the semantic field at %s", CHUNKING, semanticFieldPath)
                );
            }
        }

        return false;
    }

    /**
     * Check if the semantic field search analyzer is provided in the semantic field config.
     * If the field is not defined then return null as the default value.
     * @param fieldConfigMap The config for a semantic field.
     * @return if the semantic field search analyzer is provided in the semantic field config.
     */
    public static String getSemanticFieldSearchAnalyzer(
        @NonNull final Map<String, Object> fieldConfigMap,
        @NonNull final String semanticFieldPath
    ) {
        if (fieldConfigMap.containsKey(SEMANTIC_FIELD_SEARCH_ANALYZER)) {
            final Object semanticFieldSearchAnalyzer = fieldConfigMap.get(SEMANTIC_FIELD_SEARCH_ANALYZER);
            if (semanticFieldSearchAnalyzer instanceof String) {
                return (String) semanticFieldSearchAnalyzer;
            } else {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "%s should be a String for the semantic field at %s",
                        SEMANTIC_FIELD_SEARCH_ANALYZER,
                        semanticFieldPath
                    )
                );
            }
        }

        return null;
    }
}
