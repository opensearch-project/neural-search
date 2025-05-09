/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import org.opensearch.neuralsearch.constants.MappingConstants;
import org.opensearch.neuralsearch.mapper.SemanticFieldMapper;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.neuralsearch.constants.MappingConstants.DOC;
import static org.opensearch.neuralsearch.constants.MappingConstants.PROPERTIES;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.MODEL_ID;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SEMANTIC_INFO_FIELD_NAME;

/**
 * A util class to help process mapping with semantic field
 */
public class SemanticMappingUtils {
    private static final int MAX_DEPTH_OF_INDEX_MAPPING = 1000;

    /**
     * It will recursively traverse the mapping to collect the full path to field config map for semantic fields
     * @param currentMapping current mapping
     * @param semanticFieldPathToConfigMap path to field config map for semantic fields
     */
    public static void collectSemanticField(
        @NonNull final Map<String, Object> currentMapping,
        @NonNull final Map<String, Map<String, Object>> semanticFieldPathToConfigMap
    ) {
        collectSemanticField(currentMapping, "", 0, semanticFieldPathToConfigMap);
    }

    @SuppressWarnings("unchecked")
    private static void collectSemanticField(
        @NonNull final Map<String, Object> currentMapping,
        @NonNull final String parentPath,
        final int depth,
        @NonNull final Map<String, Map<String, Object>> semanticFieldPathToConfigMap
    ) {
        if (depth > MAX_DEPTH_OF_INDEX_MAPPING) {
            throw new IllegalArgumentException(
                "Cannot transform the mapping for semantic fields because its depth exceeds the maximum allowed depth "
                    + MAX_DEPTH_OF_INDEX_MAPPING
            );
        }
        for (Map.Entry<String, Object> entry : currentMapping.entrySet()) {
            final String fieldName = entry.getKey();
            final Object fieldConfig = entry.getValue();

            // Build the full path for the current field
            final String fullPath = parentPath.isEmpty() ? fieldName : parentPath + "." + fieldName;

            if (fieldConfig instanceof Map) {
                final Map<String, Object> fieldConfigMap = (Map<String, Object>) fieldConfig;

                if (isSemanticField(fieldConfigMap)) {
                    semanticFieldPathToConfigMap.put(fullPath, fieldConfigMap);
                }

                // If it's an object field, recurse into the sub fields
                if (fieldConfigMap.containsKey(MappingConstants.PROPERTIES)) {
                    collectSemanticField(
                        (Map<String, Object>) fieldConfigMap.get(MappingConstants.PROPERTIES),
                        fullPath,
                        depth + 1,
                        semanticFieldPathToConfigMap
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
     * Get the semantic info field name from the semantic field config. It will validate the name is a valid string
     * value if it exists.
     * @param fieldConfigMap semantic field config
     * @param fullPath full path to the semantic field
     * @return valid model id
     */
    public static String getSemanticInfoFieldName(@NonNull final Map<String, Object> fieldConfigMap, @NonNull final String fullPath) {
        final String error = validateSemanticInfoFieldName(fullPath, fieldConfigMap);
        if (error != null) {
            throw new IllegalArgumentException(error);
        }

        return fieldConfigMap.containsKey(SEMANTIC_INFO_FIELD_NAME) ? (String) fieldConfigMap.get(SEMANTIC_INFO_FIELD_NAME) : null;
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
}
