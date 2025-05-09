/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mappingtransformer;

import com.google.common.annotations.VisibleForTesting;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.mapper.MappingTransformer;

import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.neuralsearch.constants.SemanticFieldConstants;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.neuralsearch.constants.MappingConstants.PROPERTIES;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SEMANTIC_INFO_FIELD_NAME;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.collectSemanticField;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.extractModelIdToFieldPathMap;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.getProperties;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.validateModelId;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.validateSemanticInfoFieldName;

/**
 * SemanticMappingTransformer transforms the index mapping for the semantic field to auto add the semantic info fields
 * based on the ML model id defined in the semantic field.
 */
public class SemanticMappingTransformer implements MappingTransformer {
    public final static Set<String> SUPPORTED_MODEL_ALGORITHMS = Set.of(
        FunctionName.TEXT_EMBEDDING.name(),
        FunctionName.REMOTE.name(),
        FunctionName.SPARSE_ENCODING.name(),
        FunctionName.SPARSE_TOKENIZE.name()
    );
    public final static Set<String> SUPPORTED_REMOTE_MODEL_TYPES = Set.of(
        FunctionName.TEXT_EMBEDDING.name(),
        FunctionName.SPARSE_ENCODING.name(),
        FunctionName.SPARSE_TOKENIZE.name()
    );

    private final MLCommonsClientAccessor mlClientAccessor;
    private final NamedXContentRegistry xContentRegistry;

    public SemanticMappingTransformer(final MLCommonsClientAccessor mlClientAccessor, final NamedXContentRegistry xContentRegistry) {
        this.mlClientAccessor = mlClientAccessor;
        this.xContentRegistry = xContentRegistry;
    }

    /**
     * Add semantic info fields to the mapping.
     * @param mapping original mapping
     * e.g.
     *{
     *   "_doc": {
     *     "properties": {
     *       "semantic_field": {
     *         "model_id": "model_id",
     *         "type": "semantic"
     *       }
     *     }
     *   }
     * }
     *
     * It can be transformed to
     *{
     *   "_doc": {
     *     "properties": {
     *       "semantic_field": {
     *         "model_id": "model_id",
     *         "type": "semantic"
     *       },
     *       "semantic_field_semantic_info": {
     *         "properties": {
     *           "chunks": {
     *             "type": "nested",
     *             "properties": {
     *               "embedding": {
     *                 "type": "knn_vector",
     *                 "dimension": 768,
     *                 "method": {
     *                   "engine": "faiss",
     *                   "space_type": "l2",
     *                   "name": "hnsw",
     *                   "parameters": {}
     *                 }
     *               },
     *               "text": {
     *                 "type": "text"
     *               }
     *             }
     *           },
     *           "model": {
     *             "properties": {
     *               "id": {
     *                 "type": "text",
     *                 "index": false
     *               },
     *               "name": {
     *                 "type": "text",
     *                 "index": false
     *               },
     *               "type": {
     *                 "type": "text",
     *                 "index": false
     *               }
     *             }
     *           }
     *         }
     *       }
     *     }
     *   }
     * }
     * @param context context to help transform
     */

    @Override
    public void transform(final Map<String, Object> mapping, final TransformContext context, @NonNull final ActionListener<Void> listener) {
        try {
            final Map<String, Object> properties = getProperties(mapping);
            // If there is no property or its format is not valid we simply do nothing and rely on core to validate the
            // mappings and handle the error.
            if (properties.isEmpty()) {
                listener.onResponse(null);
                return;
            }

            final Map<String, Map<String, Object>> semanticFieldPathToConfigMap = new HashMap<>();

            collectSemanticField(properties, semanticFieldPathToConfigMap);

            validateSemanticFields(semanticFieldPathToConfigMap);

            fetchModelAndModifyMapping(semanticFieldPathToConfigMap, properties, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }

    }

    private void validateSemanticFields(@NonNull final Map<String, Map<String, Object>> semanticFieldPathToConfigMap) {
        final List<String> errors = new ArrayList<>();
        for (Map.Entry<String, Map<String, Object>> entry : semanticFieldPathToConfigMap.entrySet()) {
            final String semanticFieldPath = entry.getKey();
            final Map<String, Object> semanticFieldConfig = entry.getValue();
            final String validateModelIdError = validateModelId(semanticFieldPath, semanticFieldConfig);
            final String validateSemanticInfoFieldNameError = validateSemanticInfoFieldName(semanticFieldPath, semanticFieldConfig);
            if (validateModelIdError != null) {
                errors.add(validateModelIdError);
            }
            if (validateSemanticInfoFieldNameError != null) {
                errors.add(validateSemanticInfoFieldNameError);
            }
        }
        if (errors.isEmpty() == false) {
            throw new IllegalArgumentException(String.join("; ", errors));
        }
    }

    private void fetchModelAndModifyMapping(
        @NonNull final Map<String, Map<String, Object>> semanticFieldPathToConfigMap,
        @NonNull final Map<String, Object> mappings,
        @NonNull final ActionListener<Void> listener
    ) {
        final Map<String, List<String>> modelIdToFieldPathMap = extractModelIdToFieldPathMap(semanticFieldPathToConfigMap);

        mlClientAccessor.getModels(modelIdToFieldPathMap.keySet(), modelIdToConfigMap -> {
            modifyMappings(modelIdToConfigMap, mappings, modelIdToFieldPathMap, semanticFieldPathToConfigMap);
            listener.onResponse(null);
        }, listener::onFailure);
    }

    private void modifyMappings(
        @NonNull final Map<String, MLModel> modelIdToConfigMap,
        @NonNull final Map<String, Object> mappings,
        @NonNull final Map<String, List<String>> modelIdToFieldPathMap,
        @NonNull final Map<String, Map<String, Object>> semanticFieldPathToConfigMap
    ) {
        for (String modelId : modelIdToFieldPathMap.keySet()) {
            final MLModel mlModel = modelIdToConfigMap.get(modelId);
            final List<String> fieldPathList = modelIdToFieldPathMap.get(modelId);
            for (String fieldPath : fieldPathList) {
                try {
                    final Map<String, Object> semanticInfoConfig = createSemanticInfoField(mlModel);
                    final Map<String, Object> fieldConfig = semanticFieldPathToConfigMap.get(fieldPath);
                    setSemanticInfoField(mappings, fieldPath, fieldConfig.get(SEMANTIC_INFO_FIELD_NAME), semanticInfoConfig);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(getModifyMappingErrorMessage(fieldPath, e.getMessage()), e);
                } catch (Exception e) {
                    throw new RuntimeException(getModifyMappingErrorMessage(fieldPath, e.getMessage()), e);
                }
            }
        }
    }

    private String getModifyMappingErrorMessage(@NonNull final String fieldPath, final String cause) {
        return String.format(Locale.ROOT, "Failed to transform the mapping for the semantic field at %s due to %s", fieldPath, cause);
    }

    @VisibleForTesting
    private Map<String, Object> createSemanticInfoField(final @NonNull MLModel modelConfig) {
        SemanticInfoConfigBuilder builder = new SemanticInfoConfigBuilder(xContentRegistry);
        return builder.mlModel(modelConfig).build();
    }

    @SuppressWarnings("unchecked")
    private void setSemanticInfoField(
        @NonNull final Map<String, Object> mappings,
        @NonNull final String fullPath,
        final Object userDefinedSemanticInfoFieldName,
        @NonNull final Map<String, Object> semanticInfoConfig
    ) {
        Map<String, Object> current = mappings;
        final String[] paths = fullPath.split("\\.");
        final String semanticInfoFieldName = userDefinedSemanticInfoFieldName == null
            ? paths[paths.length - 1] + SemanticFieldConstants.DEFAULT_SEMANTIC_INFO_FIELD_NAME_SUFFIX
            : (String) userDefinedSemanticInfoFieldName;

        for (int i = 0; i < paths.length - 1; i++) {
            String interFieldName = paths[i];
            Map<String, Object> interFieldConfig = (Map<String, Object>) current.get(interFieldName);

            // In OpenSearch we allow users to use "." in the field name when they define the index mapping. OpenSearch
            // core wll unflatten it later in the mapper service but here we need to unflatten the path to the semantic
            // field so that we can set the semantic info fields config by the path.
            if (interFieldConfig == null) {
                interFieldConfig = unflattenMapping(interFieldName, current);

            }

            // handle the case when the inter field is an object field
            if (interFieldConfig.containsKey(PROPERTIES)) {
                current = (Map<String, Object>) interFieldConfig.get(PROPERTIES);
            }
        }

        // We simply set the whole semantic info config at the path of the semantic info. It is possible the config of
        // semantic info fields can be invalid, but we will not validate it here. We will rely on the field mappers to
        // validate them when they parse the mappings.
        current.put(semanticInfoFieldName, semanticInfoConfig);
    }

    /**
     * e.g. input:
     * interFieldName: description
     * current mapping:
     * {
     *   "description.test1": {
     *         "type": "text"
     *    }
     *    "description.test2": {
     *          "type": "text"
     *    }
     * }
     * then output:
     * {
     *     test1": {
     *         "type": "text"
     *     },
     *     test2": {
     *         "type": "text"
     *     }
     * }
     *
     * @param interFieldName inter field name
     * @param current current mapping
     * @return unflattened inter field config
     */
    private Map<String, Object> unflattenMapping(@NonNull final String interFieldName, @NonNull final Map<String, Object> current) {
        final String prefix = interFieldName + ".";
        final Map<String, Object> properties = new HashMap<>();
        final Map<String, Object> interFieldConfig = new HashMap<>();
        interFieldConfig.put(PROPERTIES, properties);
        final Set<String> matchedKeySet = current.keySet().stream().filter(k -> k.startsWith(prefix)).collect(Collectors.toSet());
        for (String key : matchedKeySet) {
            properties.put(key.substring(prefix.length()), current.get(key));
        }
        matchedKeySet.forEach(current::remove);
        current.put(interFieldName, interFieldConfig);
        return interFieldConfig;
    }
}
