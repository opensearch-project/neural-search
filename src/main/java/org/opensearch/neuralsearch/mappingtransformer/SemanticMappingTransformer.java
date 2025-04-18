/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mappingtransformer;

import com.google.common.annotations.VisibleForTesting;
import joptsimple.internal.Strings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MappingTransformer;

import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig;
import org.opensearch.neuralsearch.constants.SemanticFieldConstants;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.util.SemanticMappingUtils;
import reactor.util.annotation.NonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.neuralsearch.constants.MappingConstants.PROPERTIES;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.MODEL_ID;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SEMANTIC_INFO_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.KNN_VECTOR_DIMENSION_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.KNN_VECTOR_METHOD_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.KNN_VECTOR_METHOD_SPACE_TYPE_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.getBaseSemanticInfoConfig;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.getBaseSparseEmbeddingConfig;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.getBaseTextEmbeddingConfig;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.getProperties;

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

    private final int FIELD_NAME_MAX_LENGTH = 512;

    private final MLCommonsClientAccessor mlClientAccessor;
    private final NamedXContentRegistry xContentRegistry;

    public SemanticMappingTransformer(
        @NonNull final MLCommonsClientAccessor mlClientAccessor,
        @NonNull final NamedXContentRegistry xContentRegistry
    ) {
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
            if (properties == null) {
                listener.onResponse(null);
                return;
            }

            final Map<String, Map<String, Object>> semanticFieldPathToConfigMap = new HashMap<>();
            final String rootPath = Strings.EMPTY;
            SemanticMappingUtils.collectSemanticField(properties, rootPath, semanticFieldPathToConfigMap);

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
            errors.addAll(validateModelId(semanticFieldPath, semanticFieldConfig));
            errors.addAll(validateSemanticInfoFieldName(semanticFieldPath, semanticFieldConfig));
        }
        if (!errors.isEmpty()) {
            throw new IllegalArgumentException(String.join("; ", errors));
        }
    }

    private List<String> validateModelId(@NonNull final String semanticFieldPath, @NonNull final Map<String, Object> semanticFieldConfig) {
        final List<String> errors = new ArrayList<>();
        if (!semanticFieldConfig.containsKey(MODEL_ID)) {
            errors.add(String.format(Locale.ROOT, "%s is required for the semantic field at %s", MODEL_ID, semanticFieldPath));
        } else {
            Object modelId = semanticFieldConfig.get(MODEL_ID);
            if (!(modelId instanceof String modelIdStr) || modelIdStr.isEmpty()) {
                errors.add(
                    String.format(Locale.ROOT, "%s should be a non-empty string for the semantic field at %s", MODEL_ID, semanticFieldPath)
                );
            }
        }
        return errors;
    }

    private List<String> validateSemanticInfoFieldName(
        @NonNull final String semanticFieldPath,
        @NonNull final Map<String, Object> semanticFieldConfig
    ) {
        final List<String> errors = new ArrayList<>();
        if (semanticFieldConfig.containsKey(SEMANTIC_INFO_FIELD_NAME)) {
            final Object semanticInfoFieldName = semanticFieldConfig.get(SEMANTIC_INFO_FIELD_NAME);
            if (semanticInfoFieldName instanceof String semanticInfoFieldNameStr) {
                if (semanticInfoFieldNameStr.isEmpty()) {
                    errors.add(
                        String.format(
                            Locale.ROOT,
                            "%s cannot be an empty string for the semantic " + "field at %s",
                            SEMANTIC_INFO_FIELD_NAME,
                            semanticFieldPath
                        )
                    );
                } else if (semanticInfoFieldNameStr.length() > FIELD_NAME_MAX_LENGTH) {
                    errors.add(
                        String.format(
                            Locale.ROOT,
                            "%s should not be longer than %d characters for " + "the semantic field at %s",
                            SEMANTIC_INFO_FIELD_NAME,
                            FIELD_NAME_MAX_LENGTH,
                            semanticFieldPath
                        )
                    );
                }
            } else {
                errors.add(
                    String.format(
                        Locale.ROOT,
                        "%s should be a non-empty string for the semantic field" + " at %s",
                        SEMANTIC_INFO_FIELD_NAME,
                        semanticFieldPath
                    )
                );
            }
        }
        return errors;
    }

    private void fetchModelAndModifyMapping(
        @NonNull final Map<String, Map<String, Object>> semanticFieldPathToConfigMap,
        @NonNull final Map<String, Object> mappings,
        @NonNull final ActionListener<Void> listener
    ) {
        final Map<String, List<String>> modelIdToFieldPathMap = SemanticMappingUtils.extractModelIdToFieldPathMap(
            semanticFieldPathToConfigMap
        );
        if (modelIdToFieldPathMap.isEmpty()) {
            listener.onResponse(null);
        }
        final AtomicInteger counter = new AtomicInteger(modelIdToFieldPathMap.size());
        final AtomicBoolean hasError = new AtomicBoolean(false);
        final List<String> errors = new ArrayList<>();
        final Map<String, MLModel> modelIdToConfigMap = new HashMap<>(modelIdToFieldPathMap.size());

        // We can have multiple semantic fields with different model ids, and we should get model config for each model
        for (String modelId : modelIdToFieldPathMap.keySet()) {
            mlClientAccessor.getModel(modelId, ActionListener.wrap(mlModel -> {
                modelIdToConfigMap.put(modelId, mlModel);
                if (counter.decrementAndGet() == 0) {
                    try {
                        if (hasError.get()) {
                            listener.onFailure(new RuntimeException(String.join("; ", errors)));
                        } else {
                            modifyMappings(modelIdToConfigMap, mappings, modelIdToFieldPathMap, semanticFieldPathToConfigMap);
                            listener.onResponse(null);
                        }
                    } catch (Exception e) {
                        errors.add(e.getMessage());
                        listener.onFailure(new RuntimeException(String.join("; ", errors)));
                    }
                }
            }, e -> {
                hasError.set(true);
                errors.add(e.getMessage());
                if (counter.decrementAndGet() == 0) {
                    listener.onFailure(new RuntimeException(String.join("; ", errors)));
                }
            }));
        }
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
        final Map<String, Object> embeddingConfig = switch (modelConfig.getAlgorithm()) {
            case FunctionName.TEXT_EMBEDDING -> createEmbeddingConfigForTextEmbeddingModel(modelConfig);
            case FunctionName.SPARSE_ENCODING, FunctionName.SPARSE_TOKENIZE -> getBaseSparseEmbeddingConfig();
            case FunctionName.REMOTE -> createEmbeddingConfigForRemoteModel(modelConfig);
            default -> throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "The algorithm %s of the model %s is not supported in the semantic field. Supported algorithms: [%s].",
                    modelConfig.getAlgorithm().name(),
                    modelConfig.getModelId(),
                    String.join(",", SUPPORTED_MODEL_ALGORITHMS)
                )
            );
        };

        return getBaseSemanticInfoConfig(embeddingConfig);
    }

    private Map<String, Object> createEmbeddingConfigForRemoteModel(@NonNull final MLModel modelConfig) {
        final String modelType = modelConfig.getModelConfig().getModelType();
        final String modelId = modelConfig.getModelId();
        final FunctionName modelTypeFunctionName;
        try {
            modelTypeFunctionName = FunctionName.from(modelType);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(getUnsupportedRemoteModelError(modelType, modelId));
        }

        return switch (modelTypeFunctionName) {
            case FunctionName.TEXT_EMBEDDING -> createEmbeddingConfigForTextEmbeddingModel(modelConfig);
            case FunctionName.SPARSE_ENCODING, FunctionName.SPARSE_TOKENIZE -> getBaseSparseEmbeddingConfig();
            default -> throw new IllegalArgumentException(getUnsupportedRemoteModelError(modelType, modelId));
        };
    }

    private String getUnsupportedRemoteModelError(final String modelType, @NonNull final String modelId) {
        return String.format(
            Locale.ROOT,
            "The model type %s of the model %s is not supported in the semantic field. Supported remote model types: [%s]",
            modelType,
            modelId,
            String.join(",", SUPPORTED_REMOTE_MODEL_TYPES)
        );

    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> createEmbeddingConfigForTextEmbeddingModel(@NonNull final MLModel modelConfig) {
        final Map<String, Object> embeddingConfig = getBaseTextEmbeddingConfig();
        final String modelId = modelConfig.getModelId();

        if (!(modelConfig.getModelConfig() instanceof TextEmbeddingModelConfig textEmbeddingModelConfig)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Model %s is a remote text embedding model but model config is not a text embedding config",
                    modelId
                )
            );
        }

        if (textEmbeddingModelConfig.getEmbeddingDimension() == null) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Model %s is a remote text embedding model but the embedding dimension is not defined in the model config.",
                    modelId
                )
            );
        }
        embeddingConfig.put(KNN_VECTOR_DIMENSION_FIELD_NAME, textEmbeddingModelConfig.getEmbeddingDimension());

        final Map<String, Object> allConfigMap;
        try {
            allConfigMap = MapperService.parseMapping(xContentRegistry, textEmbeddingModelConfig.getAllConfig());
        } catch (IOException e) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Failed to parse the all_config of the model %s. Invalid all_config: %s",
                    modelId,
                    textEmbeddingModelConfig.getAllConfig()
                )
            );
        }
        final Object spaceTypeObject = allConfigMap.get(KNN_VECTOR_METHOD_SPACE_TYPE_FIELD_NAME);
        if (!(spaceTypeObject instanceof String spaceTypeString)) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "space_type is not defined or not a string in the all_config of the model %s.", modelId)
            );
        }
        final Map<String, Object> methodConfig = (Map<String, Object>) embeddingConfig.get(KNN_VECTOR_METHOD_FIELD_NAME);
        methodConfig.put(KNN_VECTOR_METHOD_SPACE_TYPE_FIELD_NAME, spaceTypeString);

        return embeddingConfig;
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
            final Map<String, Object> interFieldConfig = (Map<String, Object>) current.get(paths[i]);
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
}
