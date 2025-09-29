/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mappingtransformer;

import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.mapper.RankFeaturesFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.knn.index.mapper.KNNVectorFieldMapper;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.model.MLModelConfig;
import org.opensearch.ml.common.model.RemoteModelConfig;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig;
import reactor.util.annotation.NonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.neuralsearch.constants.MappingConstants.PROPERTIES;
import static org.opensearch.neuralsearch.constants.MappingConstants.TYPE;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.DENSE_EMBEDDING_CONFIG;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SEMANTIC_FIELD_SEARCH_ANALYZER;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SPARSE_ENCODING_CONFIG;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.EMBEDDING_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.CHUNKS_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.CHUNKS_TEXT_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.DEFAULT_MODEL_CONFIG;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.KNN_VECTOR_DATA_TYPE_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.KNN_VECTOR_DIMENSION_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.KNN_VECTOR_INDEX_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.KNN_VECTOR_METHOD_DEFAULT_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.KNN_VECTOR_METHOD_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.KNN_VECTOR_METHOD_NAME_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.KNN_VECTOR_METHOD_SPACE_TYPE_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.MODEL_FIELD_NAME;
import static org.opensearch.neuralsearch.mappingtransformer.SemanticMappingTransformer.SUPPORTED_MODEL_ALGORITHMS;
import static org.opensearch.neuralsearch.mappingtransformer.SemanticMappingTransformer.SUPPORTED_REMOTE_MODEL_TYPES;

/**
 * SemanticInfoConfigBuilder helps build the semantic info fields config based on the ML model config
 */
public class SemanticInfoConfigBuilder {
    private final NamedXContentRegistry xContentRegistry;

    private String embeddingFieldType;
    private String spaceType;
    private String knnMethodName = KNN_VECTOR_METHOD_DEFAULT_NAME;
    private Integer embeddingDimension;
    private Boolean chunkingEnabled;
    private String semanticFieldSearchAnalyzer;
    private Map<String, Object> denseEmbeddingConfig;
    private boolean sparseEncodingConfigDefined;
    private final static List<String> UNSUPPORTED_DENSE_EMBEDDING_CONFIG = List.of(
        KNN_VECTOR_DIMENSION_FIELD_NAME,
        KNN_VECTOR_DATA_TYPE_FIELD_NAME,
        KNN_VECTOR_METHOD_SPACE_TYPE_FIELD_NAME
    );

    public SemanticInfoConfigBuilder(@NonNull final NamedXContentRegistry xContentRegistry) {
        this.xContentRegistry = xContentRegistry;
    };

    /**
     * It will construct the index mapping config for the semantic info fields.
     * {
     *     "chunks":{
     *         "type": "nested",
     *         "properties":{
     *             "text":{
     *                 "type": "text"
     *             },
     *             "embedding":{
     *                 ... // embedding config
     *             }
     *         }
     *     },
     *     "model":{
     *         ... // model config
     *     }
     * }
     *
     * @return Config of the semantic info fields.
     */
    public Map<String, Object> build() {
        validate();

        final Map<String, Object> embeddingFieldConfig = switch (embeddingFieldType) {
            case KNNVectorFieldMapper.CONTENT_TYPE -> buildKnnFieldConfig();
            case RankFeaturesFieldMapper.CONTENT_TYPE -> buildRankFeaturesFieldConfig();
            default -> throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Cannot build the semantic info config because the embedding field type %s is not supported.",
                    embeddingFieldType
                )
            );
        };

        if (Boolean.TRUE.equals(chunkingEnabled)) {
            final Map<String, Object> chunksConfig = Map.of(
                TYPE,
                ObjectMapper.NESTED_CONTENT_TYPE,
                PROPERTIES,
                Map.of(CHUNKS_TEXT_FIELD_NAME, Map.of(TYPE, TextFieldMapper.CONTENT_TYPE), EMBEDDING_FIELD_NAME, embeddingFieldConfig)
            );
            return Map.of(PROPERTIES, Map.of(CHUNKS_FIELD_NAME, chunksConfig, MODEL_FIELD_NAME, DEFAULT_MODEL_CONFIG));
        } else {
            return Map.of(PROPERTIES, Map.of(EMBEDDING_FIELD_NAME, embeddingFieldConfig, MODEL_FIELD_NAME, DEFAULT_MODEL_CONFIG));
        }
    }

    private void validate() {
        if (KNNVectorFieldMapper.CONTENT_TYPE.equals(embeddingFieldType)) {
            validateSearchAnalyzerNotDefined();
            validateSparseEncodingConfigNotDefined();
        }

        if (RankFeaturesFieldMapper.CONTENT_TYPE.equals(embeddingFieldType)) {
            validateDenseEmbeddingConfigNotDefined();
        }
    }

    private void validateSparseEncodingConfigNotDefined() {
        if (sparseEncodingConfigDefined) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Cannot build the semantic info config because the dense(text embedding) model cannot support %s.",
                    SPARSE_ENCODING_CONFIG
                )
            );
        }
    }

    private void validateDenseEmbeddingConfigNotDefined() {
        if (denseEmbeddingConfig != null && RankFeaturesFieldMapper.CONTENT_TYPE.equals(embeddingFieldType)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Cannot build the semantic info config because %s is not supported by the sparse model.",
                    DENSE_EMBEDDING_CONFIG
                )
            );
        }
    }

    private void validateSearchAnalyzerNotDefined() {
        if (semanticFieldSearchAnalyzer != null) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Cannot build the semantic info config because the dense(text embedding) model cannot support %s",
                    SEMANTIC_FIELD_SEARCH_ANALYZER
                )
            );
        }
    }

    private Map<String, Object> buildKnnFieldConfig() {
        final Map<String, Object> config = new HashMap<>();
        config.put(TYPE, KNNVectorFieldMapper.CONTENT_TYPE);
        config.put(KNN_VECTOR_DIMENSION_FIELD_NAME, this.embeddingDimension);

        boolean indexEnabled = true;
        if (denseEmbeddingConfig != null) {
            validateDenseEmbeddingConfig(denseEmbeddingConfig);

            if (denseEmbeddingConfig.containsKey(KNN_VECTOR_INDEX_FIELD_NAME)) {
                Object indexValue = denseEmbeddingConfig.get(KNN_VECTOR_INDEX_FIELD_NAME);
                indexEnabled = indexValue instanceof Boolean ? (Boolean) indexValue : true;
                config.put(KNN_VECTOR_INDEX_FIELD_NAME, indexValue);
            }

            denseEmbeddingConfig.entrySet()
                .stream()
                .filter(
                    entry -> !KNN_VECTOR_INDEX_FIELD_NAME.equals(entry.getKey()) && !KNN_VECTOR_METHOD_FIELD_NAME.equals(entry.getKey())
                )
                .forEach(entry -> config.put(entry.getKey(), entry.getValue()));
        }

        if (indexEnabled) {
            config.put(KNN_VECTOR_METHOD_FIELD_NAME, buildMethodConfig());
        }

        return config;
    }

    private Map<String, Object> buildMethodConfig() {
        final Map<String, Object> methodConfig = new HashMap<>();

        if (denseEmbeddingConfig != null) {
            Object methodConfigObj = denseEmbeddingConfig.get(KNN_VECTOR_METHOD_FIELD_NAME);
            if (methodConfigObj instanceof Map<?, ?>) {
                methodConfig.putAll((Map<String, Object>) methodConfigObj);
            } else if (methodConfigObj != null) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Cannot build the semantic info config because %s must be a Map when provided.",
                        KNN_VECTOR_METHOD_FIELD_NAME
                    )
                );
            }
        }

        methodConfig.putIfAbsent(KNN_VECTOR_METHOD_NAME_FIELD_NAME, this.knnMethodName);
        methodConfig.put(KNN_VECTOR_METHOD_SPACE_TYPE_FIELD_NAME, this.spaceType);
        return methodConfig;
    }

    private void validateDenseEmbeddingConfig(@NonNull final Map<String, Object> denseEmbeddingConfig) {
        UNSUPPORTED_DENSE_EMBEDDING_CONFIG.forEach(name -> {
            if (denseEmbeddingConfig.get(name) != null) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Cannot configure %s in %s in the semantic field.", name, DENSE_EMBEDDING_CONFIG)
                );
            }
        });

        if (denseEmbeddingConfig.get(KNN_VECTOR_METHOD_FIELD_NAME) != null
            && denseEmbeddingConfig.get(KNN_VECTOR_METHOD_FIELD_NAME) instanceof Map methodConfigMap) {
            if (methodConfigMap.get(KNN_VECTOR_METHOD_SPACE_TYPE_FIELD_NAME) != null) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Cannot configure %s in %s in the semantic field.",
                        KNN_VECTOR_METHOD_SPACE_TYPE_FIELD_NAME,
                        DENSE_EMBEDDING_CONFIG
                    )
                );
            }
        }

    }

    private Map<String, Object> buildRankFeaturesFieldConfig() {
        final Map<String, Object> config = new HashMap<>();
        config.put(TYPE, RankFeaturesFieldMapper.CONTENT_TYPE);
        return config;
    }

    /**
     * Extract the info from the ML model and set it in the SemanticInfoConfigBuilder for build later.
     * @param mlModel ML model info
     * @param modelId ID of the ML model
     * @return this
     */
    // Here we also require the model id because sometimes the MLModel does not have that info.
    public SemanticInfoConfigBuilder mlModel(@NonNull final MLModel mlModel, @NonNull final String modelId) {
        switch (mlModel.getAlgorithm()) {
            case FunctionName.TEXT_EMBEDDING -> extractInfoForTextEmbeddingModel(mlModel, modelId, false);
            case FunctionName.SPARSE_ENCODING, FunctionName.SPARSE_TOKENIZE -> extractInfoForSparseModel();
            case FunctionName.REMOTE -> extractInfoForRemoteModel(mlModel, modelId);
            default -> throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "The algorithm %s of the model %s is not supported in the semantic field. Supported algorithms: [%s].",
                    mlModel.getAlgorithm().name(),
                    modelId,
                    String.join(",", SUPPORTED_MODEL_ALGORITHMS)
                )
            );
        }

        return this;
    }

    private void extractInfoForTextEmbeddingModel(@NonNull final MLModel mlModel, @NonNull final String modelId, final boolean isRemote) {
        this.embeddingFieldType = KNNVectorFieldMapper.CONTENT_TYPE;
        TextEmbeddingInfo textEmbeddingInfo;

        if (isRemote) {
            textEmbeddingInfo = extractInfoForRemoteTextEmbeddingModel(mlModel.getModelConfig(), modelId);
        } else {
            textEmbeddingInfo = extractInfoForLocalTextEmbeddingModel(mlModel.getModelConfig(), modelId);
        }

        validateAndSetTextEmbeddingInfo(textEmbeddingInfo, modelId);
    }

    private void validateAndSetTextEmbeddingInfo(@NonNull final TextEmbeddingInfo textEmbeddingInfo, @NonNull final String modelId) {
        if (textEmbeddingInfo.getEmbeddingDimension() == null) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Model %s is a text embedding model, but the embedding dimension is not defined in the model config.",
                    modelId
                )
            );
        }
        this.embeddingDimension = textEmbeddingInfo.getEmbeddingDimension();

        if (textEmbeddingInfo.getAdditionalConfig() == null
            || textEmbeddingInfo.getAdditionalConfig().get(KNN_VECTOR_METHOD_SPACE_TYPE_FIELD_NAME) instanceof String == false) {
            throw createInvalidSpaceTypeException(modelId);
        }
        this.spaceType = (String) textEmbeddingInfo.getAdditionalConfig().get(KNN_VECTOR_METHOD_SPACE_TYPE_FIELD_NAME);
    }

    private TextEmbeddingInfo extractInfoForLocalTextEmbeddingModel(final MLModelConfig modelConfig, @NonNull final String modelId) {
        if (modelConfig instanceof TextEmbeddingModelConfig textEmbeddingModelConfig) {
            return new TextEmbeddingInfo(textEmbeddingModelConfig.getEmbeddingDimension(), textEmbeddingModelConfig.getAdditionalConfig());
        } else {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Model %s is a local text embedding model, but model_config is a %s rather than a TextEmbeddingModelConfig.",
                    modelId,
                    modelConfig.getClass().getName()
                )
            );
        }
    }

    private TextEmbeddingInfo extractInfoForRemoteTextEmbeddingModel(final MLModelConfig modelConfig, @NonNull final String modelId) {
        if (modelConfig instanceof RemoteModelConfig remoteModelConfig) {
            return new TextEmbeddingInfo(remoteModelConfig.getEmbeddingDimension(), remoteModelConfig.getAdditionalConfig());
        } else {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Model %s is marked as remote text embedding model, but model_config is a %s rather than a RemoteModelConfig.",
                    modelId,
                    modelConfig.getClass().getName()
                )
            );
        }
    }

    private IllegalArgumentException createInvalidSpaceTypeException(@NonNull final String modelId) {
        return new IllegalArgumentException(
            String.format(Locale.ROOT, "space_type is not defined or not a string in the additional_config of the model %s.", modelId)
        );
    }

    private void extractInfoForSparseModel() {
        this.embeddingFieldType = RankFeaturesFieldMapper.CONTENT_TYPE;
    }

    private void extractInfoForRemoteModel(@NonNull final MLModel mlModel, @NonNull final String modelId) {
        final MLModelConfig mlModelConfig = mlModel.getModelConfig();
        if (mlModelConfig == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Model config is null for the remote model %s.", modelId));
        }
        final String modelType = mlModel.getModelConfig().getModelType();
        final FunctionName modelTypeFunctionName;
        try {
            modelTypeFunctionName = FunctionName.from(modelType);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(getUnsupportedRemoteModelError(modelType, modelId));
        }

        switch (modelTypeFunctionName) {
            case FunctionName.TEXT_EMBEDDING -> extractInfoForTextEmbeddingModel(mlModel, modelId, true);
            case FunctionName.SPARSE_ENCODING, FunctionName.SPARSE_TOKENIZE -> extractInfoForSparseModel();
            default -> throw new IllegalArgumentException(getUnsupportedRemoteModelError(modelType, modelId));
        }
    }

    private String getUnsupportedRemoteModelError(final String modelType, @lombok.NonNull final String modelId) {
        return String.format(
            Locale.ROOT,
            "The model type %s of the model %s is not supported in the semantic field. Supported remote model types: [%s]",
            modelType,
            modelId,
            String.join(",", SUPPORTED_REMOTE_MODEL_TYPES)
        );
    }

    public SemanticInfoConfigBuilder chunkingEnabled(final Boolean chunkingEnabled) {
        this.chunkingEnabled = chunkingEnabled;
        return this;
    }

    public SemanticInfoConfigBuilder semanticFieldSearchAnalyzer(final String semanticFieldSearchAnalyzer) {
        this.semanticFieldSearchAnalyzer = semanticFieldSearchAnalyzer;
        return this;
    }

    public SemanticInfoConfigBuilder denseEmbeddingConfig(final Map<String, Object> denseEmbeddingConfig) {
        this.denseEmbeddingConfig = denseEmbeddingConfig;
        return this;
    }

    public SemanticInfoConfigBuilder sparseEncodingConfigDefined(final boolean sparseEncodingConfigDefined) {
        this.sparseEncodingConfigDefined = sparseEncodingConfigDefined;
        return this;
    }
}
