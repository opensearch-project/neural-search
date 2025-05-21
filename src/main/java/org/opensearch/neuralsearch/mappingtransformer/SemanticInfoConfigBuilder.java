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
import org.opensearch.ml.common.model.TextEmbeddingModelConfig;
import reactor.util.annotation.NonNull;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.neuralsearch.constants.MappingConstants.PROPERTIES;
import static org.opensearch.neuralsearch.constants.MappingConstants.TYPE;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.EMBEDDING_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.CHUNKS_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.CHUNKS_TEXT_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.DEFAULT_MODEL_CONFIG;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.KNN_VECTOR_DIMENSION_FIELD_NAME;
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
        final Map<String, Object> embeddingFieldConfig = switch (embeddingFieldType) {
            case KNNVectorFieldMapper.CONTENT_TYPE -> buildKnnFieldConfig();
            case RankFeaturesFieldMapper.CONTENT_TYPE -> buildRankFeaturesFieldConfig();
            default -> throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Cannot build the semantic" + " info config because the embedding field type %s is not supported.",
                    embeddingFieldType
                )
            );
        };

        if (chunkingEnabled) {
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

    private Map<String, Object> buildKnnFieldConfig() {
        final Map<String, Object> config = new HashMap<>();
        config.put(TYPE, KNNVectorFieldMapper.CONTENT_TYPE);
        config.put(KNN_VECTOR_DIMENSION_FIELD_NAME, this.embeddingDimension);
        final Map<String, Object> methodConfig = new HashMap<>();
        methodConfig.put(KNN_VECTOR_METHOD_NAME_FIELD_NAME, this.knnMethodName);
        methodConfig.put(KNN_VECTOR_METHOD_SPACE_TYPE_FIELD_NAME, this.spaceType);
        config.put(KNN_VECTOR_METHOD_FIELD_NAME, methodConfig);
        return config;
    }

    private Map<String, Object> buildRankFeaturesFieldConfig() {
        final Map<String, Object> config = new HashMap<>();
        config.put(TYPE, RankFeaturesFieldMapper.CONTENT_TYPE);
        return config;
    }

    // Here we also require the model id because sometimes the MLModel does not have that info.
    public SemanticInfoConfigBuilder mlModel(@NonNull final MLModel mlModel, @NonNull final String modelId) {
        switch (mlModel.getAlgorithm()) {
            case FunctionName.TEXT_EMBEDDING -> extractInfoForTextEmbeddingModel(mlModel, modelId);
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

    private void extractInfoForTextEmbeddingModel(@NonNull final MLModel mlModel, @NonNull final String modelId) {
        this.embeddingFieldType = KNNVectorFieldMapper.CONTENT_TYPE;

        if (mlModel.getModelConfig() instanceof TextEmbeddingModelConfig == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Model %s is a remote text embedding model but model config is not a text embedding config",
                    modelId
                )
            );
        }

        final TextEmbeddingModelConfig textEmbeddingModelConfig = (TextEmbeddingModelConfig) mlModel.getModelConfig();

        if (textEmbeddingModelConfig.getEmbeddingDimension() == null) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Model %s is a remote text embedding model but the embedding dimension is not defined in the model config.",
                    modelId
                )
            );
        }

        this.embeddingDimension = textEmbeddingModelConfig.getEmbeddingDimension();

        final Map<String, Object> additionalConfig = textEmbeddingModelConfig.getAdditionalConfig();
        if (additionalConfig != null) {
            final Object spaceTypeObject = additionalConfig.get(KNN_VECTOR_METHOD_SPACE_TYPE_FIELD_NAME);
            if (spaceTypeObject instanceof String == false) {
                throw createInvalidSpaceTypeException(modelId);
            }
            this.spaceType = (String) spaceTypeObject;
        } else {
            throw createInvalidSpaceTypeException(modelId);
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
            case FunctionName.TEXT_EMBEDDING -> extractInfoForTextEmbeddingModel(mlModel, modelId);
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
}
