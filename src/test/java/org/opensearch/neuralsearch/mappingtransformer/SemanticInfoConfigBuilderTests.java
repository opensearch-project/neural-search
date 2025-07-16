/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mappingtransformer;

import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.model.RemoteModelConfig;
import org.opensearch.ml.common.model.BaseModelConfig;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.constants.MappingConstants.PROPERTIES;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.EMBEDDING_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.KNN_VECTOR_DIMENSION_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.KNN_VECTOR_METHOD_DEFAULT_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.KNN_VECTOR_METHOD_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.KNN_VECTOR_METHOD_NAME_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.KNN_VECTOR_METHOD_SPACE_TYPE_FIELD_NAME;

public class SemanticInfoConfigBuilderTests extends OpenSearchTestCase {
    private final NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(Collections.emptyList());

    public void testMlModel_whenUnsupportedModelType_thenException() {
        final SemanticInfoConfigBuilder builder = new SemanticInfoConfigBuilder(namedXContentRegistry);

        // prepare mock model config
        final MLModel unsupportedModel = MLModel.builder().modelId("unsupportedModelId").algorithm(FunctionName.QUESTION_ANSWERING).build();

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> builder.mlModel(unsupportedModel, "unsupportedModelId")
        );

        final String expectedErrorMessage =
            "The algorithm QUESTION_ANSWERING of the model unsupportedModelId is not supported in the semantic field. Supported algorithms:";
        assertTrue(exception.getMessage(), exception.getMessage().contains(expectedErrorMessage));
    }

    public void testBuild_whenRemoteModelMissingEmbeddingDimension_thenException() {
        final SemanticInfoConfigBuilder builder = new SemanticInfoConfigBuilder(namedXContentRegistry);

        // prepare mock model config
        final RemoteModelConfig remoteTextEmbeddingModelConfig = mock(RemoteModelConfig.class);
        final MLModel dummyModel = MLModel.builder()
            .modelId("dummyModelId")
            .algorithm(FunctionName.REMOTE)
            .modelConfig(remoteTextEmbeddingModelConfig)
            .build();
        when(remoteTextEmbeddingModelConfig.getModelType()).thenReturn(FunctionName.TEXT_EMBEDDING.name());
        when(remoteTextEmbeddingModelConfig.getAllConfig()).thenReturn("{\"space_type\":\"l2\"}");
        when(remoteTextEmbeddingModelConfig.getEmbeddingDimension()).thenReturn(null);

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> builder.mlModel(dummyModel, "dummyModelId")
        );

        final String expectedErrorMessage =
            "Model dummyModelId is a text embedding model, but the embedding dimension is not defined in the model config.";
        assertEquals(expectedErrorMessage, exception.getMessage());
    }

    public void testMlModel_whenRemoteModelInvalidSpaceType_thenException() {
        final SemanticInfoConfigBuilder builder = new SemanticInfoConfigBuilder(namedXContentRegistry);

        // prepare mock model config
        final Integer embeddingDimension = 768;
        final String allConfig = "{}";
        final RemoteModelConfig remoteTextEmbeddingModelConfig = RemoteModelConfig.builder()
            .embeddingDimension(embeddingDimension)
            .allConfig(allConfig)
            .modelType(FunctionName.TEXT_EMBEDDING.name())
            .additionalConfig(Map.of("space_type", 1))
            .frameworkType(RemoteModelConfig.FrameworkType.HUGGINGFACE_TRANSFORMERS)
            .build();
        final MLModel remoteTextEmbeddingModel = MLModel.builder()
            .modelId("dummyModelId")
            .algorithm(FunctionName.REMOTE)
            .modelConfig(remoteTextEmbeddingModelConfig)
            .build();

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> builder.mlModel(remoteTextEmbeddingModel, "dummyModelId")
        );

        final String expectedErrorMessage = "space_type is not defined or not a string in the additional_config of the model dummyModelId.";
        assertEquals(expectedErrorMessage, exception.getMessage());
    }

    public void testMlModel_whenRemoteDenseModel_thenSuccess() {
        final SemanticInfoConfigBuilder builder = new SemanticInfoConfigBuilder(namedXContentRegistry);

        // prepare mock model config
        final Integer embeddingDimension = 768;
        final String allConfig = "{}";
        final RemoteModelConfig remoteTextEmbeddingModelConfig = RemoteModelConfig.builder()
            .embeddingDimension(embeddingDimension)
            .allConfig(allConfig)
            .modelType(FunctionName.TEXT_EMBEDDING.name())
            .additionalConfig(Map.of("space_type", "l2"))
            .frameworkType(RemoteModelConfig.FrameworkType.HUGGINGFACE_TRANSFORMERS)
            .build();
        final MLModel remoteTextEmbeddingModel = MLModel.builder()
            .modelId("dummyModelId")
            .algorithm(FunctionName.REMOTE)
            .modelConfig(remoteTextEmbeddingModelConfig)
            .build();

        builder.mlModel(remoteTextEmbeddingModel, "dummyModelId");
    }

    public void testBuild_whenDenseEmbeddingConfigWithSparseEmbedding_thenException() {
        final SemanticInfoConfigBuilder builder = new SemanticInfoConfigBuilder(namedXContentRegistry);

        // prepare mock model config
        final String modelId = "dummyModelId";
        final BaseModelConfig remoteSparseEmbeddingModelConfig = mock(BaseModelConfig.class);
        final MLModel dummyModel = MLModel.builder()
            .modelId(modelId)
            .algorithm(FunctionName.REMOTE)
            .modelConfig(remoteSparseEmbeddingModelConfig)
            .build();
        when(remoteSparseEmbeddingModelConfig.getModelType()).thenReturn(FunctionName.SPARSE_ENCODING.name());

        builder.mlModel(dummyModel, modelId);
        builder.denseEmbeddingConfig(Collections.emptyMap());

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

        final String expectedErrorMessage =
            "Cannot build the semantic info config because dense_embedding_config is not supported by the sparse model.";
        assertEquals(expectedErrorMessage, exception.getMessage());
    }

    public void testBuild_whenDenseEmbeddingConfigWithEmbeddingDimension_thenException() {
        final SemanticInfoConfigBuilder builder = new SemanticInfoConfigBuilder(namedXContentRegistry);

        // prepare mock model config
        final String modelId = "dummyModelId";
        final Integer embeddingDimension = 768;
        final String allConfig = "{}";
        final RemoteModelConfig remoteTextEmbeddingModelConfig = RemoteModelConfig.builder()
            .embeddingDimension(embeddingDimension)
            .allConfig(allConfig)
            .modelType(FunctionName.TEXT_EMBEDDING.name())
            .frameworkType(RemoteModelConfig.FrameworkType.HUGGINGFACE_TRANSFORMERS)
            .additionalConfig(Map.of(KNN_VECTOR_METHOD_SPACE_TYPE_FIELD_NAME, "l2"))
            .build();
        final MLModel remoteTextEmbeddingModel = MLModel.builder()
            .modelId(modelId)
            .algorithm(FunctionName.REMOTE)
            .modelConfig(remoteTextEmbeddingModelConfig)
            .build();
        builder.mlModel(remoteTextEmbeddingModel, modelId);
        builder.denseEmbeddingConfig(Map.of(KNN_VECTOR_DIMENSION_FIELD_NAME, 128));

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

        final String expectedErrorMessage = "Cannot configure dimension in dense_embedding_config in the semantic field.";
        assertEquals(expectedErrorMessage, exception.getMessage());
    }

    public void testBuild_whenDenseEmbeddingConfigWithMethodNotMap_thenException() {
        final SemanticInfoConfigBuilder builder = new SemanticInfoConfigBuilder(namedXContentRegistry);

        // prepare mock model config
        final String modelId = "dummyModelId";
        final Integer embeddingDimension = 768;
        final String allConfig = "{}";
        final RemoteModelConfig remoteTextEmbeddingModelConfig = RemoteModelConfig.builder()
            .embeddingDimension(embeddingDimension)
            .allConfig(allConfig)
            .modelType(FunctionName.TEXT_EMBEDDING.name())
            .frameworkType(RemoteModelConfig.FrameworkType.HUGGINGFACE_TRANSFORMERS)
            .additionalConfig(Map.of(KNN_VECTOR_METHOD_SPACE_TYPE_FIELD_NAME, "l2"))
            .build();
        final MLModel remoteTextEmbeddingModel = MLModel.builder()
            .modelId(modelId)
            .algorithm(FunctionName.REMOTE)
            .modelConfig(remoteTextEmbeddingModelConfig)
            .build();
        builder.mlModel(remoteTextEmbeddingModel, modelId);
        builder.denseEmbeddingConfig(Map.of(KNN_VECTOR_METHOD_FIELD_NAME, "invalid"));

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

        final String expectedErrorMessage = "Cannot build the semantic info config because method must be a Map when provided.";
        assertEquals(expectedErrorMessage, exception.getMessage());
    }

    public void testBuild_whenValidDenseEmbeddingConfig_thenOverride() {
        final SemanticInfoConfigBuilder builder = new SemanticInfoConfigBuilder(namedXContentRegistry);

        // prepare mock model config
        final String modelId = "dummyModelId";
        final Integer embeddingDimension = 768;
        final String allConfig = "{}";
        final RemoteModelConfig remoteTextEmbeddingModelConfig = RemoteModelConfig.builder()
            .embeddingDimension(embeddingDimension)
            .allConfig(allConfig)
            .modelType(FunctionName.TEXT_EMBEDDING.name())
            .frameworkType(RemoteModelConfig.FrameworkType.HUGGINGFACE_TRANSFORMERS)
            .additionalConfig(Map.of(KNN_VECTOR_METHOD_SPACE_TYPE_FIELD_NAME, "l2"))
            .build();
        final MLModel remoteTextEmbeddingModel = MLModel.builder()
            .modelId(modelId)
            .algorithm(FunctionName.REMOTE)
            .modelConfig(remoteTextEmbeddingModelConfig)
            .build();
        builder.mlModel(remoteTextEmbeddingModel, modelId);

        builder.denseEmbeddingConfig(Map.of(KNN_VECTOR_METHOD_FIELD_NAME, Map.of("engine", "lucene")));

        final Map<String, Object> semanticInfoConfig = builder.build();

        // verify
        final Map<String, Object> expectedMethod = Map.of(
            "engine",
            "lucene",
            KNN_VECTOR_METHOD_NAME_FIELD_NAME,
            KNN_VECTOR_METHOD_DEFAULT_NAME,
            KNN_VECTOR_METHOD_SPACE_TYPE_FIELD_NAME,
            "l2"
        );

        final Map<String, Object> properties = (Map<String, Object>) semanticInfoConfig.get(PROPERTIES);
        final Map<String, Object> embeddingConfig = (Map<String, Object>) properties.get(EMBEDDING_FIELD_NAME);
        final Map<String, Object> actualMethod = (Map<String, Object>) embeddingConfig.get(KNN_VECTOR_METHOD_FIELD_NAME);
        assertEquals(expectedMethod, actualMethod);
    }

    public void testBuild_whenSparseEncodingConfigWithDenseEmbedding_thenException() {
        final SemanticInfoConfigBuilder builder = new SemanticInfoConfigBuilder(namedXContentRegistry);

        // prepare mock model config
        final String modelId = "dummyModelId";
        final Integer embeddingDimension = 768;
        final String allConfig = "{}";
        final RemoteModelConfig remoteTextEmbeddingModelConfig = RemoteModelConfig.builder()
            .embeddingDimension(embeddingDimension)
            .allConfig(allConfig)
            .modelType(FunctionName.TEXT_EMBEDDING.name())
            .frameworkType(RemoteModelConfig.FrameworkType.HUGGINGFACE_TRANSFORMERS)
            .additionalConfig(Map.of(KNN_VECTOR_METHOD_SPACE_TYPE_FIELD_NAME, "l2"))
            .build();
        final MLModel dummyModel = MLModel.builder()
            .modelId(modelId)
            .algorithm(FunctionName.REMOTE)
            .modelConfig(remoteTextEmbeddingModelConfig)
            .build();

        builder.mlModel(dummyModel, modelId);
        builder.sparseEncodingConfigDefined(true);

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

        final String expectedErrorMessage =
            "Cannot build the semantic info config because the dense(text embedding) model cannot support sparse_encoding_config.";
        assertEquals(expectedErrorMessage, exception.getMessage());
    }
}
