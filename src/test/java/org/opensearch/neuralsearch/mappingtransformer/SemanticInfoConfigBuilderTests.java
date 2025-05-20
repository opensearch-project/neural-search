/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mappingtransformer;

import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        final TextEmbeddingModelConfig remoteTextEmbeddingModelConfig = mock(TextEmbeddingModelConfig.class);
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
            "Model dummyModelId is a remote text embedding model but the embedding dimension is not defined in the model config.";
        assertEquals(expectedErrorMessage, exception.getMessage());
    }

    public void testMlModel_whenRemoteModelMissingSpaceType_thenException() {
        final SemanticInfoConfigBuilder builder = new SemanticInfoConfigBuilder(namedXContentRegistry);

        // prepare mock model config
        final Integer embeddingDimension = 768;
        final String allConfig = "{}";
        final TextEmbeddingModelConfig remoteTextEmbeddingModelConfig = TextEmbeddingModelConfig.builder()
            .embeddingDimension(embeddingDimension)
            .allConfig(allConfig)
            .modelType(FunctionName.TEXT_EMBEDDING.name())
            .frameworkType(TextEmbeddingModelConfig.FrameworkType.HUGGINGFACE_TRANSFORMERS)
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

        final String expectedErrorMessage = "space_type is not defined or not a string in the all_config of the model dummyModelId.";
        assertEquals(expectedErrorMessage, exception.getMessage());
    }

}
