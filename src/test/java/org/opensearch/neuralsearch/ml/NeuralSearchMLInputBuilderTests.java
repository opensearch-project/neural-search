/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.model.RemoteModelConfig;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig;
import org.opensearch.neuralsearch.processor.EmbeddingContentType;
import org.opensearch.neuralsearch.processor.InferenceRequest;
import org.opensearch.test.OpenSearchTestCase;

public class NeuralSearchMLInputBuilderTests extends OpenSearchTestCase {

    public void testCreateTextEmbeddingInput_remoteAsymmetricModel_query() {
        // Setup
        MLModel model = mock(MLModel.class);
        RemoteModelConfig config = mock(RemoteModelConfig.class);
        InferenceRequest request = mock(InferenceRequest.class);

        when(model.getModelConfig()).thenReturn(config);
        when(config.getAdditionalConfig()).thenReturn(Map.of("is_asymmetric", true));
        when(request.getEmbeddingContentType()).thenReturn(EmbeddingContentType.QUERY);

        List<String> inputTexts = Arrays.asList("test query");

        // Execute
        MLInput result = NeuralSearchMLInputBuilder.createTextEmbeddingInput(model, null, inputTexts, request);

        // Verify
        assertNotNull(result);
        assertEquals(FunctionName.REMOTE, result.getAlgorithm());

        RemoteInferenceInputDataSet dataset = (RemoteInferenceInputDataSet) result.getInputDataset();
        assertNotNull(dataset.getParameters());

        String textsJson = (String) dataset.getParameters().get(AsymmetricTextEmbeddingConstants.TEXTS_KEY);
        assertTrue(textsJson.contains("test query"));
        assertEquals("query", dataset.getParameters().get(AsymmetricTextEmbeddingConstants.CONTENT_TYPE_KEY));
    }

    public void testCreateTextEmbeddingInput_remoteAsymmetricModel_passage() {
        // Setup
        MLModel model = mock(MLModel.class);
        RemoteModelConfig config = mock(RemoteModelConfig.class);
        InferenceRequest request = mock(InferenceRequest.class);

        when(model.getModelConfig()).thenReturn(config);
        when(config.getAdditionalConfig()).thenReturn(Map.of("is_asymmetric", true));
        when(request.getEmbeddingContentType()).thenReturn(EmbeddingContentType.PASSAGE);

        List<String> inputTexts = Arrays.asList("test passage");

        // Execute
        MLInput result = NeuralSearchMLInputBuilder.createTextEmbeddingInput(model, null, inputTexts, request);

        // Verify
        RemoteInferenceInputDataSet dataset = (RemoteInferenceInputDataSet) result.getInputDataset();

        String textsJson = (String) dataset.getParameters().get(AsymmetricTextEmbeddingConstants.TEXTS_KEY);
        assertTrue(textsJson.contains("test passage"));
        assertEquals("passage", dataset.getParameters().get(AsymmetricTextEmbeddingConstants.CONTENT_TYPE_KEY));
    }

    public void testCreateTextEmbeddingInput_remoteSymmetricModel_throwsException() {
        // Setup
        MLModel model = mock(MLModel.class);
        RemoteModelConfig config = mock(RemoteModelConfig.class);
        InferenceRequest request = mock(InferenceRequest.class);

        when(model.getModelConfig()).thenReturn(config);
        when(config.getAdditionalConfig()).thenReturn(Map.of("is_asymmetric", false));

        List<String> inputTexts = Arrays.asList("test text");

        // Execute & Verify
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> NeuralSearchMLInputBuilder.createTextEmbeddingInput(model, null, inputTexts, request)
        );
        assertEquals("Remote models are only supported for asymmetric E5 text embedding", exception.getMessage());
    }

    public void testCreateTextEmbeddingInput_remoteModel_multipleInputs() {
        // Setup
        MLModel model = mock(MLModel.class);
        RemoteModelConfig config = mock(RemoteModelConfig.class);
        InferenceRequest request = mock(InferenceRequest.class);

        when(model.getModelConfig()).thenReturn(config);
        when(config.getAdditionalConfig()).thenReturn(Map.of("is_asymmetric", true));
        when(request.getEmbeddingContentType()).thenReturn(EmbeddingContentType.QUERY);

        List<String> inputTexts = Arrays.asList("text1", "text2", "text3");

        // Execute
        MLInput result = NeuralSearchMLInputBuilder.createTextEmbeddingInput(model, null, inputTexts, request);

        // Verify
        RemoteInferenceInputDataSet dataset = (RemoteInferenceInputDataSet) result.getInputDataset();

        @SuppressWarnings("unchecked")
        String textsJson = (String) dataset.getParameters().get(AsymmetricTextEmbeddingConstants.TEXTS_KEY);
        assertTrue(textsJson.contains("text1") && textsJson.contains("text2") && textsJson.contains("text3"));
    }

    public void testCreateTextEmbeddingInput_localModel() {
        // Setup
        MLModel model = mock(MLModel.class);
        TextEmbeddingModelConfig config = mock(TextEmbeddingModelConfig.class);
        InferenceRequest request = mock(InferenceRequest.class);

        when(model.getModelConfig()).thenReturn(config);
        when(config.getQueryPrefix()).thenReturn(null);
        when(config.getPassagePrefix()).thenReturn(null);
        when(request.getMlAlgoParams()).thenReturn(null);

        List<String> inputTexts = Arrays.asList("test text");

        // Execute
        MLInput result = NeuralSearchMLInputBuilder.createTextEmbeddingInput(model, null, inputTexts, request);

        // Verify
        assertNotNull(result);
        assertEquals(FunctionName.TEXT_EMBEDDING, result.getAlgorithm());
    }

    public void testCreateTextEmbeddingInput_remoteAsymmetricModel_withSpecialCharacters() {
        // Setup
        MLModel model = mock(MLModel.class);
        RemoteModelConfig config = mock(RemoteModelConfig.class);
        InferenceRequest request = mock(InferenceRequest.class);

        when(model.getModelConfig()).thenReturn(config);
        when(config.getAdditionalConfig()).thenReturn(Map.of("is_asymmetric", true));
        when(request.getEmbeddingContentType()).thenReturn(EmbeddingContentType.PASSAGE);

        List<String> inputTexts = Arrays.asList("text with \"quotes\" and \n newlines");

        // Execute
        MLInput result = NeuralSearchMLInputBuilder.createTextEmbeddingInput(model, null, inputTexts, request);

        // Verify
        RemoteInferenceInputDataSet dataset = (RemoteInferenceInputDataSet) result.getInputDataset();

        @SuppressWarnings("unchecked")
        String textsJson = (String) dataset.getParameters().get(AsymmetricTextEmbeddingConstants.TEXTS_KEY);
        assertTrue(textsJson.contains("quotes") && textsJson.contains("newlines"));
        assertEquals("passage", dataset.getParameters().get(AsymmetricTextEmbeddingConstants.CONTENT_TYPE_KEY));
    }
}
