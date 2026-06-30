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
import java.util.Locale;
import java.util.Map;

import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.dataset.TextDocsInputDataSet;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.model.RemoteModelConfig;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig;
import org.opensearch.neuralsearch.processor.EmbeddingContentType;
import org.opensearch.neuralsearch.processor.InferenceRequest;
import org.opensearch.test.OpenSearchTestCase;

public class NeuralSearchMLInputBuilderTests extends OpenSearchTestCase {

    public void testResolveFunctionName_whenModelAlgorithmPresent_thenReturnsAlgorithm() {
        MLModel model = mock(MLModel.class);
        when(model.getAlgorithm()).thenReturn(FunctionName.SPARSE_ENCODING);

        assertEquals(FunctionName.SPARSE_ENCODING, NeuralSearchMLInputBuilder.resolveFunctionName(model));
    }

    public void testResolveFunctionName_whenModelNull_thenThrowsException() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> NeuralSearchMLInputBuilder.resolveFunctionName(null)
        );
        assertEquals("ML model must not be null", exception.getMessage());
    }

    public void testResolveFunctionName_whenAlgorithmNull_thenThrowsException() {
        MLModel model = mock(MLModel.class);
        when(model.getModelId()).thenReturn("test-model-id");
        when(model.getAlgorithm()).thenReturn(null);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> NeuralSearchMLInputBuilder.resolveFunctionName(model)
        );
        assertEquals(
            String.format(Locale.ROOT, "Model algorithm must not be null for model id [%s]", "test-model-id"),
            exception.getMessage()
        );
    }

    public void testCreateTextEmbeddingInput_remoteAsymmetricModel_query() {
        MLModel model = mock(MLModel.class);
        RemoteModelConfig config = mock(RemoteModelConfig.class);
        InferenceRequest request = mock(InferenceRequest.class);

        when(model.getAlgorithm()).thenReturn(FunctionName.REMOTE);
        when(model.getModelConfig()).thenReturn(config);
        when(config.getAdditionalConfig()).thenReturn(Map.of("is_asymmetric", true));
        when(request.getEmbeddingContentType()).thenReturn(EmbeddingContentType.QUERY);

        List<String> inputTexts = Arrays.asList("test query");

        MLInput result = NeuralSearchMLInputBuilder.createTextEmbeddingInput(model, null, inputTexts, request);

        assertNotNull(result);
        assertEquals(FunctionName.REMOTE, result.getAlgorithm());

        RemoteInferenceInputDataSet dataset = (RemoteInferenceInputDataSet) result.getInputDataset();
        assertNotNull(dataset.getParameters());

        String textsJson = (String) dataset.getParameters().get(AsymmetricTextEmbeddingConstants.TEXTS_KEY);
        assertTrue(textsJson.contains("test query"));
        assertEquals("query", dataset.getParameters().get(AsymmetricTextEmbeddingConstants.CONTENT_TYPE_KEY));
    }

    public void testCreateTextEmbeddingInput_remoteAsymmetricModel_passage() {
        MLModel model = mock(MLModel.class);
        RemoteModelConfig config = mock(RemoteModelConfig.class);
        InferenceRequest request = mock(InferenceRequest.class);

        when(model.getAlgorithm()).thenReturn(FunctionName.REMOTE);
        when(model.getModelConfig()).thenReturn(config);
        when(config.getAdditionalConfig()).thenReturn(Map.of("is_asymmetric", true));
        when(request.getEmbeddingContentType()).thenReturn(EmbeddingContentType.PASSAGE);

        List<String> inputTexts = Arrays.asList("test passage");

        MLInput result = NeuralSearchMLInputBuilder.createTextEmbeddingInput(model, null, inputTexts, request);

        RemoteInferenceInputDataSet dataset = (RemoteInferenceInputDataSet) result.getInputDataset();

        String textsJson = (String) dataset.getParameters().get(AsymmetricTextEmbeddingConstants.TEXTS_KEY);
        assertTrue(textsJson.contains("test passage"));
        assertEquals("passage", dataset.getParameters().get(AsymmetricTextEmbeddingConstants.CONTENT_TYPE_KEY));
    }

    public void testCreateTextEmbeddingInput_remoteSymmetricModel_usesRegisteredAlgorithm() {
        MLModel model = mock(MLModel.class);
        RemoteModelConfig config = mock(RemoteModelConfig.class);
        InferenceRequest request = mock(InferenceRequest.class);

        when(model.getAlgorithm()).thenReturn(FunctionName.REMOTE);
        when(model.getModelConfig()).thenReturn(config);
        when(config.getAdditionalConfig()).thenReturn(Map.of("is_asymmetric", false));
        when(request.getMlAlgoParams()).thenReturn(null);

        List<String> inputTexts = Arrays.asList("test text");

        MLInput result = NeuralSearchMLInputBuilder.createTextEmbeddingInput(model, null, inputTexts, request);

        assertNotNull(result);
        assertEquals(FunctionName.REMOTE, result.getAlgorithm());
        assertTrue(result.getInputDataset() instanceof TextDocsInputDataSet);
    }

    public void testCreateTextEmbeddingInput_remoteModel_multipleInputs() {
        MLModel model = mock(MLModel.class);
        RemoteModelConfig config = mock(RemoteModelConfig.class);
        InferenceRequest request = mock(InferenceRequest.class);

        when(model.getAlgorithm()).thenReturn(FunctionName.REMOTE);
        when(model.getModelConfig()).thenReturn(config);
        when(config.getAdditionalConfig()).thenReturn(Map.of("is_asymmetric", true));
        when(request.getEmbeddingContentType()).thenReturn(EmbeddingContentType.QUERY);

        List<String> inputTexts = Arrays.asList("text1", "text2", "text3");

        MLInput result = NeuralSearchMLInputBuilder.createTextEmbeddingInput(model, null, inputTexts, request);

        RemoteInferenceInputDataSet dataset = (RemoteInferenceInputDataSet) result.getInputDataset();

        String textsJson = (String) dataset.getParameters().get(AsymmetricTextEmbeddingConstants.TEXTS_KEY);
        assertTrue(textsJson.contains("text1") && textsJson.contains("text2") && textsJson.contains("text3"));
    }

    public void testCreateTextEmbeddingInput_localModel() {
        MLModel model = mock(MLModel.class);
        TextEmbeddingModelConfig config = mock(TextEmbeddingModelConfig.class);
        InferenceRequest request = mock(InferenceRequest.class);

        when(model.getAlgorithm()).thenReturn(FunctionName.TEXT_EMBEDDING);
        when(model.getModelConfig()).thenReturn(config);
        when(config.getQueryPrefix()).thenReturn(null);
        when(config.getPassagePrefix()).thenReturn(null);
        when(request.getMlAlgoParams()).thenReturn(null);

        List<String> inputTexts = Arrays.asList("test text");

        MLInput result = NeuralSearchMLInputBuilder.createTextEmbeddingInput(model, null, inputTexts, request);

        assertNotNull(result);
        assertEquals(FunctionName.TEXT_EMBEDDING, result.getAlgorithm());
    }

    public void testCreateTextEmbeddingInput_sparseEncodingModel_usesRegisteredAlgorithm() {
        MLModel model = mock(MLModel.class);
        RemoteModelConfig config = mock(RemoteModelConfig.class);
        InferenceRequest request = mock(InferenceRequest.class);

        when(model.getAlgorithm()).thenReturn(FunctionName.SPARSE_ENCODING);
        when(model.getModelConfig()).thenReturn(config);
        when(config.getAdditionalConfig()).thenReturn(Map.of("is_asymmetric", false));
        when(request.getMlAlgoParams()).thenReturn(null);

        List<String> inputTexts = Arrays.asList("sparse text");

        MLInput result = NeuralSearchMLInputBuilder.createTextEmbeddingInput(model, null, inputTexts, request);

        assertEquals(FunctionName.SPARSE_ENCODING, result.getAlgorithm());
    }

    public void testCreateTextEmbeddingInput_remoteAsymmetricModel_withSpecialCharacters() {
        MLModel model = mock(MLModel.class);
        RemoteModelConfig config = mock(RemoteModelConfig.class);
        InferenceRequest request = mock(InferenceRequest.class);

        when(model.getAlgorithm()).thenReturn(FunctionName.REMOTE);
        when(model.getModelConfig()).thenReturn(config);
        when(config.getAdditionalConfig()).thenReturn(Map.of("is_asymmetric", true));
        when(request.getEmbeddingContentType()).thenReturn(EmbeddingContentType.PASSAGE);

        List<String> inputTexts = Arrays.asList("text with \"quotes\" and \n newlines");

        MLInput result = NeuralSearchMLInputBuilder.createTextEmbeddingInput(model, null, inputTexts, request);

        RemoteInferenceInputDataSet dataset = (RemoteInferenceInputDataSet) result.getInputDataset();

        String textsJson = (String) dataset.getParameters().get(AsymmetricTextEmbeddingConstants.TEXTS_KEY);
        assertTrue(textsJson.contains("quotes") && textsJson.contains("newlines"));
        assertEquals("passage", dataset.getParameters().get(AsymmetricTextEmbeddingConstants.CONTENT_TYPE_KEY));
    }

    public void testCreateMultimodalInputFromMap_localModel_usesRegisteredAlgorithm() {
        MLModel model = mock(MLModel.class);
        TextEmbeddingModelConfig config = mock(TextEmbeddingModelConfig.class);
        InferenceRequest request = mock(InferenceRequest.class);

        when(model.getAlgorithm()).thenReturn(FunctionName.TEXT_EMBEDDING);
        when(model.getModelConfig()).thenReturn(config);
        when(config.getQueryPrefix()).thenReturn(null);
        when(config.getPassagePrefix()).thenReturn(null);

        MLInput result = NeuralSearchMLInputBuilder.createMultimodalInputFromMap(model, null, Map.of("input_text", "hello"), request);

        assertEquals(FunctionName.TEXT_EMBEDDING, result.getAlgorithm());
    }

    public void testCreateTextSimilarityInput_usesProvidedFunctionName() {
        MLInput result = NeuralSearchMLInputBuilder.createTextSimilarityInput(
            FunctionName.TEXT_SIMILARITY,
            "query",
            List.of("doc1", "doc2")
        );

        assertEquals(FunctionName.TEXT_SIMILARITY, result.getAlgorithm());
    }

    public void testCreateQuestionAnsweringInput_usesProvidedFunctionName() {
        MLInput result = NeuralSearchMLInputBuilder.createQuestionAnsweringInput(FunctionName.QUESTION_ANSWERING, "question", "context");

        assertEquals(FunctionName.QUESTION_ANSWERING, result.getAlgorithm());
    }

    public void testCreateSingleRemoteHighlightingInput_usesProvidedFunctionName() {
        MLInput result = NeuralSearchMLInputBuilder.createSingleRemoteHighlightingInput(FunctionName.REMOTE, "question", "context");

        assertEquals(FunctionName.REMOTE, result.getAlgorithm());
    }

    public void testCreateBatchHighlightingInput_usesProvidedFunctionName() {
        MLInput result = NeuralSearchMLInputBuilder.createBatchHighlightingInput(
            FunctionName.REMOTE,
            List.of(Map.of("question", "q1", "context", "c1"))
        );

        assertEquals(FunctionName.REMOTE, result.getAlgorithm());
    }
}
