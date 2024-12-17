/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters;
import org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters.EmbeddingContentType;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig;
import org.opensearch.ml.common.output.MLOutput;
import org.opensearch.ml.common.output.model.MLResultDataType;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.neuralsearch.constants.TestCommonConstants;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor.InferenceRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.NodeNotConnectedException;

public class MLCommonsClientAccessorTests extends OpenSearchTestCase {

    @Mock
    private ActionListener<List<List<Float>>> resultListener;

    @Mock
    private ActionListener<List<Float>> singleSentenceResultListener;

    @Mock
    private MachineLearningNodeClient client;

    @InjectMocks
    private MLCommonsClientAccessor accessor;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    public void testInferenceSentence_whenValidInput_thenSuccess() {
        final List<Float> vector = new ArrayList<>(List.of(TestCommonConstants.PREDICT_VECTOR_ARRAY));
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        setupMocksForTextEmbeddingModelAsymmetryCheck(false);

        accessor.inferenceSentence(
            InferenceRequest.builder().modelId(TestCommonConstants.MODEL_ID).inputText(TestCommonConstants.SENTENCES_LIST.get(0)).build(),
            singleSentenceResultListener
        );

        Mockito.verify(client)
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(singleSentenceResultListener).onResponse(vector);
        Mockito.verifyNoMoreInteractions(singleSentenceResultListener);
    }

    private void setupMocksForTextEmbeddingModelAsymmetryCheck(boolean isAsymmetric) {
        MLModel modelMock = mock(MLModel.class);
        TextEmbeddingModelConfig textEmbeddingModelConfigMock = mock(TextEmbeddingModelConfig.class);
        Mockito.when(textEmbeddingModelConfigMock.getPassagePrefix()).thenReturn(isAsymmetric ? "passage: " : null);
        Mockito.when(textEmbeddingModelConfigMock.getQueryPrefix()).thenReturn(isAsymmetric ? "query: " : null);
        Mockito.when(modelMock.getModelConfig()).thenReturn(textEmbeddingModelConfigMock);
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(1);
            actionListener.onResponse(modelMock);
            return null;
        }).when(client).getModel(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(ActionListener.class));
    }

    public void testInferenceSentences_whenValidInputThenSuccess() {
        final List<List<Float>> vectorList = new ArrayList<>();
        vectorList.add(Arrays.asList(TestCommonConstants.PREDICT_VECTOR_ARRAY));
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        setupMocksForTextEmbeddingModelAsymmetryCheck(false);

        accessor.inferenceSentences(
            InferenceRequest.builder().modelId(TestCommonConstants.MODEL_ID).inputTexts(TestCommonConstants.SENTENCES_LIST).build(),
            resultListener
        );

        Mockito.verify(client)
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(resultListener).onResponse(vectorList);
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSentences_whenResultFromClient_thenEmptyVectorList() {
        final List<List<Float>> vectorList = new ArrayList<>();
        vectorList.add(Collections.emptyList());
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(new Float[] {}));
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        setupMocksForTextEmbeddingModelAsymmetryCheck(false);

        accessor.inferenceSentences(
            InferenceRequest.builder().modelId(TestCommonConstants.MODEL_ID).inputTexts(TestCommonConstants.SENTENCES_LIST).build(),
            resultListener
        );

        Mockito.verify(client)
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(resultListener).onResponse(vectorList);
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSentences_whenExceptionFromMLClient_thenFailure() {
        final RuntimeException exception = new RuntimeException();
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(exception);
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        setupMocksForTextEmbeddingModelAsymmetryCheck(false);

        accessor.inferenceSentences(
            InferenceRequest.builder()
                .modelId(TestCommonConstants.MODEL_ID)
                .inputTexts(TestCommonConstants.SENTENCES_LIST)
                .targetResponseFilters(TestCommonConstants.TARGET_RESPONSE_FILTERS)
                .build(),
            resultListener
        );

        Mockito.verify(client)
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(resultListener).onFailure(exception);
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSentences_whenNodeNotConnectedException_thenRetry_3Times() {
        final NodeNotConnectedException nodeNodeConnectedException = new NodeNotConnectedException(
            mock(DiscoveryNode.class),
            "Node not connected"
        );
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(nodeNodeConnectedException);
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        setupMocksForTextEmbeddingModelAsymmetryCheck(false);

        accessor.inferenceSentences(
            InferenceRequest.builder()
                .modelId(TestCommonConstants.MODEL_ID)
                .inputTexts(TestCommonConstants.SENTENCES_LIST)
                .targetResponseFilters(TestCommonConstants.TARGET_RESPONSE_FILTERS)
                .build(),
            resultListener
        );

        Mockito.verify(client, times(4))
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(resultListener).onFailure(nodeNodeConnectedException);
    }

    public void testInferenceSentences_whenNotConnectionException_thenNoRetry() {
        final IllegalStateException illegalStateException = new IllegalStateException("Illegal state");
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(illegalStateException);
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        setupMocksForTextEmbeddingModelAsymmetryCheck(false);

        accessor.inferenceSentences(
            InferenceRequest.builder()
                .modelId(TestCommonConstants.MODEL_ID)
                .targetResponseFilters(TestCommonConstants.TARGET_RESPONSE_FILTERS)
                .inputTexts(TestCommonConstants.SENTENCES_LIST)
                .build(),
            resultListener
        );

        Mockito.verify(client, times(1))
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(resultListener).onFailure(illegalStateException);
    }

    public void testInferenceSentences_whenModelAsymmetric_thenSuccess() {
        final List<Float> vector = new ArrayList<>(List.of(TestCommonConstants.PREDICT_VECTOR_ARRAY));
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        setupMocksForTextEmbeddingModelAsymmetryCheck(true);

        accessor.inferenceSentence(
            InferenceRequest.builder()
                .modelId(TestCommonConstants.MODEL_ID)
                .inputText(TestCommonConstants.SENTENCES_LIST.get(0))
                .mlAlgoParams(AsymmetricTextEmbeddingParameters.builder().embeddingContentType(EmbeddingContentType.PASSAGE).build())
                .build(),
            singleSentenceResultListener
        );

        Mockito.verify(client)
            .predict(
                Mockito.eq(TestCommonConstants.MODEL_ID),
                Mockito.argThat((MLInput input) -> input.getParameters() != null),
                Mockito.isA(ActionListener.class)
            );
        Mockito.verify(singleSentenceResultListener).onResponse(vector);
        Mockito.verifyNoMoreInteractions(singleSentenceResultListener);
    }

    public void testInferenceSentences_whenGetModelException_thenFailure() {
        final List<Float> vector = new ArrayList<>(List.of(TestCommonConstants.PREDICT_VECTOR_ARRAY));
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        RuntimeException exception = new RuntimeException("Bam!");
        setupMocksForTextEmbeddingModelAsymmetryCheck(exception);

        accessor.inferenceSentence(
            InferenceRequest.builder()
                .modelId(TestCommonConstants.MODEL_ID)
                .inputText(TestCommonConstants.SENTENCES_LIST.get(0))
                .mlAlgoParams(AsymmetricTextEmbeddingParameters.builder().embeddingContentType(EmbeddingContentType.PASSAGE).build())
                .build(),
            singleSentenceResultListener
        );

        Mockito.verify(client).getModel(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(ActionListener.class));
        Mockito.verify(singleSentenceResultListener).onFailure(exception);
        Mockito.verifyNoMoreInteractions(singleSentenceResultListener);
    }

    private void setupMocksForTextEmbeddingModelAsymmetryCheck(Exception exception) {
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(1);
            actionListener.onFailure(exception);
            return null;
        }).when(client).getModel(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(ActionListener.class));
    }

    public void testInferenceSentencesWithMapResult_whenValidInput_thenSuccess() {
        final Map<String, String> map = Map.of("key", "value");
        final ActionListener<List<Map<String, ?>>> resultListener = mock(ActionListener.class);
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(map));
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        setupMocksForTextEmbeddingModelAsymmetryCheck(false);

        accessor.inferenceSentencesWithMapResult(
            InferenceRequest.builder().modelId(TestCommonConstants.MODEL_ID).inputTexts(TestCommonConstants.SENTENCES_LIST).build(),
            resultListener
        );

        Mockito.verify(client)
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(resultListener).onResponse(List.of(map));
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSentencesWithMapResult_whenTensorOutputListEmpty_thenException() {
        final ActionListener<List<Map<String, ?>>> resultListener = mock(ActionListener.class);
        final ModelTensorOutput modelTensorOutput = new ModelTensorOutput(Collections.emptyList());
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(modelTensorOutput);
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        setupMocksForTextEmbeddingModelAsymmetryCheck(false);

        accessor.inferenceSentencesWithMapResult(
            InferenceRequest.builder().modelId(TestCommonConstants.MODEL_ID).inputTexts(TestCommonConstants.SENTENCES_LIST).build(),
            resultListener
        );

        Mockito.verify(client)
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(IllegalStateException.class);
        Mockito.verify(resultListener).onFailure(argumentCaptor.capture());
        assertEquals(
            "Empty model result produced. Expected at least [1] tensor output and [1] model tensor, but got [0]",
            argumentCaptor.getValue().getMessage()
        );
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSentencesWithMapResult_whenModelTensorListEmpty_thenException() {
        final ActionListener<List<Map<String, ?>>> resultListener = mock(ActionListener.class);
        final List<ModelTensors> tensorsList = new ArrayList<>();
        final List<ModelTensor> mlModelTensorList = new ArrayList<>();
        tensorsList.add(new ModelTensors(mlModelTensorList));
        final ModelTensorOutput modelTensorOutput = new ModelTensorOutput(tensorsList);
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(modelTensorOutput);
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        setupMocksForTextEmbeddingModelAsymmetryCheck(false);

        accessor.inferenceSentencesWithMapResult(
            InferenceRequest.builder().modelId(TestCommonConstants.MODEL_ID).inputTexts(TestCommonConstants.SENTENCES_LIST).build(),
            resultListener
        );

        Mockito.verify(client)
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(IllegalStateException.class);
        Mockito.verify(resultListener).onFailure(argumentCaptor.capture());
        assertEquals(
            "Empty model result produced. Expected at least [1] tensor output and [1] model tensor, but got [0]",
            argumentCaptor.getValue().getMessage()
        );
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSentencesWithMapResult_whenModelTensorListSizeBiggerThan1_thenSuccess() {
        final ActionListener<List<Map<String, ?>>> resultListener = mock(ActionListener.class);
        final List<ModelTensors> tensorsList = new ArrayList<>();
        final List<ModelTensor> mlModelTensorList = new ArrayList<>();
        final ModelTensor tensor = new ModelTensor("response", null, null, null, null, null, Map.of("key", "value"));
        mlModelTensorList.add(tensor);
        mlModelTensorList.add(tensor);
        tensorsList.add(new ModelTensors(mlModelTensorList));
        final ModelTensorOutput modelTensorOutput = new ModelTensorOutput(tensorsList);
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(modelTensorOutput);
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        setupMocksForTextEmbeddingModelAsymmetryCheck(false);

        accessor.inferenceSentencesWithMapResult(
            InferenceRequest.builder().modelId(TestCommonConstants.MODEL_ID).inputTexts(TestCommonConstants.SENTENCES_LIST).build(),
            resultListener
        );

        Mockito.verify(client)
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(resultListener).onResponse(List.of(Map.of("key", "value"), Map.of("key", "value")));
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSentencesWithMapResult_whenRetryableException_retry3Times() {
        final NodeNotConnectedException nodeNodeConnectedException = new NodeNotConnectedException(
            mock(DiscoveryNode.class),
            "Node not connected"
        );
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(nodeNodeConnectedException);
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        final ActionListener<List<Map<String, ?>>> resultListener = mock(ActionListener.class);

        setupMocksForTextEmbeddingModelAsymmetryCheck(false);

        accessor.inferenceSentencesWithMapResult(
            InferenceRequest.builder().modelId(TestCommonConstants.MODEL_ID).inputTexts(TestCommonConstants.SENTENCES_LIST).build(),
            resultListener
        );

        Mockito.verify(client, times(4))
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(resultListener).onFailure(nodeNodeConnectedException);
    }

    public void testInferenceSentencesWithMapResult_whenNotRetryableException_thenFail() {
        final IllegalStateException illegalStateException = new IllegalStateException("Illegal state");
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(illegalStateException);
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        final ActionListener<List<Map<String, ?>>> resultListener = mock(ActionListener.class);

        setupMocksForTextEmbeddingModelAsymmetryCheck(false);

        accessor.inferenceSentencesWithMapResult(
            InferenceRequest.builder().modelId(TestCommonConstants.MODEL_ID).inputTexts(TestCommonConstants.SENTENCES_LIST).build(),
            resultListener
        );

        Mockito.verify(client, times(1))
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(resultListener).onFailure(illegalStateException);
    }

    public void testInferenceMultimodal_whenValidInput_thenSuccess() {
        final List<Float> vector = new ArrayList<>(List.of(TestCommonConstants.PREDICT_VECTOR_ARRAY));
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        setupMocksForTextEmbeddingModelAsymmetryCheck(false);

        accessor.inferenceSentencesMap(
            InferenceRequest.builder().modelId(TestCommonConstants.MODEL_ID).inputObjects(TestCommonConstants.SENTENCES_MAP).build(),
            singleSentenceResultListener
        );

        Mockito.verify(client)
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(singleSentenceResultListener).onResponse(vector);
        Mockito.verifyNoMoreInteractions(singleSentenceResultListener);
    }

    public void testInferenceMultimodal_whenExceptionFromMLClient_thenFailure() {
        final RuntimeException exception = new RuntimeException();
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(exception);
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        setupMocksForTextEmbeddingModelAsymmetryCheck(false);

        accessor.inferenceSentencesMap(
            InferenceRequest.builder().modelId(TestCommonConstants.MODEL_ID).inputObjects(TestCommonConstants.SENTENCES_MAP).build(),
            singleSentenceResultListener
        );

        Mockito.verify(client)
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(singleSentenceResultListener).onFailure(exception);
        Mockito.verifyNoMoreInteractions(singleSentenceResultListener);
    }

    public void testInferenceSentencesMapMultimodal_whenNodeNotConnectedException_thenRetryThreeTimes() {
        final NodeNotConnectedException nodeNodeConnectedException = new NodeNotConnectedException(
            mock(DiscoveryNode.class),
            "Node not connected"
        );
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(nodeNodeConnectedException);
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        setupMocksForTextEmbeddingModelAsymmetryCheck(false);

        accessor.inferenceSentencesMap(
            InferenceRequest.builder().modelId(TestCommonConstants.MODEL_ID).inputObjects(TestCommonConstants.SENTENCES_MAP).build(),
            singleSentenceResultListener
        );

        Mockito.verify(client, times(4))
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(singleSentenceResultListener).onFailure(nodeNodeConnectedException);
    }

    public void testInferenceSimilarity_whenValidInput_thenSuccess() {
        final List<Float> vector = new ArrayList<>(List.of(TestCommonConstants.PREDICT_VECTOR_ARRAY));
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createManyModelTensorOutputs(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        setupMocksForTextEmbeddingModelAsymmetryCheck(false);

        accessor.inferenceSimilarity(
            InferenceRequest.builder()
                .modelId(TestCommonConstants.MODEL_ID)
                .queryText("is it sunny")
                .inputTexts(List.of("it is sunny today", "roses are red"))
                .build(),
            singleSentenceResultListener
        );

        Mockito.verify(client)
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(singleSentenceResultListener).onResponse(vector);
        Mockito.verifyNoMoreInteractions(singleSentenceResultListener);
    }

    public void testInferencesSimilarity_whenExceptionFromMLClient_ThenFail() {
        final RuntimeException exception = new RuntimeException();
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(exception);
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        setupMocksForTextEmbeddingModelAsymmetryCheck(false);

        accessor.inferenceSimilarity(
            InferenceRequest.builder()
                .modelId(TestCommonConstants.MODEL_ID)
                .queryText("is it sunny")
                .inputTexts(List.of("it is sunny today", "roses are red"))
                .build(),
            singleSentenceResultListener
        );

        Mockito.verify(client)
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(singleSentenceResultListener).onFailure(exception);
        Mockito.verifyNoMoreInteractions(singleSentenceResultListener);
    }

    public void testInferenceSimilarity_whenNodeNotConnectedException_ThenTryThreeTimes() {
        final NodeNotConnectedException nodeNodeConnectedException = new NodeNotConnectedException(
            mock(DiscoveryNode.class),
            "Node not connected"
        );
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(nodeNodeConnectedException);
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        setupMocksForTextEmbeddingModelAsymmetryCheck(false);

        accessor.inferenceSimilarity(
            InferenceRequest.builder()
                .modelId(TestCommonConstants.MODEL_ID)
                .queryText("is it sunny")
                .inputTexts(List.of("it is sunny today", "roses are red"))
                .build(),
            singleSentenceResultListener
        );

        Mockito.verify(client, times(4))
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(singleSentenceResultListener).onFailure(nodeNodeConnectedException);
    }

    private ModelTensorOutput createModelTensorOutput(final Float[] output) {
        final List<ModelTensors> tensorsList = new ArrayList<>();
        final List<ModelTensor> mlModelTensorList = new ArrayList<>();
        final ModelTensor tensor = new ModelTensor(
            "someValue",
            output,
            new long[] { 1, 2 },
            MLResultDataType.FLOAT64,
            ByteBuffer.wrap(new byte[12]),
            "someValue",
            Map.of()
        );
        mlModelTensorList.add(tensor);
        final ModelTensors modelTensors = new ModelTensors(mlModelTensorList);
        tensorsList.add(modelTensors);
        return new ModelTensorOutput(tensorsList);
    }

    private ModelTensorOutput createModelTensorOutput(final Map<String, String> map) {
        final List<ModelTensors> tensorsList = new ArrayList<>();
        final List<ModelTensor> mlModelTensorList = new ArrayList<>();
        final ModelTensor tensor = new ModelTensor("response", null, null, null, null, null, map);
        mlModelTensorList.add(tensor);
        final ModelTensors modelTensors = new ModelTensors(mlModelTensorList);
        tensorsList.add(modelTensors);
        return new ModelTensorOutput(tensorsList);
    }

    private ModelTensorOutput createManyModelTensorOutputs(final Float[] output) {
        final List<ModelTensors> tensorsList = new ArrayList<>();
        for (Float score : output) {
            List<ModelTensor> tensorList = new ArrayList<>();
            String name = "logits";
            Number[] data = new Number[] { score };
            long[] shape = new long[] { 1 };
            MLResultDataType dataType = MLResultDataType.FLOAT32;
            MLResultDataType mlResultDataType = MLResultDataType.valueOf(dataType.name());
            ModelTensor tensor = ModelTensor.builder().name(name).data(data).shape(shape).dataType(mlResultDataType).build();
            tensorList.add(tensor);
            tensorsList.add(new ModelTensors(tensorList));
        }
        ModelTensorOutput modelTensorOutput = new ModelTensorOutput(tensorsList);
        return modelTensorOutput;
    }
}
