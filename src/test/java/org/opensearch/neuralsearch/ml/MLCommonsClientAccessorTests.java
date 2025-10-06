/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.neuralsearch.ml.dto.AgentInfoDTO;
import org.opensearch.neuralsearch.ml.dto.AgentExecutionDTO;
import org.opensearch.neuralsearch.query.AgenticSearchQueryBuilder;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.input.execute.agent.AgentMLInput;
import org.opensearch.ml.common.input.parameter.MLAlgoParams;
import org.opensearch.ml.common.output.MLOutput;
import org.opensearch.ml.common.output.model.MLResultDataType;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.transport.execute.MLExecuteTaskResponse;
import org.opensearch.neuralsearch.constants.TestCommonConstants;
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.NodeNotConnectedException;

public class MLCommonsClientAccessorTests extends OpenSearchTestCase {

    @Mock
    private ActionListener<List<List<Number>>> resultListener;

    @Mock
    private ActionListener<List<Number>> singleSentenceResultListener;

    @Mock
    private ActionListener<List<Float>> similarityResultListener;

    @Mock
    private MachineLearningNodeClient client;

    @InjectMocks
    private MLCommonsClientAccessor accessor;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    public void testInferenceSentence_whenValidInput_thenSuccess() {
        final List<Number> vector = new ArrayList<>(List.of(TestCommonConstants.PREDICT_VECTOR_ARRAY));
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.inferenceSentence(TestCommonConstants.MODEL_ID, TestCommonConstants.SENTENCES_LIST.get(0), singleSentenceResultListener);

        verify(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(singleSentenceResultListener).onResponse(vector);
        Mockito.verifyNoMoreInteractions(singleSentenceResultListener);
    }

    public void testInferenceSentences_whenValidInputThenSuccess() {
        final List<List<Number>> vectorList = new ArrayList<>();
        vectorList.add(Arrays.asList(TestCommonConstants.PREDICT_VECTOR_ARRAY));
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentences(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

        verify(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(resultListener).onResponse(vectorList);
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSentences_whenResultFromClient_thenEmptyVectorList() {
        final List<List<Number>> vectorList = new ArrayList<>();
        vectorList.add(Collections.emptyList());
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(new Float[] {}));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentences(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

        verify(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(resultListener).onResponse(vectorList);
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSentences_whenExceptionFromMLClient_thenFailure() {
        final RuntimeException exception = new RuntimeException();
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(exception);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentences(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

        verify(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(resultListener).onFailure(exception);
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSimilarity_whenNodeNotConnectedException_ThenRetry() {
        final NodeNotConnectedException nodeNodeConnectedException = new NodeNotConnectedException(
            mock(DiscoveryNode.class),
            "Node not connected"
        );

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(nodeNodeConnectedException);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.inferenceSimilarity(TestCommonConstants.SIMILARITY_INFERENCE_REQUEST, similarityResultListener);

        // Verify client.predict is called 4 times (1 initial + 3 retries)
        verify(client, times(4)).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        // Verify failure is propagated to the listener after all retries
        verify(similarityResultListener).onFailure(nodeNodeConnectedException);

        // Ensure no additional interactions with the listener
        Mockito.verifyNoMoreInteractions(similarityResultListener);
    }

    public void testInferenceSentences_whenExceptionFromMLClient_thenRetry_thenFailure() {
        final NodeNotConnectedException nodeNodeConnectedException = new NodeNotConnectedException(
            mock(DiscoveryNode.class),
            "Node not connected"
        );
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(nodeNodeConnectedException);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentences(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

        verify(client, times(4)).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(resultListener).onFailure(nodeNodeConnectedException);
    }

    public void testInferenceSentences_whenNotConnectionException_thenNoRetry() {
        final IllegalStateException illegalStateException = new IllegalStateException("Illegal state");
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(illegalStateException);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentences(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

        verify(client, times(1)).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(resultListener).onFailure(illegalStateException);
    }

    public void testInferenceSentencesWithMapResult_whenValidInput_thenSuccess() {
        final Map<String, Object> map = Map.of("key", "value");
        final ActionListener<List<Map<String, ?>>> resultListener = mock(ActionListener.class);
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(map));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.TEXT_INFERENCE_REQUEST, null, resultListener);

        verify(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(resultListener).onResponse(List.of(map));
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSentencesWithMapResult_whenValidInput_withParameter() {
        final Map<String, Object> map = Map.of("key", "value");
        final ActionListener<List<Map<String, ?>>> resultListener = mock(ActionListener.class);
        MLAlgoParams algoParameter = mock(MLAlgoParams.class);
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(map));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.TEXT_INFERENCE_REQUEST, algoParameter, resultListener);

        ArgumentCaptor<MLInput> mlInputCaptor = ArgumentCaptor.forClass(MLInput.class);
        verify(client).predict(eq(TestCommonConstants.MODEL_ID), mlInputCaptor.capture(), Mockito.isA(ActionListener.class));
        assertSame(algoParameter, mlInputCaptor.getValue().getParameters());
        verify(resultListener).onResponse(List.of(map));
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSentencesWithMapResult_whenTensorOutputListEmpty_thenException() {
        final ActionListener<List<Map<String, ?>>> resultListener = mock(ActionListener.class);
        final ModelTensorOutput modelTensorOutput = new ModelTensorOutput(Collections.emptyList());
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(modelTensorOutput);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.TEXT_INFERENCE_REQUEST, null, resultListener);

        verify(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(IllegalStateException.class);
        verify(resultListener).onFailure(argumentCaptor.capture());
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
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.TEXT_INFERENCE_REQUEST, null, resultListener);

        verify(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(IllegalStateException.class);
        verify(resultListener).onFailure(argumentCaptor.capture());
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
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.TEXT_INFERENCE_REQUEST, null, resultListener);

        verify(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(resultListener).onResponse(List.of(Map.of("key", "value"), Map.of("key", "value")));
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
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        final ActionListener<List<Map<String, ?>>> resultListener = mock(ActionListener.class);
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.TEXT_INFERENCE_REQUEST, null, resultListener);

        verify(client, times(4)).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(resultListener).onFailure(nodeNodeConnectedException);
    }

    public void testInferenceSentencesWithMapResult_whenNotRetryableException_thenFail() {
        final IllegalStateException illegalStateException = new IllegalStateException("Illegal state");
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(illegalStateException);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        final ActionListener<List<Map<String, ?>>> resultListener = mock(ActionListener.class);
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.TEXT_INFERENCE_REQUEST, null, resultListener);

        verify(client, times(1)).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(resultListener).onFailure(illegalStateException);
    }

    public void testInferenceMultimodal_whenValidInput_thenSuccess() {
        final List<Number> vector = new ArrayList<>(List.of(TestCommonConstants.PREDICT_VECTOR_ARRAY));
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.inferenceSentencesMap(TestCommonConstants.MAP_INFERENCE_REQUEST, singleSentenceResultListener);

        verify(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(singleSentenceResultListener).onResponse(vector);
        Mockito.verifyNoMoreInteractions(singleSentenceResultListener);
    }

    public void testInferenceMultimodal_whenExceptionFromMLClient_thenRetry_thenFailure() {
        final NodeNotConnectedException nodeNodeConnectedException = new NodeNotConnectedException(
            mock(DiscoveryNode.class),
            "Node not connected"
        );

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(nodeNodeConnectedException);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.inferenceSentencesMap(TestCommonConstants.MAP_INFERENCE_REQUEST, singleSentenceResultListener);

        // Verify client.predict is called 4 times (1 initial + 3 retries)
        verify(client, times(4)).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        // Verify failure is propagated to the listener after retries
        verify(singleSentenceResultListener).onFailure(nodeNodeConnectedException);

        // Verify no further interactions with the listener
        Mockito.verifyNoMoreInteractions(singleSentenceResultListener);
    }

    public void testInferenceSentencesMultimodal_whenNodeNotConnectedException_thenRetryThreeTimes() {
        final NodeNotConnectedException nodeNodeConnectedException = new NodeNotConnectedException(
            mock(DiscoveryNode.class),
            "Node not connected"
        );
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(nodeNodeConnectedException);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentencesMap(TestCommonConstants.MAP_INFERENCE_REQUEST, singleSentenceResultListener);

        verify(client, times(4)).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(singleSentenceResultListener).onFailure(nodeNodeConnectedException);
    }

    public void testInferenceSimilarity_whenValidInput_thenSuccess() {
        final List<Float> vector = new ArrayList<>(List.of(TestCommonConstants.PREDICT_VECTOR_ARRAY));
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createManyModelTensorOutputs(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.inferenceSimilarity(TestCommonConstants.SIMILARITY_INFERENCE_REQUEST, similarityResultListener);

        verify(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(similarityResultListener).onResponse(vector);
        Mockito.verifyNoMoreInteractions(similarityResultListener);
    }

    public void testInferencesSimilarity_whenExceptionFromMLClient_ThenFail() {
        final RuntimeException exception = new RuntimeException();
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(exception);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.inferenceSimilarity(TestCommonConstants.SIMILARITY_INFERENCE_REQUEST, similarityResultListener);

        verify(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(similarityResultListener).onFailure(exception);
        Mockito.verifyNoMoreInteractions(similarityResultListener);
    }

    /**
     * Tests successful sentence highlighting inference with valid input.
     */
    public void testInferenceSentenceHighlighting_whenValidInput_thenSuccess() {
        final ActionListener<List<Map<String, Object>>> resultListener = mock(ActionListener.class);
        final Map<String, Object> highlights = Map.of(
            "highlights",
            List.of(
                Map.of(
                    "start",
                    0.0,
                    "end",
                    67.0,
                    "text",
                    "Global temperatures have risen significantly over the past century.",
                    "position",
                    0.0
                ),
                Map.of("start", 68.0, "end", 115.0, "text", "Polar ice caps are melting at an alarming rate.", "position", 1.0)
            )
        );

        // Mock getModel to return REMOTE function
        MLModel mockModel = mock(MLModel.class);
        when(mockModel.getAlgorithm()).thenReturn(FunctionName.REMOTE);
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(mockModel);
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(highlights));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.inferenceSentenceHighlighting(
            SentenceHighlightingRequest.builder()
                .modelId(TestCommonConstants.MODEL_ID)
                .question("What are the impacts of climate change?")
                .context(
                    "Global temperatures have risen significantly over the past century. Polar ice caps are melting at an alarming rate."
                )
                .build(),
            FunctionName.QUESTION_ANSWERING,
            resultListener
        );

        Mockito.verify(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(resultListener).onResponse(List.of(highlights));
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    /**
     * Tests sentence highlighting inference when no highlights are found.
     */
    public void testInferenceSentenceHighlighting_whenEmptyHighlights_thenReturnEmptyList() {
        final ActionListener<List<Map<String, Object>>> resultListener = mock(ActionListener.class);
        final Map<String, Object> emptyHighlights = Map.of("highlights", Collections.emptyList());

        // Mock getModel to return REMOTE function
        MLModel mockModel = mock(MLModel.class);
        when(mockModel.getAlgorithm()).thenReturn(FunctionName.REMOTE);
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(mockModel);
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(emptyHighlights));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.inferenceSentenceHighlighting(
            SentenceHighlightingRequest.builder()
                .modelId(TestCommonConstants.MODEL_ID)
                .question("test question")
                .context("test context")
                .build(),
            FunctionName.QUESTION_ANSWERING,
            resultListener
        );

        Mockito.verify(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(resultListener).onResponse(List.of(emptyHighlights));
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    /**
     * Tests sentence highlighting inference retry behavior on connection issues.
     */
    public void testInferenceSentenceHighlighting_whenNodeNotConnectedException_thenRetry() {
        final ActionListener<List<Map<String, Object>>> resultListener = mock(ActionListener.class);
        final NodeNotConnectedException nodeNotConnectedException = new NodeNotConnectedException(
            mock(DiscoveryNode.class),
            "Node not connected"
        );

        // Mock getModel to return REMOTE function (will be called 4 times due to retries)
        MLModel mockModel = mock(MLModel.class);
        when(mockModel.getAlgorithm()).thenReturn(FunctionName.REMOTE);
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(mockModel);
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(nodeNotConnectedException);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.inferenceSentenceHighlighting(
            SentenceHighlightingRequest.builder()
                .modelId(TestCommonConstants.MODEL_ID)
                .question("test question")
                .context("test context")
                .build(),
            FunctionName.QUESTION_ANSWERING,
            resultListener
        );

        // Verify client.predict is called 4 times (1 initial + 3 retries)
        Mockito.verify(client, times(4))
            .predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(resultListener).onFailure(nodeNotConnectedException);
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    /**
     * Tests sentence highlighting inference failure with non-retryable exceptions.
     */
    public void testInferenceSentenceHighlighting_whenNotRetryableException_thenFail() {
        final ActionListener<List<Map<String, Object>>> resultListener = mock(ActionListener.class);
        final IllegalStateException illegalStateException = new IllegalStateException("Illegal state");

        // Mock getModel to return REMOTE function
        MLModel mockModel = mock(MLModel.class);
        when(mockModel.getAlgorithm()).thenReturn(FunctionName.REMOTE);
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(mockModel);
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(illegalStateException);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.inferenceSentenceHighlighting(
            SentenceHighlightingRequest.builder()
                .modelId(TestCommonConstants.MODEL_ID)
                .question("test question")
                .context("test context")
                .build(),
            FunctionName.QUESTION_ANSWERING,
            resultListener
        );

        Mockito.verify(client, times(1))
            .predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(resultListener).onFailure(illegalStateException);
        Mockito.verifyNoMoreInteractions(resultListener);
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

    private ModelTensorOutput createModelTensorOutput(final Map<String, Object> map) {
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

    public void testGetModel_Success() {
        final String modelId = "someModelId";
        final ActionListener<MLModel> listener = mock(ActionListener.class);

        accessor.getModel(modelId, listener);

        verify(client).getModel(eq(modelId), eq(null), any(ActionListener.class));
    }

    public void testGetModels_Success() {
        final String modelId1 = "dummyModel1";
        final String modelId2 = "dummyModel2";
        final Set<String> modelIds = Set.of(modelId1, modelId2);
        final MLModel mlModel1 = mock(MLModel.class);
        final MLModel mlModel2 = mock(MLModel.class);
        final Consumer<Map<String, MLModel>> onSuccess = mock(Consumer.class);
        final Consumer<Exception> onFailure = mock(Consumer.class);

        Mockito.doAnswer(invocation -> {
            final String modelId = (String) invocation.getArgument(0);
            final ActionListener<MLModel> listener = invocation.getArgument(2);
            if (modelId.equals(modelId1)) {
                listener.onResponse(mlModel1);
            } else if (modelId.equals(modelId2)) {
                listener.onResponse(mlModel2);
            }
            return null;
        }).when(client).getModel(any(), any(), any());

        accessor.getModels(modelIds, onSuccess, onFailure);

        // Capture the exception passed to onFailure
        final ArgumentCaptor resultCaptor = ArgumentCaptor.forClass(Map.class);
        verify(onSuccess, times(1)).accept((Map<String, MLModel>) resultCaptor.capture());

        final Map<String, MLModel> expectedResultMap = Map.of(modelId1, mlModel1, modelId2, mlModel2);
        assertEquals(expectedResultMap, resultCaptor.getValue());
    }

    public void testGetModels_Failure() {
        final String modelId1 = "dummyModel1";
        final String modelId2 = "dummyModel2";
        final Set<String> modelIds = Set.of(modelId1, modelId2);
        final MLModel mlModel1 = mock(MLModel.class);
        final Consumer<Map<String, MLModel>> onSuccess = mock(Consumer.class);
        final Consumer<Exception> onFailure = mock(Consumer.class);

        Mockito.doAnswer(invocation -> {
            final String modelId = (String) invocation.getArgument(0);
            final ActionListener<MLModel> listener = invocation.getArgument(2);
            if (modelId.equals(modelId1)) {
                listener.onResponse(mlModel1);
            } else if (modelId.equals(modelId2)) {
                listener.onFailure(new ResourceNotFoundException("dummyModel2 not found"));
            }
            return null;
        }).when(client).getModel(any(), any(), any());

        accessor.getModels(modelIds, onSuccess, onFailure);

        // Capture the exception passed to onFailure
        final ArgumentCaptor<Exception> resultCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(onFailure, times(1)).accept(resultCaptor.capture());

        final String expectedMessage = "Failed to fetch model [dummyModel2]: dummyModel2 not found";
        assertEquals(expectedMessage, resultCaptor.getValue().getMessage());
    }

    public void testExecuteAgent_Success() throws Exception {
        final String agentId = "test-agent-id";
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);
        final String expectedDslQuery = "{\"query\":{\"match\":{\"field\":\"value\"}}}";

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "test-index" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("test query");
        when(mockQuery.getQueryFields()).thenReturn(List.of("field1", "field2"));

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(2);
            MLExecuteTaskResponse mockResponse = mock(MLExecuteTaskResponse.class);
            when(mockResponse.getOutput()).thenReturn(createConversationalAgentResponse());
            actionListener.onResponse(mockResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false);

        accessor.executeAgent(
            mockRequest,
            mockQuery,
            agentId,
            agentInfo,
            mock(org.opensearch.core.xcontent.NamedXContentRegistry.class),
            listener
        );

        verify(client).execute(any(), any(), any());
        ArgumentCaptor<AgentExecutionDTO> resultCaptor = ArgumentCaptor.forClass(AgentExecutionDTO.class);
        verify(listener).onResponse(resultCaptor.capture());

        AgentExecutionDTO result = resultCaptor.getValue();
        assertEquals(expectedDslQuery, result.getDslQuery());
        assertEquals("test summary", result.getAgentStepsSummary());
        assertEquals("test-memory-123", result.getMemoryId());
        Mockito.verifyNoMoreInteractions(listener);
    }

    public void testExecuteAgent_Failure() throws Exception {
        final String agentId = "test-agent-id";
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);
        final RuntimeException exception = new RuntimeException("Agent execution failed");

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "test-index" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("test query");

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(2);
            actionListener.onFailure(exception);
            return null;
        }).when(client).execute(any(), any(), any());

        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false);

        accessor.executeAgent(
            mockRequest,
            mockQuery,
            agentId,
            agentInfo,
            mock(org.opensearch.core.xcontent.NamedXContentRegistry.class),
            listener
        );

        verify(client).execute(any(), any(), any());
        verify(listener).onFailure(exception);
        Mockito.verifyNoMoreInteractions(listener);
    }

    public void testExecuteAgent_WithRetry() throws Exception {
        final String agentId = "test-agent-id";
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);
        final NodeNotConnectedException nodeNotConnectedException = new NodeNotConnectedException(
            mock(DiscoveryNode.class),
            "Node not connected"
        );

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "test-index" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("test query");

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(2);
            actionListener.onFailure(nodeNotConnectedException);
            return null;
        }).when(client).execute(any(), any(), any());

        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false);

        accessor.executeAgent(
            mockRequest,
            mockQuery,
            agentId,
            agentInfo,
            mock(org.opensearch.core.xcontent.NamedXContentRegistry.class),
            listener
        );

        verify(client, times(4)).execute(any(), any(), any());
        verify(listener).onFailure(nodeNotConnectedException);
        Mockito.verifyNoMoreInteractions(listener);
    }

    private ModelTensorOutput createModelTensorOutputWithResult(String result) {
        final List<ModelTensors> tensorsList = new ArrayList<>();
        final List<ModelTensor> mlModelTensorList = new ArrayList<>();
        // For flow agents, use the result directly
        final ModelTensor tensor = new ModelTensor("response", null, null, null, null, result, Map.of());
        mlModelTensorList.add(tensor);
        final ModelTensors modelTensors = new ModelTensors(mlModelTensorList);
        tensorsList.add(modelTensors);
        return new ModelTensorOutput(tensorsList);
    }

    public void testGetAgentDetails_Success() {
        final String agentId = "test-agent-id";
        final ActionListener<AgentInfoDTO> listener = mock(ActionListener.class);

        // Mock ML agent with type and system prompt
        org.opensearch.ml.common.agent.MLAgent mockMLAgent = mock(org.opensearch.ml.common.agent.MLAgent.class);
        org.opensearch.ml.common.transport.agent.MLAgentGetResponse mockResponse = mock(
            org.opensearch.ml.common.transport.agent.MLAgentGetResponse.class
        );
        org.opensearch.ml.common.agent.LLMSpec mockLLM = mock(org.opensearch.ml.common.agent.LLMSpec.class);

        when(mockResponse.getMlAgent()).thenReturn(mockMLAgent);
        when(mockMLAgent.getType()).thenReturn("conversational");
        when(mockMLAgent.getLlm()).thenReturn(mockLLM);

        Map<String, String> parameters = new HashMap<>();
        parameters.put("system_prompt", "test prompt");
        when(mockLLM.getParameters()).thenReturn(parameters);

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(1);
            actionListener.onResponse(mockResponse);
            return null;
        }).when(client).getAgent(eq(agentId), any(ActionListener.class));

        accessor.getAgentDetails(agentId, listener);

        ArgumentCaptor<AgentInfoDTO> resultCaptor = ArgumentCaptor.forClass(AgentInfoDTO.class);
        verify(listener).onResponse(resultCaptor.capture());

        AgentInfoDTO result = resultCaptor.getValue();
        assertEquals("conversational", result.getType());
        assertEquals(true, result.isHasSystemPrompt());
    }

    public void testGetAgentDetails_NoSystemPrompt() {
        final String agentId = "test-agent-id";
        final ActionListener<AgentInfoDTO> listener = mock(ActionListener.class);

        // Mock ML agent without system prompt
        org.opensearch.ml.common.agent.MLAgent mockMLAgent = mock(org.opensearch.ml.common.agent.MLAgent.class);
        org.opensearch.ml.common.transport.agent.MLAgentGetResponse mockResponse = mock(
            org.opensearch.ml.common.transport.agent.MLAgentGetResponse.class
        );
        org.opensearch.ml.common.agent.LLMSpec mockLLM = mock(org.opensearch.ml.common.agent.LLMSpec.class);

        when(mockResponse.getMlAgent()).thenReturn(mockMLAgent);
        when(mockMLAgent.getType()).thenReturn("flow");
        when(mockMLAgent.getLlm()).thenReturn(mockLLM);
        when(mockLLM.getParameters()).thenReturn(new HashMap<>());

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(1);
            actionListener.onResponse(mockResponse);
            return null;
        }).when(client).getAgent(eq(agentId), any(ActionListener.class));

        accessor.getAgentDetails(agentId, listener);

        ArgumentCaptor<AgentInfoDTO> resultCaptor = ArgumentCaptor.forClass(AgentInfoDTO.class);
        verify(listener).onResponse(resultCaptor.capture());

        AgentInfoDTO result = resultCaptor.getValue();
        assertEquals("flow", result.getType());
        assertEquals(false, result.isHasSystemPrompt());
    }

    public void testGetAgentDetails_NullAgent() {
        final String agentId = "test-agent-id";
        final ActionListener<AgentInfoDTO> listener = mock(ActionListener.class);

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(1);
            actionListener.onResponse(null);
            return null;
        }).when(client).getAgent(eq(agentId), any(ActionListener.class));

        accessor.getAgentDetails(agentId, listener);

        verify(listener).onFailure(any(IllegalStateException.class));
    }

    public void testExecuteAgent_FlowAgent() throws Exception {
        final String agentId = "test-agent-id";
        final String agentType = "flow";
        final boolean hasSystemPrompt = false;
        final NamedXContentRegistry registry = mock(NamedXContentRegistry.class);
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);
        final String expectedResponse = "{\"query\": {\"match\": {\"field\": \"value\"}}}";

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "test-index" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("test query");

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(2);
            MLExecuteTaskResponse mockResponse = mock(MLExecuteTaskResponse.class);
            when(mockResponse.getOutput()).thenReturn(createModelTensorOutputWithResult(expectedResponse));
            actionListener.onResponse(mockResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        AgentInfoDTO agentInfo = new AgentInfoDTO(agentType, hasSystemPrompt, false);

        accessor.executeAgent(mockRequest, mockQuery, agentId, agentInfo, registry, listener);

        verify(client).execute(any(), any(), any());
        ArgumentCaptor<AgentExecutionDTO> resultCaptor = ArgumentCaptor.forClass(AgentExecutionDTO.class);
        verify(listener).onResponse(resultCaptor.capture());

        AgentExecutionDTO result = resultCaptor.getValue();
        assertEquals(expectedResponse, result.getDslQuery());
        assertNull(result.getAgentStepsSummary());
        assertNull(result.getMemoryId());
    }

    public void testExecuteAgent_ConversationalAgent() throws Exception {
        final String agentId = "test-agent-id";
        final String agentType = "conversational";
        final boolean hasSystemPrompt = true;
        final NamedXContentRegistry registry = mock(NamedXContentRegistry.class);
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "test-index" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("test query");

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(2);
            MLExecuteTaskResponse mockResponse = mock(MLExecuteTaskResponse.class);
            when(mockResponse.getOutput()).thenReturn(createConversationalAgentResponse());
            actionListener.onResponse(mockResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        AgentInfoDTO agentInfo = new AgentInfoDTO(agentType, hasSystemPrompt, false);

        accessor.executeAgent(mockRequest, mockQuery, agentId, agentInfo, registry, listener);

        verify(client).execute(any(), any(), any());
        ArgumentCaptor<AgentExecutionDTO> resultCaptor = ArgumentCaptor.forClass(AgentExecutionDTO.class);
        verify(listener).onResponse(resultCaptor.capture());

        AgentExecutionDTO result = resultCaptor.getValue();
        assertEquals("{\"query\":{\"match\":{\"field\":\"value\"}}}", result.getDslQuery());
        assertEquals("test summary", result.getAgentStepsSummary());
        assertEquals("test-memory-123", result.getMemoryId());
    }

    public void testExecuteAgent_WithSystemPromptFalse() throws Exception {
        final String agentId = "test-agent-id";
        final String agentType = "conversational";
        final boolean hasSystemPrompt = false;
        final NamedXContentRegistry registry = mock(NamedXContentRegistry.class);
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "test-index" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("test query");

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(2);
            MLExecuteTaskResponse mockResponse = mock(MLExecuteTaskResponse.class);
            when(mockResponse.getOutput()).thenReturn(createConversationalAgentResponse());
            actionListener.onResponse(mockResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        AgentInfoDTO agentInfo = new AgentInfoDTO(agentType, hasSystemPrompt, false);

        accessor.executeAgent(mockRequest, mockQuery, agentId, agentInfo, registry, listener);

        ArgumentCaptor<AgentMLInput> inputCaptor = ArgumentCaptor.forClass(AgentMLInput.class);
        verify(client).execute(any(), inputCaptor.capture(), any());

        AgentMLInput capturedInput = inputCaptor.getValue();
        RemoteInferenceInputDataSet dataset = (RemoteInferenceInputDataSet) capturedInput.getInputDataset();
        assertTrue(dataset.getParameters().containsKey("system_prompt"));

        ArgumentCaptor<AgentExecutionDTO> resultCaptor = ArgumentCaptor.forClass(AgentExecutionDTO.class);
        verify(listener).onResponse(resultCaptor.capture());

        AgentExecutionDTO result = resultCaptor.getValue();
        assertEquals("{\"query\":{\"match\":{\"field\":\"value\"}}}", result.getDslQuery());
    }

    public void testExecuteAgent_UnsupportedAgentType() throws Exception {
        final String agentId = "test-agent-id";
        final String agentType = "unsupported";
        final boolean hasSystemPrompt = false;
        final NamedXContentRegistry registry = mock(NamedXContentRegistry.class);
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "test-index" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("test query");

        AgentInfoDTO agentInfo = new AgentInfoDTO(agentType, hasSystemPrompt, false);

        accessor.executeAgent(mockRequest, mockQuery, agentId, agentInfo, registry, listener);

        ArgumentCaptor<IllegalArgumentException> exceptionCaptor = ArgumentCaptor.forClass(IllegalArgumentException.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertEquals("Unsupported agent type: unsupported", exceptionCaptor.getValue().getMessage());
    }

    public void testExecuteAgent_FlowAgentWithMultipleIndices() throws Exception {
        final String agentId = "test-agent-id";
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "index1", "index2" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("test query");

        AgentInfoDTO agentInfo = new AgentInfoDTO("flow", false, false);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> accessor.executeAgent(mockRequest, mockQuery, agentId, agentInfo, mock(NamedXContentRegistry.class), listener)
        );

        assertEquals("Flow agent does not support multiple indices", exception.getMessage());
    }

    public void testExecuteAgent_FlowAgentWithMemoryId() throws Exception {
        final String agentId = "test-agent-id";
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "test-index" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("test query");
        when(mockQuery.getMemoryId()).thenReturn("test-memory-123");

        AgentInfoDTO agentInfo = new AgentInfoDTO("flow", false, false);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> accessor.executeAgent(mockRequest, mockQuery, agentId, agentInfo, mock(NamedXContentRegistry.class), listener)
        );

        assertEquals("Flow agent does not support memory_id", exception.getMessage());
    }

    private ModelTensorOutput createConversationalAgentResponse() {
        final List<ModelTensors> tensorsList = new ArrayList<>();
        final List<ModelTensor> mlModelTensorList = new ArrayList<>();

        // Create proper conversational agent response format
        String responseJson = "{\"dsl_query\":{\"query\":{\"match\":{\"field\":\"value\"}}},\"agent_steps_summary\":\"test summary\"}";
        final ModelTensor responseTensor = new ModelTensor("response", null, null, null, null, null, Map.of("response", responseJson));
        final ModelTensor memoryTensor = new ModelTensor("memory_id", null, null, null, null, "test-memory-123", Map.of());

        mlModelTensorList.add(responseTensor);
        mlModelTensorList.add(memoryTensor);
        final ModelTensors modelTensors = new ModelTensors(mlModelTensorList);
        tensorsList.add(modelTensors);
        return new ModelTensorOutput(tensorsList);
    }

    /**
     * Tests successful batch sentence highlighting inference with valid input.
     */
    public void testBatchInferenceSentenceHighlighting_whenValidInput_thenSuccess() {
        final ActionListener<List<List<Map<String, Object>>>> batchResultListener = mock(ActionListener.class);

        List<SentenceHighlightingRequest> requests = List.of(
            SentenceHighlightingRequest.builder()
                .modelId("test-model")
                .question("What is AI?")
                .context("Artificial Intelligence is a field of computer science.")
                .build(),
            SentenceHighlightingRequest.builder()
                .modelId("test-model")
                .question("What is ML?")
                .context("Machine Learning is a subset of AI.")
                .build()
        );

        // Create batch response with highlights for both documents
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put(
            "highlights",
            List.of(
                List.of(Map.of("start", 0, "end", 22), Map.of("start", 26, "end", 31)),
                List.of(Map.of("start", 0, "end", 16), Map.of("start", 20, "end", 25))
            )
        );

        ModelTensor tensor = new ModelTensor("response", null, null, null, null, null, dataMap);
        ModelTensors tensors = new ModelTensors(List.of(tensor));
        ModelTensorOutput mlOutput = new ModelTensorOutput(List.of(tensors));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(mlOutput);
            return null;
        }).when(client).predict(eq("test-model"), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.batchInferenceSentenceHighlighting("test-model", requests, FunctionName.REMOTE, batchResultListener);

        ArgumentCaptor<List<List<Map<String, Object>>>> resultCaptor = ArgumentCaptor.forClass(List.class);
        verify(batchResultListener).onResponse(resultCaptor.capture());

        List<List<Map<String, Object>>> result = resultCaptor.getValue();
        assertEquals(2, result.size());
        assertEquals(2, result.get(0).size()); // First doc has 2 highlights
        assertEquals(2, result.get(1).size()); // Second doc has 2 highlights
    }

    /**
     * Tests batch sentence highlighting with mixed results (some with highlights, some without).
     */
    public void testBatchInferenceSentenceHighlighting_whenMixedResults_thenHandleCorrectly() {
        final ActionListener<List<List<Map<String, Object>>>> batchResultListener = mock(ActionListener.class);

        List<SentenceHighlightingRequest> requests = List.of(
            SentenceHighlightingRequest.builder().modelId("test-model").question("Q1").context("C1").build(),
            SentenceHighlightingRequest.builder().modelId("test-model").question("Q2").context("C2").build()
        );

        // Create response with highlights for first doc, empty for second
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("highlights", List.of(List.of(Map.of("start", 0, "end", 10)), Collections.emptyList()));

        ModelTensor tensor = new ModelTensor("response", null, null, null, null, null, dataMap);
        ModelTensors tensors = new ModelTensors(List.of(tensor));
        ModelTensorOutput mlOutput = new ModelTensorOutput(List.of(tensors));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(mlOutput);
            return null;
        }).when(client).predict(eq("test-model"), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.batchInferenceSentenceHighlighting("test-model", requests, FunctionName.REMOTE, batchResultListener);

        ArgumentCaptor<List<List<Map<String, Object>>>> resultCaptor = ArgumentCaptor.forClass(List.class);
        verify(batchResultListener).onResponse(resultCaptor.capture());

        List<List<Map<String, Object>>> result = resultCaptor.getValue();
        assertEquals(2, result.size());
        assertEquals(1, result.get(0).size()); // First doc has 1 highlight
        assertEquals(0, result.get(1).size()); // Second doc has no highlights
    }

    /**
     * Tests batch sentence highlighting with malformed response.
     */
    public void testBatchInferenceSentenceHighlighting_whenMalformedResponse_thenHandleGracefully() {
        final ActionListener<List<List<Map<String, Object>>>> batchResultListener = mock(ActionListener.class);

        List<SentenceHighlightingRequest> requests = List.of(
            SentenceHighlightingRequest.builder().modelId("test-model").question("Q1").context("C1").build()
        );

        // Create malformed response without highlights key
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("wrong_key", "wrong_value");

        ModelTensor tensor = new ModelTensor("response", null, null, null, null, null, dataMap);
        ModelTensors tensors = new ModelTensors(List.of(tensor));
        ModelTensorOutput mlOutput = new ModelTensorOutput(List.of(tensors));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(mlOutput);
            return null;
        }).when(client).predict(eq("test-model"), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.batchInferenceSentenceHighlighting("test-model", requests, FunctionName.REMOTE, batchResultListener);

        ArgumentCaptor<List<List<Map<String, Object>>>> resultCaptor = ArgumentCaptor.forClass(List.class);
        verify(batchResultListener).onResponse(resultCaptor.capture());

        List<List<Map<String, Object>>> result = resultCaptor.getValue();
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).size()); // Should return empty list for malformed response
    }

    /**
     * Tests batch sentence highlighting with failure handling.
     */
    public void testBatchInferenceSentenceHighlighting_whenException_thenPropagateFailure() {
        final ActionListener<List<List<Map<String, Object>>>> batchResultListener = mock(ActionListener.class);
        final RuntimeException exception = new RuntimeException("Batch inference failed");

        List<SentenceHighlightingRequest> requests = List.of(
            SentenceHighlightingRequest.builder().modelId("test-model").question("Q1").context("C1").build()
        );

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(exception);
            return null;
        }).when(client).predict(eq("test-model"), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.batchInferenceSentenceHighlighting("test-model", requests, FunctionName.REMOTE, batchResultListener);

        verify(batchResultListener).onFailure(exception);
    }

    public void testBatchInferenceSentenceHighlighting_whenNonRemoteModel_thenFailure() {
        final ActionListener<List<List<Map<String, Object>>>> batchResultListener = mock(ActionListener.class);

        List<SentenceHighlightingRequest> requests = List.of(
            SentenceHighlightingRequest.builder().modelId("local-model").question("Q1").context("C1").build()
        );

        // Test with QUESTION_ANSWERING model
        accessor.batchInferenceSentenceHighlighting("local-model", requests, FunctionName.QUESTION_ANSWERING, batchResultListener);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(batchResultListener).onFailure(exceptionCaptor.capture());

        Exception capturedError = exceptionCaptor.getValue();
        assertTrue(capturedError instanceof IllegalArgumentException);
        assertTrue(capturedError.getMessage().contains("does not support batch inference"));
        assertTrue(capturedError.getMessage().contains("QUESTION_ANSWERING"));

        // Test with TEXT_EMBEDDING model
        final ActionListener<List<List<Map<String, Object>>>> embeddingListener = mock(ActionListener.class);
        accessor.batchInferenceSentenceHighlighting("embed-model", requests, FunctionName.TEXT_EMBEDDING, embeddingListener);

        ArgumentCaptor<Exception> embeddingExceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(embeddingListener).onFailure(embeddingExceptionCaptor.capture());

        Exception embeddingError = embeddingExceptionCaptor.getValue();
        assertTrue(embeddingError instanceof IllegalArgumentException);
        assertTrue(embeddingError.getMessage().contains("does not support batch inference"));
        assertTrue(embeddingError.getMessage().contains("TEXT_EMBEDDING"));
    }

    public void testInferenceSentenceHighlighting_withDifferentModelTypes() {
        final ActionListener<List<Map<String, Object>>> resultListener = mock(ActionListener.class);
        final ActionListener<List<Map<String, Object>>> remoteListener = mock(ActionListener.class);

        // Mock ML client to return an empty result
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            ModelTensor tensor = ModelTensor.builder().dataAsMap(Map.of()).build();
            ModelTensorOutput output = ModelTensorOutput.builder()
                .mlModelOutputs(List.of(ModelTensors.builder().mlModelTensors(List.of(tensor)).build()))
                .build();
            actionListener.onResponse(output);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        // Test with QUESTION_ANSWERING model (should work)
        accessor.inferenceSentenceHighlighting(
            SentenceHighlightingRequest.builder()
                .modelId(TestCommonConstants.MODEL_ID)
                .question("test question")
                .context("test context")
                .build(),
            FunctionName.QUESTION_ANSWERING,
            resultListener
        );

        // Verify that the method was called and completed without error
        verify(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(resultListener).onResponse(anyList());

        // Test with REMOTE model (should also work)
        accessor.inferenceSentenceHighlighting(
            SentenceHighlightingRequest.builder()
                .modelId(TestCommonConstants.MODEL_ID)
                .question("test question")
                .context("test context")
                .build(),
            FunctionName.REMOTE,
            remoteListener
        );

        // Verify that it was called without error
        verify(remoteListener).onResponse(anyList());
    }

    public void testExecuteAgent_ConversationalAgentWithTrailingZeros() throws Exception {
        final String agentId = "test-agent-id";
        final String agentType = "conversational";
        final boolean hasSystemPrompt = true;
        final NamedXContentRegistry registry = mock(NamedXContentRegistry.class);
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "test-index" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("test query");

        // Create conversational agent response with trailing decimal zeros that should be normalized
        String responseJson =
            "{\"dsl_query\":{\"size\": 12.00, \"track_total_hits\": 7.0, \"terminate_after\": 1.0e5, \"query\":{\"match\":{\"message\":\"ip 192.168.1.0 version 1.0.0\"}}},\"agent_steps_summary\":\"test summary\"}";

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(2);
            MLExecuteTaskResponse mockResponse = mock(MLExecuteTaskResponse.class);
            when(mockResponse.getOutput()).thenReturn(createConversationalAgentResponseWithTrailingZeros(responseJson));
            actionListener.onResponse(mockResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        AgentInfoDTO agentInfo = new AgentInfoDTO(agentType, hasSystemPrompt, false);

        accessor.executeAgent(mockRequest, mockQuery, agentId, agentInfo, registry, listener);

        verify(client).execute(any(), any(), any());
        ArgumentCaptor<AgentExecutionDTO> resultCaptor = ArgumentCaptor.forClass(AgentExecutionDTO.class);
        verify(listener).onResponse(resultCaptor.capture());

        AgentExecutionDTO result = resultCaptor.getValue();
        String dslQuery = result.getDslQuery();

        // Assertions for normalized numbers (JSON format has no spaces after colons)
        assertTrue(dslQuery.contains("\"size\":12")); // 12.00 -> 12
        assertFalse(dslQuery.contains("\"size\":12.00"));
        assertTrue(dslQuery.contains("\"track_total_hits\":7")); // 7.0 -> 7
        assertFalse(dslQuery.contains("\"track_total_hits\":7.0"));

        // Assertions for preserved values
        assertTrue(dslQuery.contains("192.168.1.0")); // IP preserved
        assertTrue(dslQuery.contains("1.0.0")); // version preserved
        assertTrue(dslQuery.contains("100000")); // scientific notation 1.0e5 converted to 100000

        assertEquals("test summary", result.getAgentStepsSummary());
        assertEquals("test-memory-123", result.getMemoryId());
    }

    private ModelTensorOutput createConversationalAgentResponseWithTrailingZeros(String responseJson) {
        final List<ModelTensors> tensorsList = new ArrayList<>();
        final List<ModelTensor> mlModelTensorList = new ArrayList<>();

        // Create proper conversational agent response format with trailing zeros
        final ModelTensor responseTensor = new ModelTensor("response", null, null, null, null, null, Map.of("response", responseJson));
        final ModelTensor memoryTensor = new ModelTensor("memory_id", null, null, null, null, "test-memory-123", Map.of());

        mlModelTensorList.add(responseTensor);
        mlModelTensorList.add(memoryTensor);
        final ModelTensors modelTensors = new ModelTensors(mlModelTensorList);
        tensorsList.add(modelTensors);
        return new ModelTensorOutput(tensorsList);
    }
}
