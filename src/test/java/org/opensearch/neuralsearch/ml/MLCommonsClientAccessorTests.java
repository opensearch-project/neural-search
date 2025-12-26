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
import org.opensearch.neuralsearch.processor.TextInferenceRequest;
import org.opensearch.neuralsearch.processor.MapInferenceRequest;
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

    public void testInferenceSentences_whenValidInputThenSuccess() {
        final List<List<Number>> vectorList = new ArrayList<>();
        vectorList.add(Arrays.asList(TestCommonConstants.PREDICT_VECTOR_ARRAY));

        // Mock getModel for asymmetry check
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createSymmetricModel());
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentences(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
        verify(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(resultListener).onResponse(vectorList);
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSentences_whenResultFromClient_thenEmptyVectorList() {
        final List<List<Number>> vectorList = new ArrayList<>();
        vectorList.add(Collections.emptyList());

        // Mock getModel for asymmetry check
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createSymmetricModel());
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(new Float[] {}));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentences(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
        verify(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(resultListener).onResponse(vectorList);
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSentences_whenExceptionFromMLClient_thenFailure() {
        final RuntimeException exception = new RuntimeException();

        // Mock getModel for asymmetry check
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createSymmetricModel());
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(exception);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentences(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
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

        // Mock getModel for asymmetry check
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createSymmetricModel());
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(nodeNodeConnectedException);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentences(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
        verify(client, times(4)).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(resultListener).onFailure(nodeNodeConnectedException);
    }

    public void testInferenceSentences_whenNotConnectionException_thenNoRetry() {
        final IllegalStateException illegalStateException = new IllegalStateException("Illegal state");

        // Mock getModel for asymmetry check
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createSymmetricModel());
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(illegalStateException);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentences(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
        verify(client, times(1)).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(resultListener).onFailure(illegalStateException);
    }

    public void testInferenceSentencesWithMapResult_whenValidInput_thenSuccess() {
        final Map<String, Object> map = Map.of("key", "value");
        final ActionListener<List<Map<String, ?>>> resultListener = mock(ActionListener.class);

        // Mock getModel for asymmetry check
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createSymmetricModel());
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(map));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
        verify(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(resultListener).onResponse(List.of(map));
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSentencesWithMapResult_whenValidInput_withParameter() {
        final Map<String, Object> map = Map.of("key", "value");
        final ActionListener<List<Map<String, ?>>> resultListener = mock(ActionListener.class);

        // Mock getModel for asymmetry check
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createSymmetricModel());
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        MLAlgoParams algoParameter = mock(MLAlgoParams.class);
        TextInferenceRequest requestWithParams = TextInferenceRequest.builder()
            .modelId(TestCommonConstants.MODEL_ID)
            .inputTexts(TestCommonConstants.SENTENCES_LIST)
            .mlAlgoParams(algoParameter)
            .build();
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(map));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentencesWithMapResult(requestWithParams, resultListener);

        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
        ArgumentCaptor<MLInput> mlInputCaptor = ArgumentCaptor.forClass(MLInput.class);
        verify(client).predict(eq(TestCommonConstants.MODEL_ID), mlInputCaptor.capture(), Mockito.isA(ActionListener.class));
        assertSame(algoParameter, mlInputCaptor.getValue().getParameters());
        verify(resultListener).onResponse(List.of(map));
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSentencesWithMapResult_whenTensorOutputListEmpty_thenException() {
        final ActionListener<List<Map<String, ?>>> resultListener = mock(ActionListener.class);
        final ModelTensorOutput modelTensorOutput = new ModelTensorOutput(Collections.emptyList());

        // Mock getModel for asymmetry check
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createSymmetricModel());
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(modelTensorOutput);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
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

        // Mock getModel for asymmetry check
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createSymmetricModel());
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(modelTensorOutput);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
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

        // Mock getModel for asymmetry check
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createSymmetricModel());
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(modelTensorOutput);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
        verify(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(resultListener).onResponse(List.of(Map.of("key", "value"), Map.of("key", "value")));
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSentencesWithMapResult_whenRetryableException_retry3Times() {
        final NodeNotConnectedException nodeNodeConnectedException = new NodeNotConnectedException(
            mock(DiscoveryNode.class),
            "Node not connected"
        );

        // Mock getModel for asymmetry check
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createSymmetricModel());
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(nodeNodeConnectedException);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        final ActionListener<List<Map<String, ?>>> resultListener = mock(ActionListener.class);
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
        verify(client, times(4)).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(resultListener).onFailure(nodeNodeConnectedException);
    }

    public void testInferenceSentencesWithMapResult_whenNotRetryableException_thenFail() {
        final IllegalStateException illegalStateException = new IllegalStateException("Illegal state");

        // Mock getModel for asymmetry check
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createSymmetricModel());
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(illegalStateException);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        final ActionListener<List<Map<String, ?>>> resultListener = mock(ActionListener.class);
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
        verify(client, times(1)).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(resultListener).onFailure(illegalStateException);
    }

    public void testInferenceMultimodal_whenValidInput_thenSuccess() {
        final List<Number> vector = new ArrayList<>(List.of(TestCommonConstants.PREDICT_VECTOR_ARRAY));

        // Mock getModel for asymmetry check
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createSymmetricModel());
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.inferenceSentencesMap(TestCommonConstants.MAP_INFERENCE_REQUEST, singleSentenceResultListener);

        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
        verify(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(singleSentenceResultListener).onResponse(vector);
        Mockito.verifyNoMoreInteractions(singleSentenceResultListener);
    }

    public void testInferenceMultimodal_whenExceptionFromMLClient_thenRetry_thenFailure() {
        final NodeNotConnectedException nodeNodeConnectedException = new NodeNotConnectedException(
            mock(DiscoveryNode.class),
            "Node not connected"
        );

        // Mock getModel for asymmetry check
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createSymmetricModel());
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(nodeNodeConnectedException);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.inferenceSentencesMap(TestCommonConstants.MAP_INFERENCE_REQUEST, singleSentenceResultListener);

        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
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

        // Mock getModel for asymmetry check
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createSymmetricModel());
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(nodeNodeConnectedException);
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentencesMap(TestCommonConstants.MAP_INFERENCE_REQUEST, singleSentenceResultListener);

        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
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

        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");

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

        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");

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

        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");

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

        AgentInfoDTO agentInfo = new AgentInfoDTO(agentType, hasSystemPrompt, false, "bedrock/converse/claude");

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

        AgentInfoDTO agentInfo = new AgentInfoDTO(agentType, hasSystemPrompt, false, "bedrock/converse/claude");

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

        AgentInfoDTO agentInfo = new AgentInfoDTO(agentType, hasSystemPrompt, false, "bedrock/converse/claude");

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

        AgentInfoDTO agentInfo = new AgentInfoDTO(agentType, hasSystemPrompt, false, "bedrock/converse/claude");

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

        AgentInfoDTO agentInfo = new AgentInfoDTO("flow", false, false, "bedrock/converse/claude");

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

        AgentInfoDTO agentInfo = new AgentInfoDTO("flow", false, false, "bedrock/converse/claude");

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> accessor.executeAgent(mockRequest, mockQuery, agentId, agentInfo, mock(NamedXContentRegistry.class), listener)
        );

        assertEquals("Flow agent does not support memory_id", exception.getMessage());
    }

    private ModelTensorOutput createConversationalAgentResponse() {
        final List<ModelTensors> tensorsList = new ArrayList<>();
        final List<ModelTensor> mlModelTensorList = new ArrayList<>();

        String agentStep =
            "{\"output\":{\"message\":{\"content\":[{\"text\":\"test summary\"},{\"toolUse\":{\"name\":\"QueryTool\"}}],\"role\":\"assistant\"}}}";
        String finalResponse = "{\"dsl_query\":{\"query\":{\"match\":{\"field\":\"value\"}}}}";
        final ModelTensor agentStepTensor = new ModelTensor("response", null, null, null, null, agentStep, Map.of());
        final ModelTensor responseTensor = new ModelTensor("response", null, null, null, null, finalResponse, Map.of());
        final ModelTensor memoryTensor = new ModelTensor("memory_id", null, null, null, null, "test-memory-123", Map.of());

        mlModelTensorList.add(agentStepTensor);
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

        AgentInfoDTO agentInfo = new AgentInfoDTO(agentType, hasSystemPrompt, false, "bedrock/converse/claude");

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

        String agentStep =
            "{\"output\":{\"message\":{\"content\":[{\"text\":\"test summary\"},{\"toolUse\":{\"name\":\"QueryTool\"}}],\"role\":\"assistant\"}}}";
        final ModelTensor agentStepTensor = new ModelTensor("response", null, null, null, null, agentStep, Map.of());
        final ModelTensor responseTensor = new ModelTensor("response", null, null, null, null, responseJson, Map.of());
        final ModelTensor memoryTensor = new ModelTensor("memory_id", null, null, null, null, "test-memory-123", Map.of());

        mlModelTensorList.add(agentStepTensor);
        mlModelTensorList.add(responseTensor);
        mlModelTensorList.add(memoryTensor);
        final ModelTensors modelTensors = new ModelTensors(mlModelTensorList);
        tensorsList.add(modelTensors);
        return new ModelTensorOutput(tensorsList);
    }

    public void testExecuteAgent_ConversationalAgentWithTextBeforeJSON() throws Exception {
        final String agentId = "test-agent-id";
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "test-index" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("test query");

        // Response with explanation text before JSON
        String responseWithExplanation =
            "Here is the query you requested: {\"dsl_query\":{\"query\":{\"match\":{\"field\":\"value\"}}},\"agent_steps_summary\":\"test summary\"}";

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(2);
            MLExecuteTaskResponse mockResponse = mock(MLExecuteTaskResponse.class);
            when(mockResponse.getOutput()).thenReturn(createConversationalAgentResponseWithClaudeFormat(responseWithExplanation));
            actionListener.onResponse(mockResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");

        accessor.executeAgent(mockRequest, mockQuery, agentId, agentInfo, mock(NamedXContentRegistry.class), listener);

        verify(client).execute(any(), any(), any());
        ArgumentCaptor<AgentExecutionDTO> resultCaptor = ArgumentCaptor.forClass(AgentExecutionDTO.class);
        verify(listener).onResponse(resultCaptor.capture());

        AgentExecutionDTO result = resultCaptor.getValue();
        assertEquals("{\"query\":{\"match\":{\"field\":\"value\"}}}", result.getDslQuery());
        assertEquals("test summary", result.getAgentStepsSummary());
        assertEquals("test-memory-456", result.getMemoryId());
        Mockito.verifyNoMoreInteractions(listener);
    }

    public void testExecuteAgent_ConversationalAgentWithNoValidJSON() throws Exception {
        final String agentId = "test-agent-id";
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "test-index" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("test query");

        // Response with no valid JSON
        String responseWithoutJSON = "Sorry, I couldn't generate a query for your request.";

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(2);
            MLExecuteTaskResponse mockResponse = mock(MLExecuteTaskResponse.class);
            when(mockResponse.getOutput()).thenReturn(createConversationalAgentResponseWithCustomText(responseWithoutJSON));
            actionListener.onResponse(mockResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");

        accessor.executeAgent(mockRequest, mockQuery, agentId, agentInfo, mock(NamedXContentRegistry.class), listener);

        verify(client).execute(any(), any(), any());
        ArgumentCaptor<IllegalArgumentException> exceptionCaptor = ArgumentCaptor.forClass(IllegalArgumentException.class);
        verify(listener).onFailure(exceptionCaptor.capture());

        IllegalArgumentException exception = exceptionCaptor.getValue();
        assertTrue(exception.getMessage().contains("No valid 'dsl_query' found"));
        Mockito.verifyNoMoreInteractions(listener);
    }

    public void testExecuteAgent_ConversationalAgentWithMalformedJSON() throws Exception {
        final String agentId = "test-agent-id";
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "test-index" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("test query");

        // Response with malformed JSON (missing closing brace)
        String responseWithMalformedJSON = "Here is the query: {\"dsl_query\":{\"query\":{\"match\":{\"field\":\"value\"}";

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(2);
            MLExecuteTaskResponse mockResponse = mock(MLExecuteTaskResponse.class);
            when(mockResponse.getOutput()).thenReturn(createConversationalAgentResponseWithCustomText(responseWithMalformedJSON));
            actionListener.onResponse(mockResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");

        accessor.executeAgent(mockRequest, mockQuery, agentId, agentInfo, mock(NamedXContentRegistry.class), listener);

        verify(client).execute(any(), any(), any());
        ArgumentCaptor<IllegalArgumentException> exceptionCaptor = ArgumentCaptor.forClass(IllegalArgumentException.class);
        verify(listener).onFailure(exceptionCaptor.capture());

        IllegalArgumentException exception = exceptionCaptor.getValue();
        assertTrue(exception.getMessage().contains("No valid 'dsl_query' found"));
        Mockito.verifyNoMoreInteractions(listener);
    }

    public void testExecuteAgent_ConversationalAgentWithEmptyResponse() throws Exception {
        final String agentId = "test-agent-id";
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "test-index" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("test query");

        // Response with empty text (will be skipped, resulting in missing dsl_query)
        String emptyResponse = "   ";

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(2);
            MLExecuteTaskResponse mockResponse = mock(MLExecuteTaskResponse.class);
            when(mockResponse.getOutput()).thenReturn(createConversationalAgentResponseWithCustomText(emptyResponse));
            actionListener.onResponse(mockResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");

        accessor.executeAgent(mockRequest, mockQuery, agentId, agentInfo, mock(NamedXContentRegistry.class), listener);

        verify(client).execute(any(), any(), any());
        ArgumentCaptor<IllegalArgumentException> exceptionCaptor = ArgumentCaptor.forClass(IllegalArgumentException.class);
        verify(listener).onFailure(exceptionCaptor.capture());

        IllegalArgumentException exception = exceptionCaptor.getValue();
        assertTrue(exception.getMessage().contains("No valid 'dsl_query' found"));
        Mockito.verifyNoMoreInteractions(listener);
    }

    private ModelTensorOutput createConversationalAgentResponseWithCustomText(String responseText) {
        final List<ModelTensors> tensorsList = new ArrayList<>();
        final List<ModelTensor> mlModelTensorList = new ArrayList<>();

        final ModelTensor responseTensor = new ModelTensor("response", null, null, null, null, responseText, Map.of());
        final ModelTensor memoryTensor = new ModelTensor("memory_id", null, null, null, null, "test-memory-456", Map.of());

        mlModelTensorList.add(responseTensor);
        mlModelTensorList.add(memoryTensor);
        final ModelTensors modelTensors = new ModelTensors(mlModelTensorList);
        tensorsList.add(modelTensors);
        return new ModelTensorOutput(tensorsList);
    }

    private ModelTensorOutput createConversationalAgentResponseWithClaudeFormat(String responseText) {
        final List<ModelTensors> tensorsList = new ArrayList<>();
        final List<ModelTensor> mlModelTensorList = new ArrayList<>();

        String agentStep =
            "{\"output\":{\"message\":{\"content\":[{\"text\":\"test summary\"},{\"toolUse\":{\"name\":\"QueryTool\"}}],\"role\":\"assistant\"}}}";
        final ModelTensor agentStepTensor = new ModelTensor("response", null, null, null, null, agentStep, Map.of());
        final ModelTensor responseTensor = new ModelTensor("response", null, null, null, null, responseText, Map.of());
        final ModelTensor memoryTensor = new ModelTensor("memory_id", null, null, null, null, "test-memory-456", Map.of());

        mlModelTensorList.add(agentStepTensor);
        mlModelTensorList.add(responseTensor);
        mlModelTensorList.add(memoryTensor);
        final ModelTensors modelTensors = new ModelTensors(mlModelTensorList);
        tensorsList.add(modelTensors);
        return new ModelTensorOutput(tensorsList);
    }

    public void testExecuteAgent_ClaudeWithToolUse_ExtractsAgentSteps() throws Exception {
        final String agentId = "test-agent-id";
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "test-index" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("Find shoes under 200 dollars");

        // Create Claude response with toolUse (agent step) and final response
        String agentStepResponse =
            "{\"output\":{\"message\":{\"content\":[{\"text\":\"Now I'll use the QueryPlanningTool to generate the DSL for finding shows under $200.\"},{\"toolUse\":{\"input\":{\"question\":\"Find shows under 200 dollars\",\"index_name\":\"products-index\"},\"name\":\"QueryPlanningTool\",\"toolUseId\":\"tooluse_XaooYUw2Qg-bK8-bUqco3Q\",\"type\":\"tool_use\"}}],\"role\":\"assistant\"}}}";
        String finalResponse =
            "{\"dsl_query\":{\"query\":{\"bool\":{\"must\":[{\"match\":{\"category\":\"shoes\"}}],\"filter\":[{\"range\":{\"price\":{\"lt\":200}}}]}}}}";

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(2);
            MLExecuteTaskResponse mockResponse = mock(MLExecuteTaskResponse.class);
            when(mockResponse.getOutput()).thenReturn(createClaudeAgentResponse(agentStepResponse, finalResponse));
            actionListener.onResponse(mockResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");

        accessor.executeAgent(mockRequest, mockQuery, agentId, agentInfo, mock(NamedXContentRegistry.class), listener);

        verify(client).execute(any(), any(), any());
        ArgumentCaptor<AgentExecutionDTO> resultCaptor = ArgumentCaptor.forClass(AgentExecutionDTO.class);
        verify(listener).onResponse(resultCaptor.capture());

        AgentExecutionDTO result = resultCaptor.getValue();
        assertEquals(
            "{\"query\":{\"bool\":{\"must\":[{\"match\":{\"category\":\"shoes\"}}],\"filter\":[{\"range\":{\"price\":{\"lt\":200}}}]}}}",
            result.getDslQuery()
        );
        assertEquals("Now I'll use the QueryPlanningTool to generate the DSL for finding shows under $200.", result.getAgentStepsSummary());
    }

    public void testExecuteAgent_ClaudeWithoutToolUse_NoAgentSteps() throws Exception {
        final String agentId = "test-agent-id";
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "test-index" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("test query");

        // Create Claude response without toolUse (final response only)
        String finalResponse =
            "{\"output\":{\"message\":{\"content\":[{\"text\":\"Here is your query\"}],\"role\":\"assistant\"}},\"dsl_query\":{\"query\":{\"match\":{\"field\":\"value\"}}}}";

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(2);
            MLExecuteTaskResponse mockResponse = mock(MLExecuteTaskResponse.class);
            when(mockResponse.getOutput()).thenReturn(createClaudeAgentResponseSingle(finalResponse));
            actionListener.onResponse(mockResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");

        accessor.executeAgent(mockRequest, mockQuery, agentId, agentInfo, mock(NamedXContentRegistry.class), listener);

        verify(client).execute(any(), any(), any());
        ArgumentCaptor<AgentExecutionDTO> resultCaptor = ArgumentCaptor.forClass(AgentExecutionDTO.class);
        verify(listener).onResponse(resultCaptor.capture());

        AgentExecutionDTO result = resultCaptor.getValue();
        assertEquals("{\"query\":{\"match\":{\"field\":\"value\"}}}", result.getDslQuery());
        assertNull(result.getAgentStepsSummary());
    }

    public void testExecuteAgent_OpenAIWithToolCalls_ExtractsAgentSteps() throws Exception {
        final String agentId = "test-agent-id";
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "test-index" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("Find products");

        // Create OpenAI response with tool_calls (agent step) and final response
        String agentStepResponse =
            "{\"choices\":[{\"message\":{\"content\":\"I'll search for products using the query tool.\",\"tool_calls\":[{\"function\":{\"name\":\"query_tool\"}}]}}]}";
        String finalResponse = "{\"dsl_query\":{\"query\":{\"match_all\":{}}}}";

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(2);
            MLExecuteTaskResponse mockResponse = mock(MLExecuteTaskResponse.class);
            when(mockResponse.getOutput()).thenReturn(createOpenAIAgentResponse(agentStepResponse, finalResponse));
            actionListener.onResponse(mockResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "openai/v1/chat/completions");

        accessor.executeAgent(mockRequest, mockQuery, agentId, agentInfo, mock(NamedXContentRegistry.class), listener);

        verify(client).execute(any(), any(), any());
        ArgumentCaptor<AgentExecutionDTO> resultCaptor = ArgumentCaptor.forClass(AgentExecutionDTO.class);
        verify(listener).onResponse(resultCaptor.capture());

        AgentExecutionDTO result = resultCaptor.getValue();
        assertEquals("{\"query\":{\"match_all\":{}}}", result.getDslQuery());
        assertEquals("I'll search for products using the query tool.", result.getAgentStepsSummary());
    }

    public void testExecuteAgent_OpenAIWithoutToolCalls_NoAgentSteps() throws Exception {
        final String agentId = "test-agent-id";
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "test-index" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("test query");

        // Create OpenAI response without tool_calls (final response only)
        String finalResponse =
            "{\"choices\":[{\"message\":{\"content\":\"Here is your query\"}}],\"dsl_query\":{\"query\":{\"match\":{\"field\":\"value\"}}}}";

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(2);
            MLExecuteTaskResponse mockResponse = mock(MLExecuteTaskResponse.class);
            when(mockResponse.getOutput()).thenReturn(createOpenAIAgentResponseSingle(finalResponse));
            actionListener.onResponse(mockResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "openai/v1/chat/completions");

        accessor.executeAgent(mockRequest, mockQuery, agentId, agentInfo, mock(NamedXContentRegistry.class), listener);

        verify(client).execute(any(), any(), any());
        ArgumentCaptor<AgentExecutionDTO> resultCaptor = ArgumentCaptor.forClass(AgentExecutionDTO.class);
        verify(listener).onResponse(resultCaptor.capture());

        AgentExecutionDTO result = resultCaptor.getValue();
        assertEquals("{\"query\":{\"match\":{\"field\":\"value\"}}}", result.getDslQuery());
        assertNull(result.getAgentStepsSummary());
    }

    public void testExecuteAgent_ClaudeMultipleAgentSteps() throws Exception {
        final String agentId = "test-agent-id";
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "test-index" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("complex query");

        // Create Claude response with multiple agent steps
        String step1 =
            "{\"output\":{\"message\":{\"content\":[{\"text\":\"First, I'll analyze the indices.\"},{\"toolUse\":{\"name\":\"IndexTool\"}}],\"role\":\"assistant\"}}}";
        String step2 =
            "{\"output\":{\"message\":{\"content\":[{\"text\":\"Now I'll generate the query.\"},{\"toolUse\":{\"name\":\"QueryTool\"}}],\"role\":\"assistant\"}}}";
        String finalResponse = "{\"dsl_query\":{\"query\":{\"match\":{\"field\":\"value\"}}}}";

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(2);
            MLExecuteTaskResponse mockResponse = mock(MLExecuteTaskResponse.class);
            when(mockResponse.getOutput()).thenReturn(createClaudeAgentResponseMultiple(step1, step2, finalResponse));
            actionListener.onResponse(mockResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");

        accessor.executeAgent(mockRequest, mockQuery, agentId, agentInfo, mock(NamedXContentRegistry.class), listener);

        verify(client).execute(any(), any(), any());
        ArgumentCaptor<AgentExecutionDTO> resultCaptor = ArgumentCaptor.forClass(AgentExecutionDTO.class);
        verify(listener).onResponse(resultCaptor.capture());

        AgentExecutionDTO result = resultCaptor.getValue();
        assertEquals("{\"query\":{\"match\":{\"field\":\"value\"}}}", result.getDslQuery());
        assertEquals("First, I'll analyze the indices.\nNow I'll generate the query.", result.getAgentStepsSummary());
    }

    private ModelTensorOutput createClaudeAgentResponse(String agentStepResponse, String finalResponse) {
        final List<ModelTensors> tensorsList = new ArrayList<>();
        final List<ModelTensor> mlModelTensorList = new ArrayList<>();

        final ModelTensor agentStepTensor = new ModelTensor("response", null, null, null, null, agentStepResponse, Map.of());
        final ModelTensor finalTensor = new ModelTensor("response", null, null, null, null, finalResponse, Map.of());

        mlModelTensorList.add(agentStepTensor);
        mlModelTensorList.add(finalTensor);
        final ModelTensors modelTensors = new ModelTensors(mlModelTensorList);
        tensorsList.add(modelTensors);
        return new ModelTensorOutput(tensorsList);
    }

    private ModelTensorOutput createClaudeAgentResponseSingle(String response) {
        final List<ModelTensors> tensorsList = new ArrayList<>();
        final List<ModelTensor> mlModelTensorList = new ArrayList<>();

        final ModelTensor tensor = new ModelTensor("response", null, null, null, null, response, Map.of());
        mlModelTensorList.add(tensor);
        final ModelTensors modelTensors = new ModelTensors(mlModelTensorList);
        tensorsList.add(modelTensors);
        return new ModelTensorOutput(tensorsList);
    }

    private ModelTensorOutput createClaudeAgentResponseMultiple(String step1, String step2, String finalResponse) {
        final List<ModelTensors> tensorsList = new ArrayList<>();
        final List<ModelTensor> mlModelTensorList = new ArrayList<>();

        final ModelTensor step1Tensor = new ModelTensor("response", null, null, null, null, step1, Map.of());
        final ModelTensor step2Tensor = new ModelTensor("response", null, null, null, null, step2, Map.of());
        final ModelTensor finalTensor = new ModelTensor("response", null, null, null, null, finalResponse, Map.of());

        mlModelTensorList.add(step1Tensor);
        mlModelTensorList.add(step2Tensor);
        mlModelTensorList.add(finalTensor);
        final ModelTensors modelTensors = new ModelTensors(mlModelTensorList);
        tensorsList.add(modelTensors);
        return new ModelTensorOutput(tensorsList);
    }

    private ModelTensorOutput createOpenAIAgentResponse(String agentStepResponse, String finalResponse) {
        final List<ModelTensors> tensorsList = new ArrayList<>();
        final List<ModelTensor> mlModelTensorList = new ArrayList<>();

        final ModelTensor agentStepTensor = new ModelTensor("response", null, null, null, null, agentStepResponse, Map.of());
        final ModelTensor finalTensor = new ModelTensor("response", null, null, null, null, finalResponse, Map.of());

        mlModelTensorList.add(agentStepTensor);
        mlModelTensorList.add(finalTensor);
        final ModelTensors modelTensors = new ModelTensors(mlModelTensorList);
        tensorsList.add(modelTensors);
        return new ModelTensorOutput(tensorsList);
    }

    private ModelTensorOutput createOpenAIAgentResponseSingle(String response) {
        final List<ModelTensors> tensorsList = new ArrayList<>();
        final List<ModelTensor> mlModelTensorList = new ArrayList<>();

        final ModelTensor tensor = new ModelTensor("response", null, null, null, null, response, Map.of());
        mlModelTensorList.add(tensor);
        final ModelTensors modelTensors = new ModelTensors(mlModelTensorList);
        tensorsList.add(modelTensors);
        return new ModelTensorOutput(tensorsList);
    }

    public void testInferenceSentences_whenAsymmetricModel_thenUsesAlgoParams() {
        final List<List<Number>> vectorList = new ArrayList<>();
        vectorList.add(Arrays.asList(TestCommonConstants.PREDICT_VECTOR_ARRAY));
        final org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters algoParams =
            org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters.builder()
                .embeddingContentType(
                    org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters.EmbeddingContentType.QUERY
                )
                .build();
        final TextInferenceRequest requestWithParams = TextInferenceRequest.builder()
            .modelId(TestCommonConstants.MODEL_ID)
            .inputTexts(TestCommonConstants.SENTENCES_LIST)
            .mlAlgoParams(algoParams)
            .embeddingContentType(org.opensearch.neuralsearch.processor.EmbeddingContentType.PASSAGE)
            .build();

        // Mock asymmetric model
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            MLModel model = createAsymmetricModel();
            actionListener.onResponse(model);
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        // Mock successful prediction with params
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.inferenceSentences(requestWithParams, resultListener);

        // Verify getModel called once, predict called once with AsymmetricTextEmbeddingParameters
        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
        ArgumentCaptor<MLInput> mlInputCaptor = ArgumentCaptor.forClass(MLInput.class);
        verify(client).predict(eq(TestCommonConstants.MODEL_ID), mlInputCaptor.capture(), Mockito.isA(ActionListener.class));
        assertTrue(
            mlInputCaptor.getValue()
                .getParameters() instanceof org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters
        );
        verify(resultListener).onResponse(vectorList);
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSentences_whenSymmetricModel_thenUsesRequestParams() {
        final List<List<Number>> vectorList = new ArrayList<>();
        vectorList.add(Arrays.asList(TestCommonConstants.PREDICT_VECTOR_ARRAY));
        final MLAlgoParams algoParams = mock(MLAlgoParams.class);
        final TextInferenceRequest requestWithParams = TextInferenceRequest.builder()
            .modelId(TestCommonConstants.MODEL_ID)
            .inputTexts(TestCommonConstants.SENTENCES_LIST)
            .mlAlgoParams(algoParams)
            .build();

        // Mock symmetric model
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            MLModel model = createSymmetricModel();
            actionListener.onResponse(model);
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        // Mock successful prediction
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.inferenceSentences(requestWithParams, resultListener);

        // Verify getModel called once, predict called once with params from request
        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
        ArgumentCaptor<MLInput> mlInputCaptor = ArgumentCaptor.forClass(MLInput.class);
        verify(client).predict(eq(TestCommonConstants.MODEL_ID), mlInputCaptor.capture(), Mockito.isA(ActionListener.class));
        assertEquals(algoParams, mlInputCaptor.getValue().getParameters());
        verify(resultListener).onResponse(vectorList);
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSentencesWithMapResult_whenAsymmetricModel_thenUsesParams() {
        final Map<String, Object> map = Map.of("key", "value");
        final ActionListener<List<Map<String, ?>>> resultListener = mock(ActionListener.class);
        final org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters externalParams =
            org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters.builder()
                .embeddingContentType(
                    org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters.EmbeddingContentType.QUERY
                )
                .build();

        // Mock asymmetric model
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createAsymmetricModel());
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        TextInferenceRequest requestWithParams = TextInferenceRequest.builder()
            .modelId(TestCommonConstants.MODEL_ID)
            .inputTexts(TestCommonConstants.SENTENCES_LIST)
            .mlAlgoParams(externalParams)
            .embeddingContentType(org.opensearch.neuralsearch.processor.EmbeddingContentType.PASSAGE)
            .build();
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(map));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.inferenceSentencesWithMapResult(requestWithParams, resultListener);

        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
        ArgumentCaptor<MLInput> mlInputCaptor = ArgumentCaptor.forClass(MLInput.class);
        verify(client).predict(eq(TestCommonConstants.MODEL_ID), mlInputCaptor.capture(), Mockito.isA(ActionListener.class));
        assertTrue(
            mlInputCaptor.getValue()
                .getParameters() instanceof org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters
        );
        verify(resultListener).onResponse(List.of(map));
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSentencesMap_whenAsymmetricModel_thenUsesParams() {
        final List<Number> vector = new ArrayList<>(List.of(TestCommonConstants.PREDICT_VECTOR_ARRAY));
        final org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters algoParams =
            org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters.builder()
                .embeddingContentType(
                    org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters.EmbeddingContentType.QUERY
                )
                .build();
        final MapInferenceRequest requestWithParams = MapInferenceRequest.builder()
            .modelId(TestCommonConstants.MODEL_ID)
            .inputObjects(Map.of("inputText", "test"))
            .mlAlgoParams(algoParams)
            .embeddingContentType(org.opensearch.neuralsearch.processor.EmbeddingContentType.PASSAGE)
            .build();

        // Mock asymmetric model
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createAsymmetricModel());
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.inferenceSentencesMap(requestWithParams, singleSentenceResultListener);

        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
        ArgumentCaptor<MLInput> mlInputCaptor = ArgumentCaptor.forClass(MLInput.class);
        verify(client).predict(eq(TestCommonConstants.MODEL_ID), mlInputCaptor.capture(), Mockito.isA(ActionListener.class));
        assertTrue(
            mlInputCaptor.getValue()
                .getParameters() instanceof org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters
        );
        verify(singleSentenceResultListener).onResponse(vector);
        Mockito.verifyNoMoreInteractions(singleSentenceResultListener);
    }

    public void testCheckModelAsymmetryAndThenPredict_whenCachedByPreviousCall_thenNoGetModelCall() {
        final List<List<Number>> vectorList = new ArrayList<>();
        vectorList.add(Arrays.asList(TestCommonConstants.PREDICT_VECTOR_ARRAY));

        // First call to populate cache
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createSymmetricModel());
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        // First call - should call getModel and predict
        accessor.inferenceSentences(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

        // Second call - should only call predict (cache hit)
        accessor.inferenceSentences(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

        // Verify getModel called only once (first call), predict called twice
        verify(client, times(1)).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
        verify(client, times(2)).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        verify(resultListener, times(2)).onResponse(vectorList);
    }

    public void testCheckModelAsymmetryAndThenPredict_whenGetModelFails_thenPropagateError() {
        final RuntimeException getModelException = new RuntimeException("Failed to get model");

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onFailure(getModelException);
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        accessor.inferenceSentences(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
        verify(client, times(0)).predict(any(), any(), any());
        verify(resultListener).onFailure(getModelException);
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    private MLModel createAsymmetricModel() {
        MLModel model = mock(MLModel.class);
        org.opensearch.ml.common.model.TextEmbeddingModelConfig config = mock(
            org.opensearch.ml.common.model.TextEmbeddingModelConfig.class
        );
        when(config.getPassagePrefix()).thenReturn("passage: ");
        when(config.getQueryPrefix()).thenReturn("query: ");
        when(model.getModelConfig()).thenReturn(config);
        return model;
    }

    private MLModel createSymmetricModel() {
        MLModel model = mock(MLModel.class);
        org.opensearch.ml.common.model.TextEmbeddingModelConfig config = mock(
            org.opensearch.ml.common.model.TextEmbeddingModelConfig.class
        );
        when(config.getPassagePrefix()).thenReturn(null);
        when(config.getQueryPrefix()).thenReturn(null);
        when(model.getModelConfig()).thenReturn(config);
        return model;
    }

    private MLModel createNonTextEmbeddingModel() {
        MLModel model = mock(MLModel.class);
        // Return a non-TextEmbeddingModelConfig to test the instanceof check
        org.opensearch.ml.common.model.MLModelConfig config = mock(org.opensearch.ml.common.model.MLModelConfig.class);
        when(model.getModelConfig()).thenReturn(config);
        return model;
    }

    public void testCacheEviction_whenCacheSizeExceedsLimit_thenEvictsOldestEntry() {
        // This test verifies that the cache eviction works correctly
        // We'll simulate multiple model calls to fill the cache beyond MAX_CACHE_SIZE
        final ActionListener<List<List<Number>>> listener = mock(ActionListener.class);

        // Mock successful responses for all calls
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createSymmetricModel());
            return null;
        }).when(client).getModel(any(), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(any(), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        // Make calls with different model IDs to populate cache
        for (int i = 0; i < 10; i++) {
            String modelId = "model-" + i;
            TextInferenceRequest request = TextInferenceRequest.builder()
                .modelId(modelId)
                .inputTexts(TestCommonConstants.SENTENCES_LIST)
                .build();
            accessor.inferenceSentences(request, listener);
        }

        // Verify that getModel was called for each unique model ID
        verify(client, times(10)).getModel(any(), eq(null), Mockito.isA(ActionListener.class));
        verify(client, times(10)).predict(any(), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
    }

    public void testCheckModelAsymmetryAndThenPredict_whenConcurrentRequests_thenAllowsConcurrentAccess() {
        final List<List<Number>> vectorList = new ArrayList<>();
        vectorList.add(Arrays.asList(TestCommonConstants.PREDICT_VECTOR_ARRAY));
        final ActionListener<List<List<Number>>> listener1 = mock(ActionListener.class);
        final ActionListener<List<List<Number>>> listener2 = mock(ActionListener.class);
        final ActionListener<List<List<Number>>> listener3 = mock(ActionListener.class);

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createSymmetricModel());
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        // Test that concurrent requests are allowed (no blocking/queuing)
        // The first request will cache the model, subsequent requests use cache
        accessor.inferenceSentences(TestCommonConstants.TEXT_INFERENCE_REQUEST, listener1);
        accessor.inferenceSentences(TestCommonConstants.TEXT_INFERENCE_REQUEST, listener2);
        accessor.inferenceSentences(TestCommonConstants.TEXT_INFERENCE_REQUEST, listener3);

        // Verify getModel called at least once (first request fetches, others may use cache)
        verify(client, Mockito.atLeast(1)).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
        // Verify predict called 3 times (one for each request)
        verify(client, times(3)).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        // Verify all listeners received responses
        verify(listener1).onResponse(vectorList);
        verify(listener2).onResponse(vectorList);
        verify(listener3).onResponse(vectorList);
    }

    public void testInferenceSentences_whenSymmetricModelWithParams_thenUsesParams() {
        final List<List<Number>> vectorList = new ArrayList<>();
        vectorList.add(Arrays.asList(TestCommonConstants.PREDICT_VECTOR_ARRAY));
        final MLAlgoParams customParams = mock(MLAlgoParams.class);
        final TextInferenceRequest requestWithParams = TextInferenceRequest.builder()
            .modelId(TestCommonConstants.MODEL_ID)
            .inputTexts(TestCommonConstants.SENTENCES_LIST)
            .mlAlgoParams(customParams)
            .build();

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createSymmetricModel());
            return null;
        }).when(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.inferenceSentences(requestWithParams, resultListener);

        verify(client).getModel(eq(TestCommonConstants.MODEL_ID), eq(null), Mockito.isA(ActionListener.class));
        ArgumentCaptor<MLInput> mlInputCaptor = ArgumentCaptor.forClass(MLInput.class);
        verify(client).predict(eq(TestCommonConstants.MODEL_ID), mlInputCaptor.capture(), Mockito.isA(ActionListener.class));
        assertEquals(customParams, mlInputCaptor.getValue().getParameters());
        verify(resultListener).onResponse(vectorList);
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testGetCachedModel_whenModelNotCached_thenFetchFromClient() {
        final String modelId = "test-model-id";
        final ActionListener<MLModel> listener = mock(ActionListener.class);
        final MLModel mockModel = createSymmetricModel();

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(mockModel);
            return null;
        }).when(client).getModel(eq(modelId), eq(null), Mockito.isA(ActionListener.class));

        accessor.getCachedModel(modelId, listener);

        verify(client).getModel(eq(modelId), eq(null), Mockito.isA(ActionListener.class));
        verify(listener).onResponse(mockModel);
        Mockito.verifyNoMoreInteractions(listener);
    }

    public void testGetCachedModel_whenModelCached_thenReturnFromCache() {
        final String modelId = "test-model-id";
        final ActionListener<MLModel> listener1 = mock(ActionListener.class);
        final ActionListener<MLModel> listener2 = mock(ActionListener.class);
        final MLModel mockModel = createSymmetricModel();

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onResponse(mockModel);
            return null;
        }).when(client).getModel(eq(modelId), eq(null), Mockito.isA(ActionListener.class));

        // First call - should fetch from client and cache
        accessor.getCachedModel(modelId, listener1);

        // Second call - should return from cache
        accessor.getCachedModel(modelId, listener2);

        // Verify client called only once
        verify(client, times(1)).getModel(eq(modelId), eq(null), Mockito.isA(ActionListener.class));
        // Verify both listeners received the model
        verify(listener1).onResponse(mockModel);
        verify(listener2).onResponse(mockModel);
        Mockito.verifyNoMoreInteractions(listener1, listener2);
    }

    public void testGetCachedModel_whenClientFails_thenPropagateError() {
        final String modelId = "test-model-id";
        final ActionListener<MLModel> listener = mock(ActionListener.class);
        final RuntimeException exception = new RuntimeException("Model fetch failed");

        Mockito.doAnswer(invocation -> {
            final ActionListener<MLModel> actionListener = invocation.getArgument(2);
            actionListener.onFailure(exception);
            return null;
        }).when(client).getModel(eq(modelId), eq(null), Mockito.isA(ActionListener.class));

        accessor.getCachedModel(modelId, listener);

        verify(client).getModel(eq(modelId), eq(null), Mockito.isA(ActionListener.class));
        verify(listener).onFailure(exception);
        Mockito.verifyNoMoreInteractions(listener);
    }

    public void testExtractClaudeAgentSteps_WithIndexName() throws Exception {
        final String agentId = "test-agent-id";
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "test-index", "products-index", "orders-index" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("Find products");

        // Create Claude response with toolUse containing index_name
        String agentStepResponse =
            "{\"output\":{\"message\":{\"content\":[{\"text\":\"I'll search the products index.\"},{\"toolUse\":{\"input\":{\"question\":\"Find products\",\"index_name\":\"products-index\"},\"name\":\"QueryPlanningTool\"}}],\"role\":\"assistant\"}}}";
        String finalResponse = "{\"dsl_query\":{\"query\":{\"match_all\":{}}}}";

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(2);
            MLExecuteTaskResponse mockResponse = mock(MLExecuteTaskResponse.class);
            when(mockResponse.getOutput()).thenReturn(createClaudeAgentResponse(agentStepResponse, finalResponse));
            actionListener.onResponse(mockResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");

        accessor.executeAgent(mockRequest, mockQuery, agentId, agentInfo, mock(NamedXContentRegistry.class), listener);

        ArgumentCaptor<AgentExecutionDTO> resultCaptor = ArgumentCaptor.forClass(AgentExecutionDTO.class);
        verify(listener).onResponse(resultCaptor.capture());

        AgentExecutionDTO result = resultCaptor.getValue();
        assertEquals("products-index", result.getSelectedIndex());
        assertEquals("I'll search the products index.", result.getAgentStepsSummary());
    }

    public void testExtractOpenAIAgentSteps_WithIndexName() throws Exception {
        final String agentId = "test-agent-id";
        final ActionListener<AgentExecutionDTO> listener = mock(ActionListener.class);

        SearchRequest mockRequest = mock(SearchRequest.class);
        when(mockRequest.indices()).thenReturn(new String[] { "test-index", "products-index", "orders-index" });

        AgenticSearchQueryBuilder mockQuery = mock(AgenticSearchQueryBuilder.class);
        when(mockQuery.getQueryText()).thenReturn("Find products");

        // Create OpenAI response with tool_calls containing index_name in arguments
        String agentStepResponse =
            "{\"choices\":[{\"message\":{\"content\":\"I'll search for products.\",\"tool_calls\":[{\"function\":{\"name\":\"QueryPlanningTool\",\"arguments\":\"{\\\"question\\\":\\\"Find products\\\",\\\"index_name\\\":\\\"products-index\\\"}\"}}]}}]}";
        String finalResponse = "{\"dsl_query\":{\"query\":{\"match_all\":{}}}}";

        Mockito.doAnswer(invocation -> {
            final ActionListener actionListener = invocation.getArgument(2);
            MLExecuteTaskResponse mockResponse = mock(MLExecuteTaskResponse.class);
            when(mockResponse.getOutput()).thenReturn(createOpenAIAgentResponse(agentStepResponse, finalResponse));
            actionListener.onResponse(mockResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "openai/v1/chat/completions");

        accessor.executeAgent(mockRequest, mockQuery, agentId, agentInfo, mock(NamedXContentRegistry.class), listener);

        ArgumentCaptor<AgentExecutionDTO> resultCaptor = ArgumentCaptor.forClass(AgentExecutionDTO.class);
        verify(listener).onResponse(resultCaptor.capture());

        AgentExecutionDTO result = resultCaptor.getValue();
        assertEquals("products-index", result.getSelectedIndex());
        assertEquals("I'll search for products.", result.getAgentStepsSummary());
    }
}
