/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.output.MLOutput;
import org.opensearch.ml.common.output.model.MLResultDataType;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
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
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

        verify(client).predict(eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
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
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

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
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

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
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

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
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

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
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.TEXT_INFERENCE_REQUEST, resultListener);

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
}
