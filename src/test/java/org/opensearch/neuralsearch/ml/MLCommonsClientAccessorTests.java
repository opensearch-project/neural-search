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
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.output.MLOutput;
import org.opensearch.ml.common.output.model.MLResultDataType;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.neuralsearch.constants.TestCommonConstants;
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
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.inferenceSentence(TestCommonConstants.MODEL_ID, TestCommonConstants.SENTENCES_LIST.get(0), singleSentenceResultListener);

        Mockito.verify(client)
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(singleSentenceResultListener).onResponse(vector);
        Mockito.verifyNoMoreInteractions(singleSentenceResultListener);
    }

    public void testInferenceSentences_whenValidInputThenSuccess() {
        final List<List<Number>> vectorList = new ArrayList<>();
        vectorList.add(Arrays.asList(TestCommonConstants.PREDICT_VECTOR_ARRAY));
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentences(TestCommonConstants.MODEL_ID, TestCommonConstants.SENTENCES_LIST, resultListener);

        Mockito.verify(client)
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(resultListener).onResponse(vectorList);
        Mockito.verifyNoMoreInteractions(resultListener);
    }

    public void testInferenceSentences_whenResultFromClient_thenEmptyVectorList() {
        final List<List<Number>> vectorList = new ArrayList<>();
        vectorList.add(Collections.emptyList());
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(new Float[] {}));
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentences(TestCommonConstants.MODEL_ID, TestCommonConstants.SENTENCES_LIST, resultListener);

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
        accessor.inferenceSentences(
            TestCommonConstants.TARGET_RESPONSE_FILTERS,
            TestCommonConstants.MODEL_ID,
            TestCommonConstants.SENTENCES_LIST,
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
        accessor.inferenceSentences(
            TestCommonConstants.TARGET_RESPONSE_FILTERS,
            TestCommonConstants.MODEL_ID,
            TestCommonConstants.SENTENCES_LIST,
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
        accessor.inferenceSentences(
            TestCommonConstants.TARGET_RESPONSE_FILTERS,
            TestCommonConstants.MODEL_ID,
            TestCommonConstants.SENTENCES_LIST,
            resultListener
        );

        Mockito.verify(client, times(1))
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(resultListener).onFailure(illegalStateException);
    }

    public void testInferenceSentencesWithMapResult_whenValidInput_thenSuccess() {
        final Map<String, String> map = Map.of("key", "value");
        final ActionListener<List<Map<String, ?>>> resultListener = mock(ActionListener.class);
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(map));
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.MODEL_ID, TestCommonConstants.SENTENCES_LIST, resultListener);

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
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.MODEL_ID, TestCommonConstants.SENTENCES_LIST, resultListener);

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
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.MODEL_ID, TestCommonConstants.SENTENCES_LIST, resultListener);

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
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.MODEL_ID, TestCommonConstants.SENTENCES_LIST, resultListener);

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
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.MODEL_ID, TestCommonConstants.SENTENCES_LIST, resultListener);

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
        accessor.inferenceSentencesWithMapResult(TestCommonConstants.MODEL_ID, TestCommonConstants.SENTENCES_LIST, resultListener);

        Mockito.verify(client, times(1))
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(resultListener).onFailure(illegalStateException);
    }

    public void testInferenceMultimodal_whenValidInput_thenSuccess() {
        final List<Number> vector = new ArrayList<>(List.of(TestCommonConstants.PREDICT_VECTOR_ARRAY));
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onResponse(createModelTensorOutput(TestCommonConstants.PREDICT_VECTOR_ARRAY));
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.inferenceSentences(TestCommonConstants.MODEL_ID, TestCommonConstants.SENTENCES_MAP, singleSentenceResultListener);

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
        accessor.inferenceSentences(TestCommonConstants.MODEL_ID, TestCommonConstants.SENTENCES_MAP, singleSentenceResultListener);

        Mockito.verify(client)
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(singleSentenceResultListener).onFailure(exception);
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
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        accessor.inferenceSentences(TestCommonConstants.MODEL_ID, TestCommonConstants.SENTENCES_MAP, singleSentenceResultListener);

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

        accessor.inferenceSimilarity(
            TestCommonConstants.MODEL_ID,
            "is it sunny",
            List.of("it is sunny today", "roses are red"),
            similarityResultListener
        );

        Mockito.verify(client)
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(similarityResultListener).onResponse(vector);
        Mockito.verifyNoMoreInteractions(similarityResultListener);
    }

    public void testInferencesSimilarity_whenExceptionFromMLClient_ThenFail() {
        final RuntimeException exception = new RuntimeException();
        Mockito.doAnswer(invocation -> {
            final ActionListener<MLOutput> actionListener = invocation.getArgument(2);
            actionListener.onFailure(exception);
            return null;
        }).when(client).predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));

        accessor.inferenceSimilarity(
            TestCommonConstants.MODEL_ID,
            "is it sunny",
            List.of("it is sunny today", "roses are red"),
            similarityResultListener
        );

        Mockito.verify(client)
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(similarityResultListener).onFailure(exception);
        Mockito.verifyNoMoreInteractions(similarityResultListener);
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

        accessor.inferenceSimilarity(
            TestCommonConstants.MODEL_ID,
            "is it sunny",
            List.of("it is sunny today", "roses are red"),
            similarityResultListener
        );

        Mockito.verify(client, times(4))
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(similarityResultListener).onFailure(nodeNodeConnectedException);
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
