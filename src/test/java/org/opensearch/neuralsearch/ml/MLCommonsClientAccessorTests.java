/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.ml;

import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import org.junit.Before;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionListener;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.model.MLResultDataType;
import org.opensearch.ml.common.output.MLOutput;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.neuralsearch.constants.TestCommonConstants;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

        accessor.inferenceSentence(TestCommonConstants.MODEL_ID, TestCommonConstants.SENTENCES_LIST.get(0), singleSentenceResultListener);

        Mockito.verify(client)
            .predict(Mockito.eq(TestCommonConstants.MODEL_ID), Mockito.isA(MLInput.class), Mockito.isA(ActionListener.class));
        Mockito.verify(singleSentenceResultListener).onResponse(vector);
        Mockito.verifyNoMoreInteractions(singleSentenceResultListener);
    }

    public void testInferenceSentences_whenValidInputThenSuccess() {
        final List<List<Float>> vectorList = new ArrayList<>();
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
        final List<List<Float>> vectorList = new ArrayList<>();
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

    @SneakyThrows
    public void test_blockingInferenceSentences() {
        ActionFuture actionFuture = mock(ActionFuture.class);
        when(client.predict(anyString(), any(MLInput.class))).thenReturn(actionFuture);
        List<ModelTensors> tensorsList = new ArrayList<>();

        List<ModelTensor> tensors = new ArrayList<>();
        ModelTensor tensor = mock(ModelTensor.class);
        when(tensor.getData()).thenReturn(TestCommonConstants.PREDICT_VECTOR_ARRAY);
        tensors.add(tensor);

        ModelTensors modelTensors = new ModelTensors(tensors);
        tensorsList.add(modelTensors);

        ModelTensorOutput mlOutput = new ModelTensorOutput(tensorsList);
        when(actionFuture.get()).thenReturn(mlOutput);
        List<List<Float>> result = accessor.inferenceSentences("modelId", ImmutableList.of("mock"));
        assertEquals(TestCommonConstants.PREDICT_VECTOR_ARRAY[0], result.get(0).get(0));
    }

    private ModelTensorOutput createModelTensorOutput(final Float[] output) {
        final List<ModelTensors> tensorsList = new ArrayList<>();
        final List<ModelTensor> mlModelTensorList = new ArrayList<>();
        final ModelTensor tensor = new ModelTensor(
            "someValue",
            output,
            new long[] { 1, 2 },
            MLResultDataType.FLOAT64,
            ByteBuffer.wrap(new byte[12])
        );
        mlModelTensorList.add(tensor);
        final ModelTensors modelTensors = new ModelTensors(mlModelTensorList);
        tensorsList.add(modelTensors);
        return new ModelTensorOutput(tensorsList);
    }
}
