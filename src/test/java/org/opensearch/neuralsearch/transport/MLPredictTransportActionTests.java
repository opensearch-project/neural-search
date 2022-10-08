/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.transport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.neuralsearch.constants.TestCommonConstants;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

public class MLPredictTransportActionTests extends OpenSearchTestCase {

    @Mock
    private MLCommonsClientAccessor mlCommonsClientAccessor;
    @Mock
    private ActionListener<MLPredictActionResponse> mlPredictActionResponseActionListener;
    @Mock
    private Task task;
    // This variable is required for construction of TransportAction
    @Mock
    private TransportService transportService;
    // This variable is required for construction of TransportAction
    @Mock
    private ActionFilters actionFilters;
    @InjectMocks
    private MLPredictTransportAction transportAction;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    public void testDoExecute_whenValidInput_thenSuccess() {
        final MLPredictActionRequest request = new MLPredictActionRequest(TestCommonConstants.MODEL_ID, TestCommonConstants.SENTENCES_LIST);

        final List<List<Float>> vectorList = new ArrayList<>();
        vectorList.add(Arrays.asList(TestCommonConstants.PREDICT_VECTOR_ARRAY));

        final MLPredictActionResponse response = new MLPredictActionResponse(vectorList);
        Mockito.doAnswer(invocation -> {
            final ActionListener<List<List<Float>>> actionListener = invocation.getArgument(2);
            actionListener.onResponse(vectorList);
            return null;
        })
            .when(mlCommonsClientAccessor)
            .inferenceSentences(
                Mockito.eq(TestCommonConstants.MODEL_ID),
                Mockito.eq(TestCommonConstants.SENTENCES_LIST),
                Mockito.isA(ActionListener.class)
            );

        transportAction.doExecute(task, request, mlPredictActionResponseActionListener);

        Mockito.verify(mlCommonsClientAccessor)
            .inferenceSentences(
                Mockito.eq(TestCommonConstants.MODEL_ID),
                Mockito.eq(TestCommonConstants.SENTENCES_LIST),
                Mockito.isA(ActionListener.class)
            );

        Mockito.verify(mlPredictActionResponseActionListener).onResponse(response);
    }
}
