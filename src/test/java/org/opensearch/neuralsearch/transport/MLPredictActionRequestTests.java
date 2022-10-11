/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.transport;

import java.util.Collections;

import lombok.SneakyThrows;

import org.junit.Assert;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.neuralsearch.constants.TestCommonConstants;
import org.opensearch.test.OpenSearchTestCase;

public class MLPredictActionRequestTests extends OpenSearchTestCase {

    @SneakyThrows
    public void testStreams_whenValidInput_thenSuccess() {
        final MLPredictActionRequest request = new MLPredictActionRequest(TestCommonConstants.MODEL_ID, TestCommonConstants.SENTENCES_LIST);
        final MLPredictActionRequest differentObject = new MLPredictActionRequest(TestCommonConstants.MODEL_ID, Collections.emptyList());
        final BytesStreamOutput streamOutput = new BytesStreamOutput();
        request.writeTo(streamOutput);
        final MLPredictActionRequest mlPredictActionRequestDuplicate = new MLPredictActionRequest(streamOutput.bytes().streamInput());
        Assert.assertEquals(request, mlPredictActionRequestDuplicate);
        Assert.assertNotEquals(differentObject, mlPredictActionRequestDuplicate);
    }

    public void testValidateForAllCases() {
        final MLPredictActionRequest validRequest = new MLPredictActionRequest(
            TestCommonConstants.MODEL_ID,
            TestCommonConstants.SENTENCES_LIST
        );
        Assert.assertNull(validRequest.validate());
        final MLPredictActionRequest inValidRequest = new MLPredictActionRequest(null, TestCommonConstants.SENTENCES_LIST);
        Assert.assertNotNull(inValidRequest.validate());

        final MLPredictActionRequest inValidRequestWithEmptySentenceList = new MLPredictActionRequest(
            TestCommonConstants.MODEL_ID,
            Collections.emptyList()
        );
        Assert.assertNotNull(inValidRequestWithEmptySentenceList.validate());
    }
}
