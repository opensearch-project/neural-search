/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.transport;

import java.util.Collections;

import lombok.SneakyThrows;

import org.junit.Assert;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.neuralsearch.constants.TestCommonConstants;
import org.opensearch.test.OpenSearchTestCase;

public class MLPredictActionResponseTests extends OpenSearchTestCase {

    @SneakyThrows
    public void testStreams_whenValidInput_thenSuccess() {
        final MLPredictActionResponse response = new MLPredictActionResponse(TestCommonConstants.PREDICTIONS_LIST);
        final BytesStreamOutput streamOutput = new BytesStreamOutput();
        response.writeTo(streamOutput);
        final MLPredictActionResponse duplicateResponse = new MLPredictActionResponse(streamOutput.bytes().streamInput());
        Assert.assertEquals(response, duplicateResponse);
    }

    @SneakyThrows
    public void testStreams_whenPredictionListEmpty_thenSuccess() {
        final MLPredictActionResponse response = new MLPredictActionResponse(Collections.emptyList());
        final BytesStreamOutput streamOutput = new BytesStreamOutput();
        response.writeTo(streamOutput);
        final MLPredictActionResponse duplicateResponse = new MLPredictActionResponse(streamOutput.bytes().streamInput());
        Assert.assertEquals(response, duplicateResponse);
    }

    @SneakyThrows
    public void testToXContent_whenValidInput_thenSuccess() {
        final MLPredictActionResponse response = new MLPredictActionResponse(TestCommonConstants.PREDICTIONS_LIST);
        final String xContentString = "{\"inference_vectors\":[[2.0,3.0]]}";
        final XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        Assert.assertEquals(xContentString, Strings.toString(response.toXContent(xContentBuilder, null)));
    }

}
