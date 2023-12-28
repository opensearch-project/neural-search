/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch;

import java.io.IOException;

import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.rest.RestRequest;

public class NeuralSearchIT extends OpenSearchSecureRestTestCase {
    private static final String NEURAL_SEARCH_PLUGIN_NAME = "neural-search";

    public void testNeuralSearchPluginInstalled() throws IOException, ParseException {
        final Request request = new Request(RestRequest.Method.GET.name(), String.join("/", "_cat", "plugins"));
        final Response response = client().performRequest(request);
        assertOK(response);

        final String responseBody = EntityUtils.toString(response.getEntity());
        Assert.assertNotNull(responseBody);
        Assert.assertTrue(responseBody.contains(NEURAL_SEARCH_PLUGIN_NAME));
    }
}
