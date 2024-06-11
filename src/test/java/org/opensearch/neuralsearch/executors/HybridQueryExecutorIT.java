/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.executors;

import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.neuralsearch.OpenSearchSecureRestTestCase;
import org.opensearch.rest.RestRequest;

import java.io.IOException;

import static org.opensearch.neuralsearch.executors.HybridQueryExecutor.getThreadPoolName;

public class HybridQueryExecutorIT extends OpenSearchSecureRestTestCase {

    public void testHybridQueryExecutorThreadIsInitialized() throws IOException, ParseException {
        final Request request = new Request(RestRequest.Method.GET.name(), String.join("/", "_cat", "thread_pool", getThreadPoolName()));
        final Response response = client().performRequest(request);
        assertOK(response);

        final String responseBody = EntityUtils.toString(response.getEntity());
        Assert.assertNotNull(responseBody);
        Assert.assertTrue(responseBody.contains(getThreadPoolName()));
    }
}
