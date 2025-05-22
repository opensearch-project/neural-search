/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch;

import org.opensearch.client.Request;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.Locale;

public class NeuralSearchRestTestCase extends OpenSearchRestTestCase {
    protected String createNormalizationPipeline(String normalizationTechnique, String combinationTechnique) throws IOException {
        final String pipelineRequestBody = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("phase_results_processors")
            .startObject()
            .startObject("normalization-processor")
            .startObject("normalization")
            .field("technique", normalizationTechnique)
            .endObject()
            .startObject("combination")
            .field("technique", combinationTechnique)
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .endObject()
            .toString();
        String pipelineName = String.format(Locale.ROOT, "%s-%s-pipeline", normalizationTechnique, combinationTechnique);
        final Request pipelineRequest = new Request(
            RestRequest.Method.PUT.name(),
            String.format(Locale.ROOT, "_search/pipeline/%s", pipelineName)
        );
        pipelineRequest.setJsonEntity(pipelineRequestBody);
        assertOK(client().performRequest(pipelineRequest));
        return pipelineName;
    }
}
