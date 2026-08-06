/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils.setUpClusterService;

import java.util.List;
import java.util.Map;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class FusionConfigResolverTests extends OpenSearchTestCase {

    private SearchRequest requestWithInlinePipeline(Map<String, Object> pipelineSource) {
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.searchPipelineSource(pipelineSource);
        return new SearchRequest("test-index").source(source);
    }

    public void testResolve_whenInlineAndNamedBothPresent_thenThrows() {
        SearchRequest request = requestWithInlinePipeline(Map.of("phase_results_processors", List.of()));
        request.pipeline("my-pipeline");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> FusionConfigResolver.resolve(request));
        assertTrue(e.getMessage().contains("Both named and inline search pipeline"));
    }

    public void testResolve_whenInlineBodyWithFusionProcessor_thenParsed() {
        Map<String, Object> pipelineSource = Map.of(
            "phase_results_processors",
            List.of(Map.of("normalization-processor", Map.of("normalization", Map.of("technique", "min_max"))))
        );
        FusionSpec spec = FusionConfigResolver.resolve(requestWithInlinePipeline(pipelineSource));
        assertNotNull(spec);
        assertEquals(FusionSpec.NORMALIZATION_MIN_MAX, spec.normalizationTechnique());
    }

    public void testResolve_whenInlineBodyHasNoFusionProcessor_thenNull() {
        SearchRequest request = requestWithInlinePipeline(Map.of("phase_results_processors", List.of()));
        assertNull(FusionConfigResolver.resolve(request));
    }

    public void testResolve_whenNamedNonePipeline_thenNull() {
        setUpClusterService();
        SearchRequest request = new SearchRequest("test-index");
        request.pipeline("_none");
        assertNull(FusionConfigResolver.resolve(request));
    }
}
