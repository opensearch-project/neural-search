/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils.setUpClusterService;

import java.util.List;
import java.util.Map;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.PipelineConfiguration;
import org.opensearch.search.pipeline.SearchPipelineMetadata;
import org.opensearch.test.OpenSearchTestCase;

public class FusionConfigResolverTests extends OpenSearchTestCase {

    private static final String INDEX = "test-index";

    private SearchRequest requestWithInlinePipeline(Map<String, Object> pipelineSource) {
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.searchPipelineSource(pipelineSource);
        return new SearchRequest(INDEX).source(source);
    }

    /** A min_max normalization pipeline config, as {@code PipelineConfiguration.getConfigAsMap()} would return it. */
    private PipelineConfiguration minMaxPipeline(String id) throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("phase_results_processors")
            .startObject()
            .startObject("normalization-processor")
            .startObject("normalization")
            .field("technique", "min_max")
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .endObject();
        return new PipelineConfiguration(id, BytesReference.bytes(builder), MediaTypeRegistry.JSON);
    }

    /**
     * Wire the NeuralSearchClusterUtil singleton to a mocked cluster state carrying (optionally) a named pipeline in
     * SearchPipelineMetadata and (optionally) a concrete index whose settings declare index.search.default_pipeline.
     */
    private void initClusterUtil(SearchPipelineMetadata pipelineMetadata, Settings indexDefaultSettings) {
        Metadata metadata = mock(Metadata.class);
        ClusterState clusterState = mock(ClusterState.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterState.getMetadata()).thenReturn(metadata);
        when(metadata.custom(SearchPipelineMetadata.TYPE)).thenReturn(pipelineMetadata);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        Index concrete = new Index(INDEX, "uuid-1");
        when(resolver.concreteIndices(any(ClusterState.class), any(org.opensearch.action.IndicesRequest.class))).thenReturn(
            new Index[] { concrete }
        );
        if (indexDefaultSettings != null) {
            IndexMetadata indexMetadata = IndexMetadata.builder(INDEX).settings(indexDefaultSettings).build();
            when(metadata.index(concrete)).thenReturn(indexMetadata);
        } else {
            when(metadata.index(concrete)).thenReturn(null);
        }

        NeuralSearchClusterUtil.instance().initialize(clusterService, resolver);
    }

    private Settings indexSettingsWithDefaultPipeline(String pipelineId) {
        Settings.Builder b = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", org.opensearch.Version.CURRENT.id);
        if (pipelineId != null) {
            b.put("index.search.default_pipeline", pipelineId);
        }
        return b.build();
    }

    // ---- inline body + precedence (no cluster state needed) ----

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
        SearchRequest request = new SearchRequest(INDEX);
        request.pipeline("_none");
        assertNull(FusionConfigResolver.resolve(request));
    }

    // ---- named ?search_pipeline= resolved by id from cluster-state metadata ----

    public void testResolve_whenNamedPipelineById_thenParsedFromClusterState() throws Exception {
        String pipelineId = "my-norm-pipeline";
        SearchPipelineMetadata metadata = new SearchPipelineMetadata(Map.of(pipelineId, minMaxPipeline(pipelineId)));
        initClusterUtil(metadata, null);

        SearchRequest request = new SearchRequest(INDEX);
        request.pipeline(pipelineId);
        FusionSpec spec = FusionConfigResolver.resolve(request);
        assertNotNull(spec);
        assertEquals(FusionSpec.NORMALIZATION_MIN_MAX, spec.normalizationTechnique());
    }

    public void testResolve_whenNamedPipelineAbsentFromClusterState_thenNull() {
        initClusterUtil(new SearchPipelineMetadata(Map.of()), null);
        SearchRequest request = new SearchRequest(INDEX);
        request.pipeline("does-not-exist");
        assertNull(FusionConfigResolver.resolve(request));
    }

    // ---- index.search.default_pipeline ----

    public void testResolve_whenIndexDefaultPipeline_thenParsed() throws Exception {
        String pipelineId = "index-default-norm";
        SearchPipelineMetadata metadata = new SearchPipelineMetadata(Map.of(pipelineId, minMaxPipeline(pipelineId)));
        initClusterUtil(metadata, indexSettingsWithDefaultPipeline(pipelineId));

        FusionSpec spec = FusionConfigResolver.resolve(new SearchRequest(INDEX));
        assertNotNull(spec);
        assertEquals(FusionSpec.NORMALIZATION_MIN_MAX, spec.normalizationTechnique());
    }

    public void testResolve_whenNoInlineNoNamedNoIndexDefault_thenNull() {
        initClusterUtil(new SearchPipelineMetadata(Map.of()), indexSettingsWithDefaultPipeline(null));
        assertNull(FusionConfigResolver.resolve(new SearchRequest(INDEX)));
    }

    public void testResolve_whenIndexDefaultIsNone_thenNull() {
        initClusterUtil(new SearchPipelineMetadata(Map.of()), indexSettingsWithDefaultPipeline("_none"));
        assertNull(FusionConfigResolver.resolve(new SearchRequest(INDEX)));
    }

    /**
     * Two concrete indices with DIFFERENT index.search.default_pipeline settings collapse to _none → resolve returns
     * null (mirrors core's conflicting-defaults behavior). Also exercises the null-IndexMetadata skip.
     */
    public void testResolve_whenConflictingIndexDefaults_thenNull() {
        Metadata metadata = mock(Metadata.class);
        ClusterState clusterState = mock(ClusterState.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterState.getMetadata()).thenReturn(metadata);
        when(metadata.custom(SearchPipelineMetadata.TYPE)).thenReturn(new SearchPipelineMetadata(Map.of()));

        Index a = new Index("idx-a", "uuid-a");
        Index b = new Index("idx-b", "uuid-b");
        Index missing = new Index("idx-missing", "uuid-m");
        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(ClusterState.class), any(org.opensearch.action.IndicesRequest.class))).thenReturn(
            new Index[] { a, b, missing }
        );
        when(metadata.index(a)).thenReturn(IndexMetadata.builder("idx-a").settings(indexSettingsWithDefaultPipeline("pipe-a")).build());
        when(metadata.index(b)).thenReturn(IndexMetadata.builder("idx-b").settings(indexSettingsWithDefaultPipeline("pipe-b")).build());
        when(metadata.index(missing)).thenReturn(null); // null-IndexMetadata skip branch
        NeuralSearchClusterUtil.instance().initialize(clusterService, resolver);

        assertNull(FusionConfigResolver.resolve(new SearchRequest("idx-a", "idx-b")));
    }
}
