/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils.mockClusterService;

import org.opensearch.Version;
import org.opensearch.action.IndicesRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.index.Index;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class NeuralSearchClusterUtilTests extends OpenSearchTestCase {

    public void testMinNodeVersion_whenSingleNodeCluster_thenSuccess() {
        ClusterService clusterService = mockClusterService(Version.V_2_4_0);

        final NeuralSearchClusterUtil neuralSearchClusterUtil = NeuralSearchClusterUtil.instance();
        final IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        neuralSearchClusterUtil.initialize(clusterService, indexNameExpressionResolver);

        final Version minVersion = neuralSearchClusterUtil.getClusterMinVersion();

        assertTrue(Version.V_2_4_0.equals(minVersion));
    }

    public void testMinNodeVersion_whenMultipleNodesCluster_thenSuccess() {
        ClusterService clusterService = mockClusterService(Version.V_2_3_0);

        final NeuralSearchClusterUtil neuralSearchClusterUtil = NeuralSearchClusterUtil.instance();
        final IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        neuralSearchClusterUtil.initialize(clusterService, indexNameExpressionResolver);

        final Version minVersion = neuralSearchClusterUtil.getClusterMinVersion();

        assertTrue(Version.V_2_3_0.equals(minVersion));
    }

    public void testGetIndexMetadataList() {
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterState clusterState = mock(ClusterState.class);
        final Metadata metadata = mock(Metadata.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);

        final IndicesRequest searchRequest = mock(IndicesRequest.class);
        final IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        final NeuralSearchClusterUtil neuralSearchClusterUtil = NeuralSearchClusterUtil.instance();
        neuralSearchClusterUtil.initialize(clusterService, indexNameExpressionResolver);

        final Index index1 = mock(Index.class);
        final Index index2 = mock(Index.class);
        final IndexMetadata indexMetadata1 = mock(IndexMetadata.class);
        final IndexMetadata indexMetadata2 = mock(IndexMetadata.class);

        when(indexNameExpressionResolver.concreteIndices(any(ClusterState.class), any(IndicesRequest.class))).thenReturn(
            List.of(index1, index2).toArray(new Index[0])
        );
        when(metadata.index(index1)).thenReturn(indexMetadata1);
        when(metadata.index(index2)).thenReturn(indexMetadata2);

        final List<IndexMetadata> indexMetadataList = neuralSearchClusterUtil.getIndexMetadataList(searchRequest);

        assertEquals(indexMetadata1, indexMetadataList.get(0));
        assertEquals(indexMetadata2, indexMetadataList.get(1));
    }

    public void testGetIndexMappingWithMultipleIndices() throws Exception {
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterState clusterState = mock(ClusterState.class);
        final Metadata metadata = mock(Metadata.class);
        final IndexMetadata indexMetadata1 = mock(IndexMetadata.class);
        final IndexMetadata indexMetadata2 = mock(IndexMetadata.class);
        final MappingMetadata mappingMetadata1 = mock(MappingMetadata.class);
        final MappingMetadata mappingMetadata2 = mock(MappingMetadata.class);
        final CompressedXContent mappingSource1 = new CompressedXContent("{\"properties\":{\"field1\":{\"type\":\"text\"}}}");
        final CompressedXContent mappingSource2 = new CompressedXContent("{\"properties\":{\"field2\":{\"type\":\"keyword\"}}}");

        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index("test-index1")).thenReturn(indexMetadata1);
        when(metadata.index("test-index2")).thenReturn(indexMetadata2);
        when(indexMetadata1.mapping()).thenReturn(mappingMetadata1);
        when(indexMetadata2.mapping()).thenReturn(mappingMetadata2);
        when(mappingMetadata1.source()).thenReturn(mappingSource1);
        when(mappingMetadata2.source()).thenReturn(mappingSource2);

        final NeuralSearchClusterUtil neuralSearchClusterUtil = NeuralSearchClusterUtil.instance();
        final IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        neuralSearchClusterUtil.initialize(clusterService, indexNameExpressionResolver);

        Map<String, String> result = neuralSearchClusterUtil.getIndexMapping(new String[] { "test-index1", "test-index2" });

        assertEquals(2, result.size());
        assertEquals("{\"properties\":{\"field1\":{\"type\":\"text\"}}}", result.get("test-index1"));
        assertEquals("{\"properties\":{\"field2\":{\"type\":\"keyword\"}}}", result.get("test-index2"));
    }

    public void testGetIndexMappingWithSingleIndex() throws Exception {
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterState clusterState = mock(ClusterState.class);
        final Metadata metadata = mock(Metadata.class);
        final IndexMetadata indexMetadata = mock(IndexMetadata.class);
        final MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        final CompressedXContent mappingSource = new CompressedXContent("{\"properties\":{\"title\":{\"type\":\"text\"}}}");

        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index("single-index")).thenReturn(indexMetadata);
        when(indexMetadata.mapping()).thenReturn(mappingMetadata);
        when(mappingMetadata.source()).thenReturn(mappingSource);

        final NeuralSearchClusterUtil neuralSearchClusterUtil = NeuralSearchClusterUtil.instance();
        final IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        neuralSearchClusterUtil.initialize(clusterService, indexNameExpressionResolver);

        Map<String, String> result = neuralSearchClusterUtil.getIndexMapping(new String[] { "single-index" });

        assertEquals(1, result.size());
        assertEquals("{\"properties\":{\"title\":{\"type\":\"text\"}}}", result.get("single-index"));
    }

    public void testGetIndexMapping_whenNoIndices_thenThrowException() {
        final ClusterService clusterService = mock(ClusterService.class);

        final NeuralSearchClusterUtil neuralSearchClusterUtil = NeuralSearchClusterUtil.instance();
        final IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        neuralSearchClusterUtil.initialize(clusterService, indexNameExpressionResolver);

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> neuralSearchClusterUtil.getIndexMapping(new String[] {})
        );
        assertEquals("No valid index found to extract mapping", exception.getMessage());
    }

    public void testGetClusterService_thenSuccess() {
        ClusterService clusterService = mockClusterService(Version.V_2_3_0);
        final IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        final NeuralSearchClusterUtil neuralSearchClusterUtil = NeuralSearchClusterUtil.instance();
        neuralSearchClusterUtil.initialize(clusterService, indexNameExpressionResolver);

        assertSame(clusterService, neuralSearchClusterUtil.getClusterService());
    }
}
