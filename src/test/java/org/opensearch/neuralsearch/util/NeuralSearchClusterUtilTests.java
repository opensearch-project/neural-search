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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.index.Index;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

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
}
