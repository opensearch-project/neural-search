/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils.mockClusterService;

import org.opensearch.Version;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchTestCase;

public class NeuralSearchClusterUtilTests extends OpenSearchTestCase {

    public void testSingleNodeCluster() {
        ClusterService clusterService = mockClusterService(Version.V_2_4_0);

        final NeuralSearchClusterUtil neuralSearchClusterUtil = NeuralSearchClusterUtil.instance();
        neuralSearchClusterUtil.initialize(clusterService);

        final Version minVersion = neuralSearchClusterUtil.getClusterMinVersion();

        assertTrue(Version.V_2_4_0.equals(minVersion));
    }

    public void testMultipleNodesCluster() {
        ClusterService clusterService = mockClusterService(Version.V_2_3_0);

        final NeuralSearchClusterUtil neuralSearchClusterUtil = NeuralSearchClusterUtil.instance();
        neuralSearchClusterUtil.initialize(clusterService);

        final Version minVersion = neuralSearchClusterUtil.getClusterMinVersion();

        assertTrue(Version.V_2_3_0.equals(minVersion));
    }

    public void testWhenErrorOnClusterStateDiscover() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenThrow(new RuntimeException("Cluster state is not ready"));

        final NeuralSearchClusterUtil neuralSearchClusterUtil = NeuralSearchClusterUtil.instance();
        neuralSearchClusterUtil.initialize(clusterService);

        final Version minVersion = neuralSearchClusterUtil.getClusterMinVersion();

        assertTrue(Version.CURRENT.equals(minVersion));
    }
}
