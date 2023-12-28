/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.neuralsearch.util;

import static org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils.mockClusterService;

import org.opensearch.Version;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchTestCase;

public class NeuralSearchClusterUtilTests extends OpenSearchTestCase {

    public void testMinNodeVersion_whenSingleNodeCluster_thenSuccess() {
        ClusterService clusterService = mockClusterService(Version.V_2_4_0);

        final NeuralSearchClusterUtil neuralSearchClusterUtil = NeuralSearchClusterUtil.instance();
        neuralSearchClusterUtil.initialize(clusterService);

        final Version minVersion = neuralSearchClusterUtil.getClusterMinVersion();

        assertTrue(Version.V_2_4_0.equals(minVersion));
    }

    public void testMinNodeVersion_whenMultipleNodesCluster_thenSuccess() {
        ClusterService clusterService = mockClusterService(Version.V_2_3_0);

        final NeuralSearchClusterUtil neuralSearchClusterUtil = NeuralSearchClusterUtil.instance();
        neuralSearchClusterUtil.initialize(clusterService);

        final Version minVersion = neuralSearchClusterUtil.getClusterMinVersion();

        assertTrue(Version.V_2_3_0.equals(minVersion));
    }
}
