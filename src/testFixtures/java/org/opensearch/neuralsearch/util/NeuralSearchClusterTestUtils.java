/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;

public class NeuralSearchClusterTestUtils {

    /**
     * Create new mock for ClusterService
     * @param version min version for cluster nodes
     * @return
     */
    public static ClusterService mockClusterService(final Version version) {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);
        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(clusterState.getNodes()).thenReturn(discoveryNodes);
        when(discoveryNodes.getMinNodeVersion()).thenReturn(version);
        return clusterService;
    }

    /**
     * Set up a simple NeuralSearchClusterUtil instance with a specified version.
     */
    public static void setUpClusterService(Version version) {
        ClusterService clusterService = NeuralSearchClusterTestUtils.mockClusterService(version);
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        NeuralSearchClusterUtil.instance().initialize(clusterService, indexNameExpressionResolver);
    }

    /**
     * Set up a simple NeuralSearchClusterUtil instance with current version.
     */
    public static void setUpClusterService() {
        setUpClusterService(Version.CURRENT);
    }
}
