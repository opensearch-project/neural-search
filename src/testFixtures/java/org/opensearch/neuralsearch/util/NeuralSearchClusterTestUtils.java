/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.HYBRID_FUSION_ENABLED;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.MAX_FUSION_LEG_SEARCHES;

import java.util.Set;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
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
        stubFusedModeEnabled(clusterService);
        return clusterService;
    }

    /**
     * Stub the cluster settings a fused hybrid rewrite reads, with fused mode turned on explicitly. A mock whose cluster
     * settings cannot be read resolves the setting's default, which is on, so this is not what lets the rewrite run; it
     * pins these tests to the fused path on their own terms, so a change to the default cannot flip what they assert
     * without a change here.
     *
     * <p>Both fused settings have to be registered on the returned {@link ClusterSettings}, because a
     * {@code ClusterSettings} built from a narrower set throws on a lookup of anything outside it.
     */
    public static void stubFusedModeEnabled(final ClusterService clusterService) {
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                Settings.builder().put(HYBRID_FUSION_ENABLED.getKey(), true).build(),
                Set.of(HYBRID_FUSION_ENABLED, MAX_FUSION_LEG_SEARCHES)
            )
        );
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
