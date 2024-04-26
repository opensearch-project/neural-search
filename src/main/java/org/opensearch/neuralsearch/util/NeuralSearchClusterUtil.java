/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import org.opensearch.Version;
import org.opensearch.cluster.service.ClusterService;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Class abstracts information related to underlying OpenSearch cluster
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Log4j2
public class NeuralSearchClusterUtil {
    private ClusterService clusterService;

    private static NeuralSearchClusterUtil instance;

    /**
     * Return instance of the cluster context, must be initialized first for proper usage
     * @return instance of cluster context
     */
    public static synchronized NeuralSearchClusterUtil instance() {
        if (instance == null) {
            instance = new NeuralSearchClusterUtil();
        }
        return instance;
    }

    /**
     * Initializes instance of cluster context by injecting dependencies
     * @param clusterService
     */
    public void initialize(final ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * Return minimal OpenSearch version based on all nodes currently discoverable in the cluster
     * @return minimal installed OpenSearch version, default to Version.CURRENT which is typically the latest version
     */
    public Version getClusterMinVersion() {
        return this.clusterService.state().getNodes().getMinNodeVersion();
    }

    /**
     * Return maximal OpenSearch version based on all nodes currently discoverable in the cluster
     * @return maximal installed OpenSearch version, default to Version.CURRENT which is typically the latest version
     */
    public Version getClusterMaxVersion() {
        return this.clusterService.state().getNodes().getMaxNodeVersion();
    }

}
