/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.restart.setup;

import org.opensearch.neuralsearch.bwc.restart.test.AbstractRestartUpgradeRestTestCase;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;

public class ModelResourcesSetUpIT extends AbstractRestartUpgradeRestTestCase {

    // Shared Model Resources SetUp
    public void testSetUp_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);

        uploadAndDeploySparseEncodingModel();
        uploadAndDeployTextEmbeddingModel();
    }

}
