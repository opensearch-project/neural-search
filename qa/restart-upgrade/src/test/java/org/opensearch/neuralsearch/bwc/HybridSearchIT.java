/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc;

import static org.opensearch.neuralsearch.TestUtils.NODES_BWC_CLUSTER;

public class HybridSearchIT extends AbstractRestartUpgradeRestTestCase {

    public void testHybridSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        if (isRunningAgainstOldCluster()) {

        } else {

        }
    }
}
