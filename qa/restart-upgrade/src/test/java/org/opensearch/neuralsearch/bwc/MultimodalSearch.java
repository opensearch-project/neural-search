/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.bwc;


import static org.opensearch.neuralsearch.TestUtils.NODES_BWC_CLUSTER;

public class MultimodalSearch extends AbstractRestartUpgradeRestTestCase{

    public void testMultiModalSearch() throws Exception{
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);

        if (isRunningAgainstOldCluster()){

        }
    }

}
