/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling.cleanup;

import org.opensearch.neuralsearch.bwc.rolling.test.AbstractRollingUpgradeTestCase;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;

public class ModelResourcesCleanUpIT extends AbstractRollingUpgradeTestCase {

    // Shared Model Resources CleanUp
    public void testCleanUp_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER, 90);

        final String sparseModelId = getSparseEncodingModelId();
        cleanUpModelId(sparseModelId);
        final String embeddingModelId = getTextEmbeddingModelId();
        cleanUpModelId(embeddingModelId);
    }

}
