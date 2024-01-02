/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.bwc;

import static org.opensearch.neuralsearch.TestUtils.NODES_BWC_CLUSTER;

public class MultiModalSearchIT extends AbstractRollingUpgradeTestCase{
    private static final String PIPELINE_NAME = "nlp-ingest-pipeline";
    private static final String TEST_FIELD = "test-field";
    private static final String TEXT = "Hello world";
    private static final String TEXT_MIXED = "Hello world mixed";
    private static final String TEXT_UPGRADED = "Hello world upgraded";
    private static final int NUM_DOCS_PER_ROUND = 1;
    public void testMultiModalSearch_E2EFlow() throws Exception{
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
    }
}
