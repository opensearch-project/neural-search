/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling.setup;

import org.opensearch.ml.common.model.MLModelState;
import org.opensearch.neuralsearch.bwc.rolling.test.AbstractRollingUpgradeTestCase;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;

public class ModelResourcesSetUpIT extends AbstractRollingUpgradeTestCase {

    // Shared Model Resources SetUp
    public void testSetUp_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER, 90);

        final String sparseModelId = uploadAndDeploySparseEncodingModel();
        MLModelState modelState = getModelState(sparseModelId);
        logger.info("Model state in OLD phase: {}", modelState);
        if (modelState != MLModelState.LOADED && modelState != MLModelState.DEPLOYED) {
            logger.error("Model {} is not in LOADED or DEPLOYED state in OLD phase. Current state: {}", sparseModelId, modelState);
            waitForModelToLoad(sparseModelId);
        }

        final String embeddingModelId = uploadAndDeployTextEmbeddingModel();
        modelState = getModelState(embeddingModelId);
        logger.info("Model state in OLD phase: {}", modelState);
        if (modelState != MLModelState.LOADED && modelState != MLModelState.DEPLOYED) {
            logger.error("Model {} is not in LOADED or DEPLOYED state in OLD phase. Current state: {}", sparseModelId, modelState);
            waitForModelToLoad(sparseModelId);
        }
    }

}
