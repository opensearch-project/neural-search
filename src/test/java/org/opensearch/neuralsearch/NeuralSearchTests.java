/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch;

import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.index.util.KNNEngine;
import org.opensearch.test.OpenSearchTestCase;

public class NeuralSearchTests extends OpenSearchTestCase {

    public void testValidateKNNDependency() {
        assertEquals(KNNConstants.LUCENE_NAME, KNNEngine.LUCENE.getName());
    }
}
