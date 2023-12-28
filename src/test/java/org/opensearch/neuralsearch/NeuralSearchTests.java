/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
