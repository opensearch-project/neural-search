/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

public class NeuralSparseWarmupActionTests extends AbstractSparseTestBase {

    public void testInstance() {
        assertNotNull(NeuralSparseWarmupAction.INSTANCE);
    }

    public void testName() {
        assertEquals("cluster:admin/neural_sparse_warmup_action", NeuralSparseWarmupAction.NAME);
        assertEquals(NeuralSparseWarmupAction.NAME, NeuralSparseWarmupAction.INSTANCE.name());
    }

    public void testGetResponseReader() {
        Writeable.Reader<NeuralSparseWarmupResponse> reader = NeuralSparseWarmupAction.INSTANCE.getResponseReader();
        assertNotNull(reader);
    }
}
