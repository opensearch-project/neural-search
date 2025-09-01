/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

public class NeuralSparseClearCacheActionTests extends AbstractSparseTestBase {

    public void testInstance() {
        assertNotNull(NeuralSparseClearCacheAction.INSTANCE);
    }

    public void testName() {
        assertEquals("cluster:admin/neural_sparse_clear_cache_action", NeuralSparseClearCacheAction.NAME);
        assertEquals(NeuralSparseClearCacheAction.NAME, NeuralSparseClearCacheAction.INSTANCE.name());
    }

    public void testGetResponseReader() {
        Writeable.Reader<NeuralSparseClearCacheResponse> reader = NeuralSparseClearCacheAction.INSTANCE.getResponseReader();
        assertNotNull(reader);
    }
}
