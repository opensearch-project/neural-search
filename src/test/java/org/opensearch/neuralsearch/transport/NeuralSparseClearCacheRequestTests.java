/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import java.io.IOException;

public class NeuralSparseClearCacheRequestTests extends AbstractSparseTestBase {

    public void testConstructorWithIndices() {
        String[] indices = { "index1", "index2", "index3" };
        NeuralSparseClearCacheRequest request = new NeuralSparseClearCacheRequest(indices);

        assertArrayEquals(indices, request.indices());
    }

    public void testConstructorWithSingleIndex() {
        String index = "test-index";
        NeuralSparseClearCacheRequest request = new NeuralSparseClearCacheRequest(index);

        assertArrayEquals(new String[] { index }, request.indices());
    }

    public void testConstructorWithEmptyIndices() {
        NeuralSparseClearCacheRequest request = new NeuralSparseClearCacheRequest();

        assertEquals(0, request.indices().length);
    }

    public void testConstructorWithNullIndices() {
        NeuralSparseClearCacheRequest request = new NeuralSparseClearCacheRequest((String[]) null);

        assertArrayEquals(null, request.indices());
    }

    public void testStreamConstructor() throws IOException {
        String[] originalIndices = { "index1", "index2" };
        NeuralSparseClearCacheRequest originalRequest = new NeuralSparseClearCacheRequest(originalIndices);
        originalRequest.indicesOptions(IndicesOptions.strictExpandOpen());

        // Serialize
        BytesStreamOutput out = new BytesStreamOutput();
        originalRequest.writeTo(out);

        // Deserialize
        StreamInput in = out.bytes().streamInput();
        NeuralSparseClearCacheRequest deserializedRequest = new NeuralSparseClearCacheRequest(in);

        // Verify
        assertArrayEquals(originalIndices, deserializedRequest.indices());
        assertEquals(originalRequest.indicesOptions(), deserializedRequest.indicesOptions());
    }
}
