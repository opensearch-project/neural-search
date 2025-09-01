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

public class NeuralSparseWarmupRequestTests extends AbstractSparseTestBase {

    public void testConstructorWithIndices() {
        String[] indices = { "index1", "index2", "index3" };
        NeuralSparseWarmupRequest request = new NeuralSparseWarmupRequest(indices);

        assertArrayEquals(indices, request.indices());
    }

    public void testConstructorWithSingleIndex() {
        String index = "test-index";
        NeuralSparseWarmupRequest request = new NeuralSparseWarmupRequest(index);

        assertArrayEquals(new String[] { index }, request.indices());
    }

    public void testConstructorWithEmptyIndices() {
        NeuralSparseWarmupRequest request = new NeuralSparseWarmupRequest();

        assertEquals(0, request.indices().length);
    }

    public void testConstructorWithNullIndices() {
        NeuralSparseWarmupRequest request = new NeuralSparseWarmupRequest((String[]) null);

        assertArrayEquals(null, request.indices());
    }

    public void testStreamConstructor() throws IOException {
        String[] originalIndices = { "index1", "index2" };
        NeuralSparseWarmupRequest originalRequest = new NeuralSparseWarmupRequest(originalIndices);
        originalRequest.indicesOptions(IndicesOptions.strictExpandOpen());

        // Serialize
        BytesStreamOutput out = new BytesStreamOutput();
        originalRequest.writeTo(out);

        // Deserialize
        StreamInput in = out.bytes().streamInput();
        NeuralSparseWarmupRequest deserializedRequest = new NeuralSparseWarmupRequest(in);

        // Verify
        assertArrayEquals(originalIndices, deserializedRequest.indices());
        assertEquals(originalRequest.indicesOptions(), deserializedRequest.indicesOptions());
    }
}
