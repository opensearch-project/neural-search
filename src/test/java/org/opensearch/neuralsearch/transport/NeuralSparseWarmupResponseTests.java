/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NeuralSparseWarmupResponseTests extends AbstractSparseTestBase {

    public void testConstructorWithSuccessfulResponse() {
        int totalShards = 5;
        int successfulShards = 5;
        int failedShards = 0;
        List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();

        NeuralSparseWarmupResponse response = new NeuralSparseWarmupResponse(totalShards, successfulShards, failedShards, shardFailures);

        assertEquals(totalShards, response.getTotalShards());
        assertEquals(successfulShards, response.getSuccessfulShards());
        assertEquals(failedShards, response.getFailedShards());
        assertEquals(shardFailures, Arrays.asList(response.getShardFailures()));
    }

    public void testConstructorWithFailedShards() {
        int totalShards = 5;
        int successfulShards = 3;
        int failedShards = 2;

        List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();
        shardFailures.add(new DefaultShardOperationFailedException("test-index", 1, new RuntimeException("Shard failure 1")));
        shardFailures.add(new DefaultShardOperationFailedException("test-index", 2, new RuntimeException("Shard failure 2")));

        NeuralSparseWarmupResponse response = new NeuralSparseWarmupResponse(totalShards, successfulShards, failedShards, shardFailures);

        assertEquals(totalShards, response.getTotalShards());
        assertEquals(successfulShards, response.getSuccessfulShards());
        assertEquals(failedShards, response.getFailedShards());
        assertEquals(failedShards, response.getShardFailures().length);
    }

    public void testConstructorWithZeroShards() {
        int totalShards = 0;
        int successfulShards = 0;
        int failedShards = 0;
        List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();

        NeuralSparseWarmupResponse response = new NeuralSparseWarmupResponse(totalShards, successfulShards, failedShards, shardFailures);

        assertEquals(0, response.getTotalShards());
        assertEquals(0, response.getSuccessfulShards());
        assertEquals(0, response.getFailedShards());
        assertEquals(0, response.getShardFailures().length);
    }

    public void testStreamConstructor() throws IOException {
        int totalShards = 3;
        int successfulShards = 2;
        int failedShards = 1;

        List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();
        shardFailures.add(new DefaultShardOperationFailedException("test-index", 0, new RuntimeException("Test failure")));

        NeuralSparseWarmupResponse originalResponse = new NeuralSparseWarmupResponse(
            totalShards,
            successfulShards,
            failedShards,
            shardFailures
        );

        // Serialize
        BytesStreamOutput out = new BytesStreamOutput();
        originalResponse.writeTo(out);

        // Deserialize
        StreamInput in = out.bytes().streamInput();
        NeuralSparseWarmupResponse deserializedResponse = new NeuralSparseWarmupResponse(in);

        // Verify
        assertEquals(originalResponse.getTotalShards(), deserializedResponse.getTotalShards());
        assertEquals(originalResponse.getSuccessfulShards(), deserializedResponse.getSuccessfulShards());
        assertEquals(originalResponse.getFailedShards(), deserializedResponse.getFailedShards());
        assertEquals(originalResponse.getShardFailures().length, deserializedResponse.getShardFailures().length);
    }
}
