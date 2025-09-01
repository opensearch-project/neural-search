/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContentObject;

import java.io.IOException;
import java.util.List;

/**
 * {@link NeuralSparseClearCacheResponse} represents Response returned by {@link NeuralSparseClearCacheRequest}.
 * Returns total number of shards on which ClearCache was performed on, as well as
 * the number of shards that succeeded and the number of shards that failed.
 */
public class NeuralSparseClearCacheResponse extends BroadcastResponse implements ToXContentObject {
    /**
     * Constructor
     *
     * @param in input stream
     * @throws IOException if read from stream fails
     */
    public NeuralSparseClearCacheResponse(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Constructor
     *
     * @param totalShards total number of shards on which ClearCache was performed
     * @param successfulShards number of shards that succeeded
     * @param failedShards number of shards that failed
     * @param shardFailures list of shard failure exceptions
     */
    public NeuralSparseClearCacheResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
    }
}
