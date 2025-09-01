/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Neural-sparse Warmup Request. This request contains a list of indices for which warmup should be performed.
 */
public class NeuralSparseWarmupRequest extends BroadcastRequest<NeuralSparseWarmupRequest> {

    public NeuralSparseWarmupRequest(StreamInput in) throws IOException {
        super(in);
    }

    public NeuralSparseWarmupRequest(String... indices) {
        super(indices);
    }
}
