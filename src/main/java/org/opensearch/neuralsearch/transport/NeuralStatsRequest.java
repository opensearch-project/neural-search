/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import lombok.Getter;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.neuralsearch.stats.NeuralStatsInput;

import java.io.IOException;

/**
 * NeuralStatsRequest gets node (cluster) level Stats for Neural
 * By default, all parameters will be true
 */
public class NeuralStatsRequest extends BaseNodesRequest<NeuralStatsRequest> {

    /**
     * Key indicating all stats should be retrieved
     */
    @Getter
    private final NeuralStatsInput neuralStatsInput;

    /**
     * Empty constructor needed for NeuralStatsTransportAction
     */
    public NeuralStatsRequest() {
        super((String[]) null);
        this.neuralStatsInput = new NeuralStatsInput();
    }

    /**
     * Constructor
     *
     * @param in input stream
     * @throws IOException in case of I/O errors
     */
    public NeuralStatsRequest(StreamInput in) throws IOException {
        super(in);
        this.neuralStatsInput = new NeuralStatsInput(in);
    }

    /**
     * Constructor
     *
     * @param nodeIds NodeIDs from which to retrieve stats
     */
    public NeuralStatsRequest(String[] nodeIds, NeuralStatsInput neuralStatsInput) {
        super(nodeIds);
        this.neuralStatsInput = neuralStatsInput;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        neuralStatsInput.writeTo(out);
    }
}
