/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import lombok.Getter;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 *  NeuralStatsNodeRequest represents the request to an individual node
 */
public class NeuralStatsNodeRequest extends TransportRequest {
    @Getter
    private NeuralStatsRequest request;

    /**
     * Constructor
     */
    public NeuralStatsNodeRequest() {
        super();
    }

    /**
     * Constructor
     *
     * @param in input stream
     * @throws IOException in case of I/O errors
     */
    public NeuralStatsNodeRequest(StreamInput in) throws IOException {
        super(in);
        request = new NeuralStatsRequest(in);
    }

    /**
     * Constructor
     *
     * @param request NeuralStatsRequest
     */
    public NeuralStatsNodeRequest(NeuralStatsRequest request) {
        this.request = request;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }
}
