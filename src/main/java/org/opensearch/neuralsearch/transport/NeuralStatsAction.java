/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.opensearch.action.ActionType;
import org.opensearch.core.common.io.stream.Writeable;

/**
 * NeuralStatsAction class
 */
public class NeuralStatsAction extends ActionType<NeuralStatsResponse> {

    public static final NeuralStatsAction INSTANCE = new NeuralStatsAction();
    public static final String NAME = "cluster:admin/neural_stats_action";

    /**
     * Constructor
     */
    private NeuralStatsAction() {
        super(NAME, NeuralStatsResponse::new);
    }

    @Override
    public Writeable.Reader<NeuralStatsResponse> getResponseReader() {
        return NeuralStatsResponse::new;
    }
}
