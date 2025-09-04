/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.opensearch.action.ActionType;
import org.opensearch.core.common.io.stream.Writeable;

/**
 * Action associated with neural-sparse warmup
 */
public class NeuralSparseWarmupAction extends ActionType<NeuralSparseWarmupResponse> {
    public static final NeuralSparseWarmupAction INSTANCE = new NeuralSparseWarmupAction();
    public static final String NAME = "cluster:admin/neural_sparse_warmup_action";

    private NeuralSparseWarmupAction() {
        super(NAME, NeuralSparseWarmupResponse::new);
    }

    @Override
    public Writeable.Reader<NeuralSparseWarmupResponse> getResponseReader() {
        return NeuralSparseWarmupResponse::new;
    }
}
