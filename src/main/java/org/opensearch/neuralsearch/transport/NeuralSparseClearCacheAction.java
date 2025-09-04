/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.opensearch.action.ActionType;
import org.opensearch.core.common.io.stream.Writeable;

/**
 * Action associated with ClearCache
 */
public class NeuralSparseClearCacheAction extends ActionType<NeuralSparseClearCacheResponse> {
    public static final NeuralSparseClearCacheAction INSTANCE = new NeuralSparseClearCacheAction();
    public static final String NAME = "cluster:admin/neural_sparse_clear_cache_action";

    private NeuralSparseClearCacheAction() {
        super(NAME, NeuralSparseClearCacheResponse::new);
    }

    @Override
    public Writeable.Reader<NeuralSparseClearCacheResponse> getResponseReader() {
        return NeuralSparseClearCacheResponse::new;
    }
}
