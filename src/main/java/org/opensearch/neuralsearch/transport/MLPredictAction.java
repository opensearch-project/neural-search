/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.transport;

import org.opensearch.action.ActionType;
import org.opensearch.common.io.stream.Writeable;

public class MLPredictAction extends ActionType<MLPredictActionResponse> {

    public static final MLPredictAction INSTANCE = new MLPredictAction();
    public static final String NAME = "cluster:admin/opensearch/neural-search/ml_predict_action";

    private MLPredictAction() {
        super(NAME, MLPredictActionResponse::new);
    }

    @Override
    public Writeable.Reader<MLPredictActionResponse> getResponseReader() {
        return MLPredictActionResponse::new;
    }

}
