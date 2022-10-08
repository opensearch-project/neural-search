/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.transport;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action to do call inference/predict api of ML Client.
 */
public class MLPredictTransportAction extends HandledTransportAction<MLPredictActionRequest, MLPredictActionResponse> {

    private final MLCommonsClientAccessor clientAccessor;

    @Inject
    public MLPredictTransportAction(
        final TransportService transportService,
        final ActionFilters filters,
        final MLCommonsClientAccessor clientAccessor
    ) {
        super(MLPredictAction.NAME, transportService, filters, MLPredictActionRequest::new);
        this.clientAccessor = clientAccessor;
    }

    @Override
    protected void doExecute(
        final Task task,
        final MLPredictActionRequest request,
        final ActionListener<MLPredictActionResponse> actionListener
    ) {
        clientAccessor.inferenceSentences(
            request.getModelId(),
            request.getInputSentencesList(),
            ActionListener.wrap(
                inferenceResponse -> actionListener.onResponse(new MLPredictActionResponse(inferenceResponse)),
                actionListener::onFailure
            )
        );
    }
}
