/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;

/**
 * Strategy interface for different highlighting approaches.
 * Allows for batch processing, single inference, and future extensions like nested highlighting.
 */
public interface HighlightingStrategy {

    /**
     * Process highlighting requests and apply results to search response
     *
     * @param context The prepared highlighting context with requests and hits
     * @param responseListener Listener for async response
     */
    void process(HighlightContext context, ActionListener<SearchResponse> responseListener);
}
