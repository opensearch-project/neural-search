/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import lombok.Builder;
import lombok.Getter;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;
import org.opensearch.search.SearchHit;

import java.util.List;

/**
 * Context containing prepared highlighting requests and associated search hits
 */
@Getter
@Builder
public class HighlightContext {
    private final List<SentenceHighlightingRequest> requests;
    private final List<SearchHit> validHits;
    private final String fieldName;
    private final SearchResponse originalResponse;
    private final long startTime;
    private final String preTag;
    private final String postTag;

    /**
     * Check if there are any requests to process
     * @return true if there are no requests
     */
    public boolean isEmpty() {
        return requests == null || requests.isEmpty();
    }

    /**
     * Get the number of requests
     * @return number of highlighting requests
     */
    public int size() {
        return requests != null ? requests.size() : 0;
    }
}
