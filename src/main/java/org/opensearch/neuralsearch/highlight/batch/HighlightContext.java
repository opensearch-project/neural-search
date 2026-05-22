/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.batch;

import java.util.List;

import lombok.Builder;
import lombok.Getter;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;
import org.opensearch.search.SearchHit;

/**
 * Inputs to one batch execution. All per-entry lists are aligned by index: entry
 * {@code i} means "send {@code requests[i]} to the model, then write the result
 * onto {@code validHits[i]} under key {@code fieldNames[i]} using
 * {@code preTags[i]}/{@code postTags[i]}, honoring {@code noMatchSizes[i]} and
 * {@code encoders[i]}."
 */
@Getter
@Builder(toBuilder = true)
public class HighlightContext {

    private final List<SentenceHighlightingRequest> requests;

    private final List<SearchHit> validHits;

    private final List<String> fieldNames;

    private final List<String> preTags;

    private final List<String> postTags;

    private final List<Integer> noMatchSizes;

    private final List<String> encoders;

    private final SearchResponse originalResponse;

    private final long startTime;

    private final String modelId;

    private final FunctionName modelType;

    private final int maxBatchSize;

    /**
     * @return true when there are no inference rows to send to the model
     */
    public boolean isEmpty() {
        return requests == null || requests.isEmpty();
    }

    /**
     * @return number of inference rows in this context
     */
    public int size() {
        return requests != null ? requests.size() : 0;
    }
}
