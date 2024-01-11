/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.rerank.context;

import java.util.Map;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;

/**
 * Interface that gets context from some source and puts it in a map
 * for a reranking processor to use
 */
public interface ContextSourceFetcher {

    /**
     * Fetch the information needed in order to rerank.
     * That could be as simple as grabbing a field from the search request or
     * as complicated as a lookup to some external service
     * @param searchRequest the search query
     * @param searchResponse the search results, in case they're relevant
     * @param listener be async
     */
    public void fetchContext(final SearchRequest searchRequest, final SearchResponse searchResponse, final ActionListener<Map<String, Object>> listener);

    /**
     * Get the name of the contextSourceFetcher. This will be used as the field
     * name in the context config for the pipeline
     * @return Name of the fetcher
     */
    public String getName();

}
