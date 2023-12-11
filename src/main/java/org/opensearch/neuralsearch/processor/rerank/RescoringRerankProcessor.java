/*
 * Copyright 2023 Aryn
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.neuralsearch.processor.rerank;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.profile.SearchProfileShardResults;

/**
 * RerankProcessor that rescores all the documents and re-sorts them using the new scores
 */
public abstract class RescoringRerankProcessor extends RerankProcessor {

    /**
     * Constructor. pass through to RerankProcessor ctor.
     * @param type
     * @param description
     * @param tag
     * @param ignoreFailure
     * @param contextSourceFetchers
     */
    public RescoringRerankProcessor(
        RerankType type,
        String description,
        String tag,
        boolean ignoreFailure,
        List<ContextSourceFetcher> contextSourceFetchers
    ) {
        super(type, description, tag, ignoreFailure, contextSourceFetchers);
    }

    /**
     * Generate a list of new scores for all of the documents, given the scoring context
     * @param response search results to rescore
     * @param rerankingContext extra information needed to score the search results; e.g. model id
     * @param listener be async. recieves the list of new scores
     */
    public abstract void rescoreSearchResponse(
        SearchResponse response,
        Map<String, Object> rerankingContext,
        ActionListener<List<Float>> listener
    );

    @Override
    public void rerank(SearchResponse searchResponse, Map<String, Object> rerankingContext, ActionListener<SearchResponse> listener) {
        try {
            rescoreSearchResponse(searchResponse, rerankingContext, ActionListener.wrap(scores -> {
                // Assign new scores
                SearchHit[] hits = searchResponse.getHits().getHits();
                if (hits.length != scores.size()) {
                    throw new Exception("scores and hits are not the same length");
                }
                for (int i = 0; i < hits.length; i++) {
                    hits[i].score(scores.get(i));
                }
                // Re-sort by the new scores
                Collections.sort(Arrays.asList(hits), new Comparator<SearchHit>() {
                    @Override
                    public int compare(SearchHit hit1, SearchHit hit2) {
                        // backwards to sort DESC
                        return Float.compare(hit2.getScore(), hit1.getScore());
                    }
                });
                // Reconstruct the search response, replacing the max score
                SearchHits newHits = new SearchHits(
                    hits,
                    searchResponse.getHits().getTotalHits(),
                    hits[0].getScore(),
                    searchResponse.getHits().getSortFields(),
                    searchResponse.getHits().getCollapseField(),
                    searchResponse.getHits().getCollapseValues()
                );
                SearchResponseSections newInternalResponse = new SearchResponseSections(
                    newHits,
                    searchResponse.getAggregations(),
                    searchResponse.getSuggest(),
                    searchResponse.isTimedOut(),
                    searchResponse.isTerminatedEarly(),
                    new SearchProfileShardResults(searchResponse.getProfileResults()),
                    searchResponse.getNumReducePhases(),
                    searchResponse.getInternalResponse().getSearchExtBuilders()
                );
                SearchResponse newResponse = new SearchResponse(
                    newInternalResponse,
                    searchResponse.getScrollId(),
                    searchResponse.getTotalShards(),
                    searchResponse.getSuccessfulShards(),
                    searchResponse.getSkippedShards(),
                    searchResponse.getTook().millis(),
                    searchResponse.getPhaseTook(),
                    searchResponse.getShardFailures(),
                    searchResponse.getClusters(),
                    searchResponse.pointInTimeId()
                );
                listener.onResponse(newResponse);
            }, e -> { listener.onFailure(e); }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}