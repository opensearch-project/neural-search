/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.lucene.search.Explanation;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.neuralsearch.processor.explain.CombinedExplanationDetails;
import org.opensearch.neuralsearch.processor.explain.ExplanationDetails;
import org.opensearch.neuralsearch.processor.explain.ExplanationPayload;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.neuralsearch.plugin.NeuralSearch.EXPLANATION_RESPONSE_KEY;
import static org.opensearch.neuralsearch.processor.explain.ExplanationPayload.PayloadType.NORMALIZATION_PROCESSOR;

/**
 * Processor to add explanation details to search response
 */
@Getter
@AllArgsConstructor
public class ExplanationResponseProcessor implements SearchResponseProcessor {

    public static final String TYPE = "explanation_response_processor";

    private final String description;
    private final String tag;
    private final boolean ignoreFailure;

    /**
     * Add explanation details to search response if it is present in request context
     */
    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) {
        return processResponse(request, response, null);
    }

    /**
     * Combines explanation from processor with search hits level explanations and adds it to search response
     */
    @Override
    public SearchResponse processResponse(
        final SearchRequest request,
        final SearchResponse response,
        final PipelineProcessingContext requestContext
    ) {
        if (Objects.isNull(requestContext)
            || (Objects.isNull(requestContext.getAttribute(EXPLANATION_RESPONSE_KEY)))
            || requestContext.getAttribute(EXPLANATION_RESPONSE_KEY) instanceof ExplanationPayload == false) {
            return response;
        }
        // Extract explanation payload from context
        ExplanationPayload explanationPayload = (ExplanationPayload) requestContext.getAttribute(EXPLANATION_RESPONSE_KEY);
        Map<ExplanationPayload.PayloadType, Object> explainPayload = explanationPayload.getExplainPayload();
        if (explainPayload.containsKey(NORMALIZATION_PROCESSOR)) {
            // for score normalization, processor level explanations will be sorted in scope of each shard,
            // and we are merging both into a single sorted list
            SearchHits searchHits = response.getHits();
            SearchHit[] searchHitsArray = searchHits.getHits();
            // create a map of searchShard and list of indexes of search hit objects in search hits array
            // the list will keep original order of sorting as per final search results
            Map<SearchShard, List<Integer>> searchHitsByShard = new HashMap<>();
            // we keep index for each shard, where index is a position in searchHitsByShard list
            Map<SearchShard, Integer> explainsByShardCount = new HashMap<>();
            // Build initial shard mappings
            for (int i = 0; i < searchHitsArray.length; i++) {
                SearchHit searchHit = searchHitsArray[i];
                SearchShardTarget searchShardTarget = searchHit.getShard();
                SearchShard searchShard = SearchShard.createSearchShard(searchShardTarget);
                searchHitsByShard.computeIfAbsent(searchShard, k -> new ArrayList<>()).add(i);
                explainsByShardCount.putIfAbsent(searchShard, -1);
            }
            // Process normalization details if available in correct format
            if (explainPayload.get(NORMALIZATION_PROCESSOR) instanceof Map<?, ?>) {
                @SuppressWarnings("unchecked")
                Map<SearchShard, List<CombinedExplanationDetails>> combinedExplainDetails = (Map<
                    SearchShard,
                    List<CombinedExplanationDetails>>) explainPayload.get(NORMALIZATION_PROCESSOR);
                // Process each search hit to add processor level explanations
                for (SearchHit searchHit : searchHitsArray) {
                    SearchShard searchShard = SearchShard.createSearchShard(searchHit.getShard());
                    int explanationIndexByShard = explainsByShardCount.get(searchShard) + 1;
                    CombinedExplanationDetails combinedExplainDetail = combinedExplainDetails.get(searchShard).get(explanationIndexByShard);
                    // Extract various explanation components
                    Explanation queryLevelExplanation = searchHit.getExplanation();
                    ExplanationDetails normalizationExplanation = combinedExplainDetail.getNormalizationExplanations();
                    ExplanationDetails combinationExplanation = combinedExplainDetail.getCombinationExplanations();
                    // Create normalized explanations for each detail
                    Explanation[] normalizedExplanation = new Explanation[queryLevelExplanation.getDetails().length];
                    for (int i = 0; i < queryLevelExplanation.getDetails().length; i++) {
                        normalizedExplanation[i] = Explanation.match(
                            // normalized score
                            normalizationExplanation.getScoreDetails().get(i).getKey(),
                            // description of normalized score
                            normalizationExplanation.getScoreDetails().get(i).getValue(),
                            // shard level details
                            queryLevelExplanation.getDetails()[i]
                        );
                    }
                    // Create and set final explanation combining all components
                    Explanation finalExplanation = Explanation.match(
                        searchHit.getScore(),
                        // combination level explanation is always a single detail
                        combinationExplanation.getScoreDetails().get(0).getValue(),
                        normalizedExplanation
                    );
                    searchHit.explanation(finalExplanation);
                    explainsByShardCount.put(searchShard, explanationIndexByShard);
                }
            }
        }
        return response;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
