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
import org.opensearch.neuralsearch.processor.explain.CombinedExplainDetails;
import org.opensearch.neuralsearch.processor.explain.ProcessorExplainDto;
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

import static org.opensearch.neuralsearch.plugin.NeuralSearch.PROCESSOR_EXPLAIN;

@Getter
@AllArgsConstructor
public class ProcessorExplainPublisher implements SearchResponseProcessor {

    public static final String TYPE = "processor_explain_publisher";

    private final String description;
    private final String tag;
    private final boolean ignoreFailure;

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception {
        return processResponse(request, response, null);
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response, PipelineProcessingContext requestContext) {
        if (Objects.isNull(requestContext.getAttribute(PROCESSOR_EXPLAIN))) {
            return response;
        }
        ProcessorExplainDto processorExplainDto = (ProcessorExplainDto) requestContext.getAttribute(PROCESSOR_EXPLAIN);
        Explanation processorExplanation = processorExplainDto.getExplanation();
        if (Objects.isNull(processorExplanation)) {
            return response;
        }
        SearchHits searchHits = response.getHits();
        SearchHit[] searchHitsArray = searchHits.getHits();
        // create a map of searchShard and list of indexes of search hit objects in search hits array
        // the list will keep original order of sorting as per final search results
        Map<SearchShard, List<Integer>> searchHitsByShard = new HashMap<>();
        Map<SearchShard, Integer> explainsByShardCount = new HashMap<>();
        for (int i = 0; i < searchHitsArray.length; i++) {
            SearchHit searchHit = searchHitsArray[i];
            SearchShardTarget searchShardTarget = searchHit.getShard();
            SearchShard searchShard = SearchShard.create(searchShardTarget);
            searchHitsByShard.computeIfAbsent(searchShard, k -> new ArrayList<>()).add(i);
            explainsByShardCount.putIfAbsent(searchShard, -1);
        }
        Map<SearchShard, List<CombinedExplainDetails>> combinedExplainDetails = processorExplainDto.getExplainDetailsByShard();

        for (int i = 0; i < searchHitsArray.length; i++) {
            SearchHit searchHit = searchHitsArray[i];
            SearchShard searchShard = SearchShard.create(searchHit.getShard());
            int explanationIndexByShard = explainsByShardCount.get(searchShard) + 1;
            CombinedExplainDetails combinedExplainDetail = combinedExplainDetails.get(searchShard).get(explanationIndexByShard);
            // searchHit.explanation(getExplanation(searchHit, processorExplainDto, processorExplanation));
            Explanation normalizedExplanation = Explanation.match(
                combinedExplainDetail.getNormalizationExplain().value(),
                combinedExplainDetail.getNormalizationExplain().description()
            );
            Explanation combinedExplanation = Explanation.match(
                combinedExplainDetail.getCombinationExplain().value(),
                combinedExplainDetail.getCombinationExplain().description()
            );

            Explanation finalExplanation = Explanation.match(
                searchHit.getScore(),
                processorExplanation.getDescription(),
                normalizedExplanation,
                combinedExplanation,
                searchHit.getExplanation()
            );
            searchHit.explanation(finalExplanation);
            explainsByShardCount.put(searchShard, explanationIndexByShard);
        }
        return response;
    }

    /*private static Explanation getExplanation(
        SearchHit searchHit,
        ProcessorExplainDto processorExplainDto,
        Explanation generalProcessorLevelExplanation
    ) {
        SearchShardTarget searchShardTarget = searchHit.getShard();
        DocIdAtSearchShard docIdAtSearchShard = new DocIdAtSearchShard(
            searchHit.docId(),
            new SearchShard(searchShardTarget.getIndex(), searchShardTarget.getShardId().id(), searchShardTarget.getNodeId())
        );
        SearchShard searchShard = new SearchShard(searchShardTarget.getIndex(), searchShardTarget.getShardId().id(), searchShardTarget.getNodeId());
        ExplainDetails normalizationExplainDetails = processorExplainDto.getNormalizedScoresByDocId().get(docIdAtSearchShard);
        Explanation normalizedExplanation = Explanation.match(
            normalizationExplainDetails.value(),
            normalizationExplainDetails.description()
        );
        List<ExplainDetails> combinedExplainDetails = processorExplainDto.getCombinedScoresByShard().get(searchShard);
        Explanation combinedExplanation = Explanation.match(combinedExplainDetails.value(), combinedExplainDetails.description());
        Explanation finalExplanation = Explanation.match(
            searchHit.getScore(),
            generalProcessorLevelExplanation.getDescription(),
            normalizedExplanation,
            combinedExplanation,
            searchHit.getExplanation()
        );
        return finalExplanation;
    }*/

    @Override
    public String getType() {
        return TYPE;
    }
}
