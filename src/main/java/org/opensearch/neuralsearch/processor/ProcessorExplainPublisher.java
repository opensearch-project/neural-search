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
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;

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
        if (Objects.nonNull(requestContext.getAttribute(PROCESSOR_EXPLAIN))) {
            ProcessorExplainDto processorExplainDto = (ProcessorExplainDto) requestContext.getAttribute(PROCESSOR_EXPLAIN);
            Explanation explanation = processorExplainDto.getExplanation();
            SearchHits searchHits = response.getHits();
            SearchHit[] searchHitsArray = searchHits.getHits();
            for (SearchHit searchHit : searchHitsArray) {
                SearchShardTarget searchShardTarget = searchHit.getShard();
                DocIdAtQueryPhase docIdAtQueryPhase = new DocIdAtQueryPhase(
                    searchHit.docId(),
                    new SearchShard(searchShardTarget.getIndex(), searchShardTarget.getShardId().id(), searchShardTarget.getNodeId())
                );
                Explanation normalizedExplanation = Explanation.match(
                    0.0f,
                    processorExplainDto.getNormalizedScoresByDocId().get(docIdAtQueryPhase)
                );
                Explanation combinedExplanation = Explanation.match(
                    0.0f,
                    processorExplainDto.getCombinedScoresByDocId().get(docIdAtQueryPhase)
                );
                Explanation finalExplanation = Explanation.match(
                    searchHit.getScore(),
                    "combined explanation from processor and query: ",
                    explanation,
                    normalizedExplanation,
                    combinedExplanation,
                    searchHit.getExplanation()
                );
                searchHit.explanation(finalExplanation);
            }
            // delete processor explain data to avoid double processing
            // requestContext.setAttribute(PROCESSOR_EXPLAIN, null);
        }

        return response;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getTag() {
        return tag;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public boolean isIgnoreFailure() {
        return ignoreFailure;
    }
}
