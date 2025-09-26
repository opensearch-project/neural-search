/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.batch.utils;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;
import org.opensearch.neuralsearch.processor.util.ProcessorUtils;
import org.opensearch.search.SearchHit;
import org.opensearch.neuralsearch.highlight.batch.HighlightContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Prepares highlighting requests from search hits
 */
@Log4j2
public class HighlightRequestPreparer {

    /**
     * Prepare highlighting context from search hits
     */
    public HighlightContext prepare(
        SearchResponse response,
        String queryText,
        String fieldName,
        String modelId,
        long startTime,
        String preTag,
        String postTag
    ) {
        SearchHit[] hits = response.getHits().getHits();
        List<SentenceHighlightingRequest> requests = new ArrayList<>();
        List<SearchHit> validHits = new ArrayList<>();

        for (SearchHit hit : hits) {
            String fieldText = extractFieldText(hit, fieldName);
            if (fieldText != null && !fieldText.isEmpty()) {
                requests.add(SentenceHighlightingRequest.builder().modelId(modelId).question(queryText).context(fieldText).build());
                validHits.add(hit);
            }
        }

        return HighlightContext.builder()
            .requests(requests)
            .validHits(validHits)
            .fieldName(fieldName)
            .originalResponse(response)
            .startTime(startTime)
            .preTag(preTag)
            .postTag(postTag)
            .build();
    }

    /**
     * Extract field text from search hit
     */
    private String extractFieldText(SearchHit hit, String fieldName) {
        return ProcessorUtils.getValueFromSource(hit.getSourceAsMap(), fieldName).map(Object::toString).orElse(null);
    }
}
