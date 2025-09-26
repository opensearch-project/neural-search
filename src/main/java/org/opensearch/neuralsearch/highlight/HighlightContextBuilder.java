/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;
import org.opensearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builds HighlightContext from validated configuration.
 * Replaces HighlightRequestPreparer with cleaner separation of concerns.
 */
@Log4j2
public class HighlightContextBuilder {

    /**
     * Build a HighlightContext from configuration and response
     * @param config the validated highlight configuration
     * @param response the search response
     * @param startTime the start time of processing
     * @return built context ready for processing
     */
    public HighlightContext build(HighlightConfig config, SearchResponse response, long startTime) {
        List<SentenceHighlightingRequest> requests = new ArrayList<>();
        List<SearchHit> validHits = new ArrayList<>();

        if (response.getHits() == null || response.getHits().getHits().length == 0) {
            log.debug("No hits to process for highlighting");
            return createEmptyContext(config, response, startTime);
        }

        for (SearchHit hit : response.getHits().getHits()) {
            String fieldText = extractFieldText(hit, config.getFieldName());

            if (fieldText != null && !fieldText.isEmpty()) {
                SentenceHighlightingRequest request = SentenceHighlightingRequest.builder()
                    .modelId(config.getModelId())
                    .question(config.getQueryText())
                    .context(fieldText)
                    .build();

                requests.add(request);
                validHits.add(hit);

                log.debug("Created highlighting request for hit ID: {}", hit.getId());
            } else {
                log.debug("Skipping hit ID: {} - no text found in field: {}", hit.getId(), config.getFieldName());
            }
        }

        log.debug("Built highlight context with {} requests for field: {}", requests.size(), config.getFieldName());

        return HighlightContext.builder()
            .requests(requests)
            .validHits(validHits)
            .fieldName(config.getFieldName())
            .originalResponse(response)
            .startTime(startTime)
            .preTag(config.getPreTag())
            .postTag(config.getPostTag())
            .modelId(config.getModelId())
            .modelType(config.getModelType())
            .build();
    }

    /**
     * Extract text from a specific field in the search hit
     * @param hit the search hit
     * @param fieldName the field name to extract
     * @return the field text or null if not found
     */
    private String extractFieldText(SearchHit hit, String fieldName) {
        Map<String, Object> sourceMap = hit.getSourceAsMap();

        if (sourceMap == null || !sourceMap.containsKey(fieldName)) {
            return null;
        }

        Object fieldValue = sourceMap.get(fieldName);

        if (fieldValue instanceof String) {
            return (String) fieldValue;
        } else if (fieldValue instanceof List) {
            // Handle array fields - join with spaces
            List<?> values = (List<?>) fieldValue;
            StringBuilder sb = new StringBuilder();
            for (Object value : values) {
                if (value != null) {
                    if (sb.length() > 0) {
                        sb.append(" ");
                    }
                    sb.append(value.toString());
                }
            }
            return sb.toString();
        } else if (fieldValue != null) {
            return fieldValue.toString();
        }

        return null;
    }

    /**
     * Create an empty context when there's nothing to highlight
     */
    private HighlightContext createEmptyContext(HighlightConfig config, SearchResponse response, long startTime) {
        return HighlightContext.builder()
            .requests(new ArrayList<>())
            .validHits(new ArrayList<>())
            .fieldName(config.getFieldName())
            .originalResponse(response)
            .startTime(startTime)
            .modelId(config.getModelId())
            .modelType(config.getModelType())
            .preTag(config.getPreTag())
            .postTag(config.getPostTag())
            .build();
    }
}
