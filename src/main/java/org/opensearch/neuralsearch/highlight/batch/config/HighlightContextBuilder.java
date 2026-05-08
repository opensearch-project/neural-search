/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.batch.config;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.neuralsearch.highlight.utils.HighlightExtractorUtils;
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;
import org.opensearch.neuralsearch.highlight.batch.HighlightContext;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

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
        List<String> fieldNames = new ArrayList<>();

        if (response.getHits() == null || response.getHits().getHits().length == 0) {
            return createEmptyContext(config, response, startTime);
        }

        if (config.getFieldName() != null) {
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
                    fieldNames.add(config.getFieldName());
                }
            }
        }

        if (config.hasInnerHitsTargets()) {
            collectFromInnerHits(config, response, requests, validHits, fieldNames);
        }

        return HighlightContext.builder()
            .requests(requests)
            .validHits(validHits)
            .fieldName(config.getFieldName())
            .fieldNames(fieldNames.isEmpty() ? null : fieldNames)
            .originalResponse(response)
            .startTime(startTime)
            .preTag(config.getPreTag())
            .postTag(config.getPostTag())
            .modelId(config.getModelId())
            .modelType(config.getModelType())
            .build();
    }

    /**
     * Collect highlighting requests and valid hits for each inner_hits target declared on nested queries
     * @param config the highlight configuration (must have at least one inner_hits target)
     * @param response the search response
     * @param requests the list to populate with highlighting requests
     * @param validHits the list to populate with inner hits that carry non-empty text
     * @param fieldNames the list (parallel to validHits) to populate with the field name each hit belongs to
     */
    private void collectFromInnerHits(
        HighlightConfig config,
        SearchResponse response,
        List<SentenceHighlightingRequest> requests,
        List<SearchHit> validHits,
        List<String> fieldNames
    ) {
        for (HighlightConfig.InnerHitsTarget target : config.getInnerHitsTargets()) {
            String innerHitName = target.getInnerHitName();
            String fieldName = target.getFieldName();
            String sourceLookupField = HighlightExtractorUtils.stripNestedPrefix(fieldName, target.getNestedPath());

            for (SearchHit topHit : response.getHits().getHits()) {
                Map<String, SearchHits> innerHitsMap = topHit.getInnerHits();
                if (innerHitsMap == null) {
                    continue;
                }
                SearchHits innerHits = innerHitsMap.get(innerHitName);
                if (innerHits == null || innerHits.getHits().length == 0) {
                    continue;
                }
                for (SearchHit innerHit : innerHits.getHits()) {
                    String fieldText = extractFieldText(innerHit, sourceLookupField);
                    if (fieldText != null && !fieldText.isEmpty()) {
                        SentenceHighlightingRequest request = SentenceHighlightingRequest.builder()
                            .modelId(config.getModelId())
                            .question(config.getQueryText())
                            .context(fieldText)
                            .build();

                        requests.add(request);
                        validHits.add(innerHit);
                        fieldNames.add(fieldName);
                    }
                }
            }
        }
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
