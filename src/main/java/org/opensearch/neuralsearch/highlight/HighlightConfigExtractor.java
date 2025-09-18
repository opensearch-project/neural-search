/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.neuralsearch.processor.util.ProcessorUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Extracts highlight configuration from search requests.
 * Single responsibility: extraction logic only, no validation.
 */
@Log4j2
public class HighlightConfigExtractor {

    /**
     * Extract highlight configuration from search request and response
     * Returns config with potentially null fields - let validator check
     * @param request the search request
     * @param response the search response
     * @return extracted configuration (may have null fields)
     */
    public HighlightConfig extract(SearchRequest request, SearchResponse response) {
        try {
            if (request == null || request.source() == null) {
                log.debug("No search request source to extract from");
                return HighlightConfig.empty();
            }

            HighlightBuilder highlighter = request.source().highlighter();
            if (highlighter == null) {
                log.debug("No highlighter in request");
                return HighlightConfig.empty();
            }

            String fieldName = extractSemanticField(highlighter);
            String modelId = extractModelId(highlighter);
            String queryText = extractQueryText(request);

            return HighlightConfig.builder()
                .fieldName(fieldName)      // Can be null
                .modelId(modelId)          // Can be null
                .queryText(queryText)      // Can be null
                .preTag(extractPreTag(highlighter))
                .postTag(extractPostTag(highlighter))
                .build();

        } catch (Exception e) {
            log.error("Failed to extract highlight configuration", e);
            return HighlightConfig.invalid("Configuration extraction failed: " + e.getMessage());
        }
    }

    private String extractSemanticField(HighlightBuilder highlighter) {
        List<HighlightBuilder.Field> fields = Optional.ofNullable(highlighter.fields()).orElse(Collections.emptyList());

        return fields.stream()
            .filter(field -> SemanticHighlightingConstants.HIGHLIGHTER_TYPE.equals(field.highlighterType()))
            .findFirst()
            .map(HighlightBuilder.Field::name)
            .orElse(null);
    }

    private String extractModelId(HighlightBuilder highlighter) {
        // Check global highlighter options first
        Map<String, Object> options = highlighter.options();
        if (options != null && options.containsKey(SemanticHighlightingConstants.MODEL_ID)) {
            Object modelIdValue = options.get(SemanticHighlightingConstants.MODEL_ID);
            if (modelIdValue instanceof String) {
                return (String) modelIdValue;
            }
        }

        // Check field-specific options
        for (HighlightBuilder.Field field : highlighter.fields()) {
            if (SemanticHighlightingConstants.HIGHLIGHTER_TYPE.equals(field.highlighterType())) {
                Map<String, Object> fieldOptions = field.options();
                if (fieldOptions != null && fieldOptions.containsKey(SemanticHighlightingConstants.MODEL_ID)) {
                    Object modelIdValue = fieldOptions.get(SemanticHighlightingConstants.MODEL_ID);
                    if (modelIdValue instanceof String) {
                        return (String) modelIdValue;
                    }
                }
            }
        }

        return null;
    }

    private String extractQueryText(SearchRequest request) {
        return Optional.ofNullable(request.source())
            .map(SearchSourceBuilder::query)
            .map(ProcessorUtils::extractQueryTextFromBuilder)
            .orElse(null);
    }

    private String extractPreTag(HighlightBuilder highlighter) {
        String[] preTags = highlighter.preTags();
        if (preTags != null && preTags.length > 0) {
            return preTags[0];
        }

        return extractHighlightOption(highlighter, SemanticHighlightingConstants.PRE_TAG, SemanticHighlightingConstants.DEFAULT_PRE_TAG);
    }

    private String extractPostTag(HighlightBuilder highlighter) {
        String[] postTags = highlighter.postTags();
        if (postTags != null && postTags.length > 0) {
            return postTags[0];
        }

        return extractHighlightOption(highlighter, SemanticHighlightingConstants.POST_TAG, SemanticHighlightingConstants.DEFAULT_POST_TAG);
    }

    private String extractHighlightOption(HighlightBuilder highlighter, String optionKey, String defaultValue) {
        // Check global highlighter options first
        Map<String, Object> options = highlighter.options();
        if (options != null && options.containsKey(optionKey)) {
            Object optionValue = options.get(optionKey);
            if (optionValue instanceof String) {
                return (String) optionValue;
            }
        }

        // Check field-specific options
        for (HighlightBuilder.Field field : highlighter.fields()) {
            if (SemanticHighlightingConstants.HIGHLIGHTER_TYPE.equals(field.highlighterType())) {
                Map<String, Object> fieldOptions = field.options();
                if (fieldOptions != null && fieldOptions.containsKey(optionKey)) {
                    Object optionValue = fieldOptions.get(optionKey);
                    if (optionValue instanceof String) {
                        return (String) optionValue;
                    }
                }
            }
        }

        return defaultValue;
    }
}
