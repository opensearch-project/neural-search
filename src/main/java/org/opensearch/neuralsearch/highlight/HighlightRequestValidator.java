/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
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
 * Validates and extracts information from highlight requests
 */
@Log4j2
public class HighlightRequestValidator {

    /**
     * Validate the search request and extract necessary information
     */
    public ValidationResult validate(
        SearchRequest request,
        SearchResponse response,
        String defaultModelId,
        String defaultPreTag,
        String defaultPostTag
    ) {
        try {
            // Basic validation
            if (request == null) {
                return ValidationResult.invalid("Search request cannot be null");
            }
            if (request.source() == null) {
                return ValidationResult.invalid("Search request source cannot be null");
            }

            // Extract semantic highlight field
            String semanticField = extractSemanticHighlightField(request);
            if (semanticField == null) {
                return ValidationResult.invalid("No semantic highlight field found");
            }

            // Extract query text
            String queryText = extractQueryText(request);
            if (queryText == null || queryText.isEmpty()) {
                return ValidationResult.invalid("Query text is required for semantic highlighting");
            }

            // Extract model ID
            String modelId = extractModelId(request, defaultModelId);

            // Extract tags (query-level overrides pipeline-level)
            String preTag = extractPreTag(request, defaultPreTag);
            String postTag = extractPostTag(request, defaultPostTag);

            // Extract batch configuration from query options
            boolean batchInference = extractBatchInference(request);
            int maxBatchSize = extractMaxBatchSize(request);

            // Check if response has hits
            if (response.getHits() == null || response.getHits().getHits().length == 0) {
                return ValidationResult.invalid("No search hits to highlight");
            }

            return ValidationResult.valid(semanticField, modelId, queryText, preTag, postTag, batchInference, maxBatchSize);

        } catch (Exception e) {
            log.error("Validation failed", e);
            return ValidationResult.invalid("Validation failed: " + e.getMessage());
        }
    }

    /**
     * Extract semantic highlight field from search request
     */
    public String extractSemanticHighlightField(SearchRequest request) {
        List<HighlightBuilder.Field> fields = Optional.ofNullable(request.source())
            .map(SearchSourceBuilder::highlighter)
            .map(HighlightBuilder::fields)
            .orElse(Collections.emptyList());

        return fields.stream()
            .filter(field -> SemanticHighlightingConstants.HIGHLIGHTER_TYPE.equals(field.highlighterType()))
            .findFirst()
            .map(HighlightBuilder.Field::name)
            .orElse(null);
    }

    /**
     * Extract model ID from highlight options or use default
     */
    public String extractModelId(SearchRequest request, String defaultModelId) {
        if (request.source() == null || request.source().highlighter() == null) {
            return defaultModelId;
        }

        HighlightBuilder highlighter = request.source().highlighter();

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

        return defaultModelId;
    }

    /**
     * Extract query text from search request
     */
    public String extractQueryText(SearchRequest request) {
        return Optional.ofNullable(request.source())
            .map(SearchSourceBuilder::query)
            .map(ProcessorUtils::extractQueryTextFromBuilder)
            .orElse(null);
    }

    /**
     * Extract pre_tag from highlight options or use default
     */
    public String extractPreTag(SearchRequest request, String defaultPreTag) {
        // First check for standard OpenSearch pre_tags array
        if (request.source() != null && request.source().highlighter() != null) {
            String[] preTags = request.source().highlighter().preTags();
            if (preTags != null && preTags.length > 0) {
                return preTags[0];  // Use first tag
            }
        }
        // Fall back to checking options for backward compatibility
        return extractHighlightOption(request, SemanticHighlightingConstants.PRE_TAG, defaultPreTag);
    }

    /**
     * Extract post_tag from highlight options or use default
     */
    public String extractPostTag(SearchRequest request, String defaultPostTag) {
        // First check for standard OpenSearch post_tags array
        if (request.source() != null && request.source().highlighter() != null) {
            String[] postTags = request.source().highlighter().postTags();
            if (postTags != null && postTags.length > 0) {
                return postTags[0];  // Use first tag
            }
        }
        // Fall back to checking options for backward compatibility
        return extractHighlightOption(request, SemanticHighlightingConstants.POST_TAG, defaultPostTag);
    }

    /**
     * Extract batch_inference from highlight options
     */
    public boolean extractBatchInference(SearchRequest request) {
        if (request.source() == null || request.source().highlighter() == null) {
            return false;
        }

        HighlightBuilder highlighter = request.source().highlighter();
        Map<String, Object> options = highlighter.options();

        if (options != null && options.containsKey(SemanticHighlightingConstants.BATCH_INFERENCE)) {
            Object value = options.get(SemanticHighlightingConstants.BATCH_INFERENCE);
            if (value instanceof Boolean) {
                return (Boolean) value;
            }
        }

        return false;  // Default to false for backward compatibility
    }

    /**
     * Extract max_inference_batch_size from highlight options
     */
    public int extractMaxBatchSize(SearchRequest request) {
        if (request.source() == null || request.source().highlighter() == null) {
            return SemanticHighlightingConstants.DEFAULT_MAX_INFERENCE_BATCH_SIZE;
        }

        HighlightBuilder highlighter = request.source().highlighter();
        Map<String, Object> options = highlighter.options();

        if (options != null && options.containsKey(SemanticHighlightingConstants.MAX_INFERENCE_BATCH_SIZE)) {
            Object value = options.get(SemanticHighlightingConstants.MAX_INFERENCE_BATCH_SIZE);
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
        }

        return SemanticHighlightingConstants.DEFAULT_MAX_INFERENCE_BATCH_SIZE;
    }

    /**
     * Generic method to extract highlight options
     */
    private String extractHighlightOption(SearchRequest request, String optionKey, String defaultValue) {
        if (request.source() == null || request.source().highlighter() == null) {
            return defaultValue;
        }

        HighlightBuilder highlighter = request.source().highlighter();

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

    /**
     * Validation result containing extracted information
     */
    @Getter
    @RequiredArgsConstructor
    public static class ValidationResult {
        private final boolean valid;
        private final String semanticField;
        private final String modelId;
        private final String queryText;
        private final String preTag;
        private final String postTag;
        private final boolean batchInference;
        private final int maxBatchSize;
        private final String errorMessage;

        public static ValidationResult valid(
            String semanticField,
            String modelId,
            String queryText,
            String preTag,
            String postTag,
            boolean batchInference,
            int maxBatchSize
        ) {
            return new ValidationResult(true, semanticField, modelId, queryText, preTag, postTag, batchInference, maxBatchSize, null);
        }

        public static ValidationResult invalid(String errorMessage) {
            return new ValidationResult(false, null, null, null, null, null, false, 0, errorMessage);
        }
    }
}
