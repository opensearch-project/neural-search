/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.core.common.text.Text;
import org.opensearch.neuralsearch.processor.util.ProcessorUtils;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Applies highlighting results to search hits
 */
@Log4j2
@RequiredArgsConstructor
public class HighlightResultApplier {

    private final String preTag;
    private final String postTag;

    /**
     * Apply batch highlighting results to valid hits
     */
    public void applyBatchResults(
        List<SearchHit> validHits,
        List<List<Map<String, Object>>> batchResults,
        String fieldName,
        String preTag,
        String postTag
    ) {
        if (batchResults.size() != validHits.size()) {
            log.error("Batch results size ({}) doesn't match valid hits size ({})", batchResults.size(), validHits.size());
            throw new IllegalStateException("Batch results size mismatch");
        }

        for (int i = 0; i < validHits.size(); i++) {
            List<Map<String, Object>> highlights = batchResults.get(i);
            if (highlights == null) {
                highlights = new ArrayList<>();
                log.debug("No highlights returned for hit at index {}", i);
            }
            applyHighlightsToHit(validHits.get(i), highlights, fieldName, preTag, postTag);
        }
    }

    // Backward compatible method using pipeline-level tags
    public void applyBatchResults(List<SearchHit> validHits, List<List<Map<String, Object>>> batchResults, String fieldName) {
        applyBatchResults(validHits, batchResults, fieldName, this.preTag, this.postTag);
    }

    /**
     * Apply batch results with specific indices
     */
    public void applyBatchResultsWithIndices(
        List<SearchHit> allValidHits,
        List<List<Map<String, Object>>> batchResults,
        int startIndex,
        int endIndex,
        String fieldName,
        String preTag,
        String postTag
    ) {
        int expectedBatchSize = endIndex - startIndex;
        if (batchResults.size() != expectedBatchSize) {
            log.error(
                "Batch results size ({}) doesn't match expected batch size ({}) for indices [{}, {})",
                batchResults.size(),
                expectedBatchSize,
                startIndex,
                endIndex
            );
            throw new IllegalStateException("Batch results size mismatch");
        }

        int batchIndex = 0;
        for (int i = startIndex; i < endIndex && batchIndex < batchResults.size(); i++, batchIndex++) {
            List<Map<String, Object>> highlights = batchResults.get(batchIndex);
            if (highlights == null) {
                highlights = new ArrayList<>();
                log.debug("No highlights returned for hit at index {}", i);
            }
            applyHighlightsToHit(allValidHits.get(i), highlights, fieldName, preTag, postTag);
        }
    }

    /**
     * Apply single highlighting result to a hit (backward compatible)
     */
    public void applySingleResult(SearchHit hit, List<Map<String, Object>> highlightResults, String fieldName) {
        applySingleResult(hit, highlightResults, fieldName, this.preTag, this.postTag);
    }

    /**
     * Apply single highlighting result to a hit
     */
    public void applySingleResult(
        SearchHit hit,
        List<Map<String, Object>> highlightResults,
        String fieldName,
        String preTag,
        String postTag
    ) {
        if (highlightResults == null || highlightResults.isEmpty()) {
            log.debug("No highlight results for hit: {}", hit.getId());
            applyHighlightsToHit(hit, new ArrayList<>(), fieldName, preTag, postTag);
            return;
        }

        Map<String, Object> mlResponse = highlightResults.get(0);
        if (mlResponse == null || !mlResponse.containsKey(SemanticHighlightingConstants.HIGHLIGHTS_KEY)) {
            log.debug("ML response missing highlights for hit: {}", hit.getId());
            applyHighlightsToHit(hit, new ArrayList<>(), fieldName, preTag, postTag);
            return;
        }

        Object highlightsObj = mlResponse.get(SemanticHighlightingConstants.HIGHLIGHTS_KEY);
        if (!(highlightsObj instanceof List)) {
            log.error("Invalid highlights type for hit: {}", hit.getId());
            throw new IllegalStateException("Expected highlights to be a List");
        }

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> highlights = (List<Map<String, Object>>) highlightsObj;
        applyHighlightsToHit(hit, highlights, fieldName, preTag, postTag);
    }

    /**
     * Apply highlights to a specific search hit
     */
    private void applyHighlightsToHit(
        SearchHit hit,
        List<Map<String, Object>> highlights,
        String fieldName,
        String preTag,
        String postTag
    ) {
        Map<String, Object> source = hit.getSourceAsMap();
        if (source == null) {
            return;
        }

        String text = (String) source.get(fieldName);
        if (text == null || text.isEmpty()) {
            return;
        }

        String highlightedText = applyPositionHighlights(text, highlights, preTag, postTag);

        Map<String, HighlightField> highlightFields = hit.getHighlightFields();
        if (highlightFields == null) {
            highlightFields = new HashMap<>();
        } else if (!(highlightFields instanceof HashMap)) {
            highlightFields = new HashMap<>(highlightFields);
        }

        HighlightField highlightField = new HighlightField(fieldName, new Text[] { new Text(highlightedText) });
        highlightFields.put(fieldName, highlightField);

        hit.highlightFields(highlightFields);
    }

    /**
     * Apply position-based highlights to text
     */
    private String applyPositionHighlights(String text, List<Map<String, Object>> highlights, String preTag, String postTag) {
        List<Map<String, Object>> validHighlights = new ArrayList<>();

        for (Map<String, Object> highlight : highlights) {
            Object startObj = highlight.get(SemanticHighlightingConstants.START_KEY);
            Object endObj = highlight.get(SemanticHighlightingConstants.END_KEY);

            if (ProcessorUtils.isNumeric(startObj) && ProcessorUtils.isNumeric(endObj)) {
                validHighlights.add(highlight);
            }
        }

        if (validHighlights.isEmpty()) {
            return text;
        }

        // Sort highlights by start position in descending order to apply from end to beginning
        validHighlights.sort((a, b) -> {
            int startA = ((Number) a.get(SemanticHighlightingConstants.START_KEY)).intValue();
            int startB = ((Number) b.get(SemanticHighlightingConstants.START_KEY)).intValue();
            return Integer.compare(startB, startA);
        });

        StringBuilder result = new StringBuilder(text);
        for (Map<String, Object> highlight : validHighlights) {
            int start = ((Number) highlight.get(SemanticHighlightingConstants.START_KEY)).intValue();
            int end = ((Number) highlight.get(SemanticHighlightingConstants.END_KEY)).intValue();

            if (start >= 0 && end <= text.length() && start < end) {
                result.insert(end, postTag);
                result.insert(start, preTag);
            }
        }

        return result.toString();
    }
}
