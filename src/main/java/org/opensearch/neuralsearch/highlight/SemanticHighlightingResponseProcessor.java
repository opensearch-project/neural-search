/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.text.Text;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;
import org.opensearch.neuralsearch.processor.util.ProcessorUtils;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants.HIGHLIGHTS_KEY;
import static org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants.PROCESSOR_TYPE;
import static org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants.START_KEY;
import static org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants.END_KEY;

/**
 * Semantic highlighting response processor that implements batch semantic highlighting
 * using composition pattern with ML inference capabilities
 */
@Log4j2
public class SemanticHighlightingResponseProcessor implements SearchResponseProcessor {
    private final MLCommonsClientAccessor mlClientAccessor;

    private final String tag;
    private final String description;
    private final boolean ignoreFailure;
    private final String modelId;
    private final boolean batchInference;
    private final String preTag;
    private final String postTag;

    public SemanticHighlightingResponseProcessor(
        String tag,
        String description,
        boolean ignoreFailure,
        String modelId,
        MLCommonsClientAccessor mlClientAccessor,
        boolean batchInference,
        String preTag,
        String postTag
    ) {
        this.tag = tag;
        this.description = description;
        this.ignoreFailure = ignoreFailure;
        this.modelId = modelId;
        this.mlClientAccessor = mlClientAccessor;
        this.batchInference = batchInference;
        this.preTag = preTag;
        this.postTag = postTag;
    }

    @Override
    public void processResponseAsync(
        SearchRequest request,
        SearchResponse response,
        PipelineProcessingContext responseContext,
        ActionListener<SearchResponse> responseListener
    ) {
        long startTime = System.currentTimeMillis();

        try {
            validateHighlightRequest(request);

            String semanticHighlightField = extractSemanticHighlightField(request);

            if (semanticHighlightField == null) {
                responseListener.onResponse(response);
                return;
            }

            SearchHit[] hits = response.getHits().getHits();
            if (hits.length == 0) {
                responseListener.onResponse(response);
                return;
            }

            String queryText = extractQueryText(request);

            processSemanticHighlighting(response, queryText, semanticHighlightField, responseListener, startTime);
        } catch (Exception e) {
            handleProcessingError(e, response, responseListener);
        }
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) {
        throw new UnsupportedOperationException(String.format(Locale.ROOT, "%s processor requires async processing", PROCESSOR_TYPE));
    }

    /**
     * Validate highlight request using ProcessorUtils patterns
     */
    private void validateHighlightRequest(SearchRequest request) {
        if (request == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "search request cannot be null"));
        }
        if (request.source() == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "search request source cannot be null"));
        }
    }

    /**
     * Extract semantic highlight field from search request
     */
    private String extractSemanticHighlightField(SearchRequest request) {
        return Optional.ofNullable(request.source())
            .map(SearchSourceBuilder::highlighter)
            .map(HighlightBuilder::fields)
            .orElse(Collections.emptyList())
            .stream()
            .filter(field -> SemanticHighlightingConstants.HIGHLIGHTER_TYPE.equals(field.highlighterType()))
            .findFirst()
            .map(HighlightBuilder.Field::name)
            .orElse(null);
    }

    /**
     * Extract query text from search request for semantic highlighting
     */
    private String extractQueryText(SearchRequest request) {
        return Optional.ofNullable(request.source())
            .map(SearchSourceBuilder::query)
            .map(ProcessorUtils::extractQueryTextFromBuilder)
            .orElseThrow(() -> new IllegalArgumentException("Query text is required for semantic highlighting"));
    }

    /**
     * Extract field text from search hit using ProcessorUtils
     */
    private String extractFieldText(SearchHit hit, String fieldName) {
        return ProcessorUtils.getValueFromSource(hit.getSourceAsMap(), fieldName).map(Object::toString).orElse(null);
    }

    /**
     * Process semantic highlighting - unified method for both batch and individual
     */
    private void processSemanticHighlighting(
        SearchResponse response,
        String queryText,
        String semanticHighlightField,
        ActionListener<SearchResponse> responseListener,
        long startTime
    ) {
        try {
            SearchHit[] hits = response.getHits().getHits();

            List<SentenceHighlightingRequest> requests = new ArrayList<>();
            List<SearchHit> validHits = new ArrayList<>();

            for (SearchHit hit : hits) {
                String fieldText = extractFieldText(hit, semanticHighlightField);
                if (fieldText != null && !fieldText.isEmpty()) {
                    requests.add(SentenceHighlightingRequest.builder().modelId(modelId).question(queryText).context(fieldText).build());
                    validHits.add(hit);
                }
            }

            if (requests.isEmpty()) {
                responseListener.onResponse(response);
                return;
            }

            if (batchInference) {
                mlClientAccessor.batchInferenceSentenceHighlighting(modelId, requests, ActionListener.wrap(batchResults -> {
                    SearchResponse highlightedResponse = applyBatchHighlightResults(
                        response,
                        batchResults,
                        validHits,
                        semanticHighlightField
                    );
                    long totalTime = System.currentTimeMillis() - startTime;
                    highlightedResponse = ProcessorUtils.updateResponseTookTime(highlightedResponse, totalTime);
                    responseListener.onResponse(highlightedResponse);
                }, error -> handleProcessingError(error, response, responseListener)));
            } else {
                processSequentialRequests(requests, validHits, response, semanticHighlightField, responseListener, startTime);
            }

        } catch (Exception e) {
            handleProcessingError(e, response, responseListener);
        }
    }

    /**
     * Handle errors with ignore failure logic
     */
    private void handleProcessingError(Exception error, SearchResponse response, ActionListener<SearchResponse> responseListener) {
        if (ignoreFailure) {
            log.warn(String.format(Locale.ROOT, "semantic highlighting failed but ignoring failure: %s", error.getMessage()));
            responseListener.onResponse(response);
        } else {
            responseListener.onFailure(error);
        }
    }

    /**
     * Process requests sequentially
     */
    private void processSequentialRequests(
        List<SentenceHighlightingRequest> requests,
        List<SearchHit> validHits,
        SearchResponse response,
        String semanticHighlightField,
        ActionListener<SearchResponse> responseListener,
        long startTime
    ) {
        processNextRequest(requests, validHits, 0, response, semanticHighlightField, responseListener, startTime);
    }

    private void processNextRequest(
        List<SentenceHighlightingRequest> requests,
        List<SearchHit> validHits,
        int currentIndex,
        SearchResponse response,
        String semanticHighlightField,
        ActionListener<SearchResponse> responseListener,
        long startTime
    ) {
        // Base case: all requests processed
        if (currentIndex >= requests.size()) {
            long totalTime = System.currentTimeMillis() - startTime;
            SearchResponse finalResponse = ProcessorUtils.updateResponseTookTime(response, totalTime);
            responseListener.onResponse(finalResponse);
            return;
        }

        // Process current request
        mlClientAccessor.inferenceSentenceHighlighting(requests.get(currentIndex), ActionListener.wrap(highlightResults -> {
            try {
                applySingleHighlightResults(validHits.get(currentIndex), highlightResults, semanticHighlightField);
                processNextRequest(requests, validHits, currentIndex + 1, response, semanticHighlightField, responseListener, startTime);
            } catch (Exception e) {
                handleProcessingError(e, response, responseListener);
            }
        }, error -> {
            log.error("sequential inference failed for request {}", currentIndex, error);
            handleProcessingError(error, response, responseListener);
        }));
    }

    /**
     * Apply highlights to a specific search hit
     */
    private void applyHighlightsToHit(SearchHit hit, List<Map<String, Object>> highlights, String fieldName) {
        Map<String, Object> source = hit.getSourceAsMap();
        if (source == null) {
            log.warn(String.format(Locale.ROOT, "No source found for hit with ID '%s'", hit.getId()));
            return;
        }

        String text = (String) source.get(fieldName);
        if (text == null || text.isEmpty()) {
            log.warn(String.format(Locale.ROOT, "No text found for field '%s' in hit with ID '%s'", fieldName, hit.getId()));
            return;
        }

        String highlightedText = applyPositionHighlights(text, highlights);
        Map<String, HighlightField> highlightFields = Optional.ofNullable(hit.getHighlightFields()).orElse(new HashMap<>());
        HighlightField highlightField = new HighlightField(fieldName, new Text[] { new Text(highlightedText) });
        highlightFields.put(fieldName, highlightField);
        hit.highlightFields(highlightFields);
    }

    /**
     * Apply position-based highlights to text using ProcessorUtils for validation
     */
    private String applyPositionHighlights(String text, List<Map<String, Object>> highlights) {
        List<Map<String, Object>> validHighlights = new ArrayList<>();
        for (Map<String, Object> highlight : highlights) {
            Object startObj = highlight.get(START_KEY);
            Object endObj = highlight.get(END_KEY);

            if (ProcessorUtils.isNumeric(startObj) && ProcessorUtils.isNumeric(endObj)) {
                validHighlights.add(highlight);
            } else {
                log.error(String.format(Locale.ROOT, "Invalid highlight positions: start=%s, end=%s", startObj, endObj));
            }
        }

        if (validHighlights.isEmpty()) {
            return text;
        }

        StringBuilder result = new StringBuilder(text);
        for (int i = validHighlights.size() - 1; i >= 0; i--) {
            Map<String, Object> highlight = validHighlights.get(i);
            int start = ((Number) highlight.get(START_KEY)).intValue();
            int end = ((Number) highlight.get(END_KEY)).intValue();

            if (start >= 0 && end <= text.length() && start < end) {
                result.insert(end, postTag);
                result.insert(start, preTag);
            } else {
                log.error(
                    String.format(Locale.ROOT, "Invalid highlight range: start=%d, end=%d for text length %d", start, end, text.length())
                );
            }
        }

        return result.toString();
    }

    /**
     * Apply batch highlight results to search response using valid hits list
     */
    private SearchResponse applyBatchHighlightResults(
        SearchResponse response,
        List<List<Map<String, Object>>> batchResults,
        List<SearchHit> validHits,
        String semanticHighlightField
    ) {
        try {
            for (int i = 0; i < validHits.size() && i < batchResults.size(); i++) {
                List<Map<String, Object>> highlights = batchResults.get(i);
                if (!highlights.isEmpty()) {
                    applyHighlightsToHit(validHits.get(i), highlights, semanticHighlightField);
                }
            }
            return response;
        } catch (Exception e) {
            log.error(String.format(Locale.ROOT, "error applying batch highlight results"), e);
            return response;
        }
    }

    /**
     * Apply single highlight results to a hit
     */
    private void applySingleHighlightResults(SearchHit hit, List<Map<String, Object>> highlightResults, String fieldName) {
        if (highlightResults == null || highlightResults.isEmpty()) {
            return;
        }

        Map<String, Object> mlResponse = highlightResults.get(0);
        if (mlResponse == null) {
            throw new IllegalStateException("ML response cannot be null");
        }
        if (!mlResponse.containsKey(HIGHLIGHTS_KEY)) {
            throw new IllegalStateException("ML response missing required '" + HIGHLIGHTS_KEY + "' field");
        }

        Object highlightsObj = mlResponse.get(HIGHLIGHTS_KEY);
        if (!(highlightsObj instanceof List)) {
            throw new IllegalStateException(
                "Expected highlights to be a List, got: " + (highlightsObj != null ? highlightsObj.getClass().getSimpleName() : "null")
            );
        }

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> highlights = (List<Map<String, Object>>) highlightsObj;
        applyHighlightsToHit(hit, highlights, fieldName);
    }

    @Override
    public String getType() {
        return PROCESSOR_TYPE;
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
