/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.search.Query;
import org.opensearch.OpenSearchException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.SentenceHighlightingRequest;
import org.opensearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.opensearch.neuralsearch.highlight.extractor.QueryTextExtractorRegistry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manager class for neural highlighting operations that handles the core highlighting logic
 */
@Log4j2
public class NeuralHighlighterManager {
    private static final String MODEL_ID_FIELD = "model_id";
    private static final String DEFAULT_PRE_TAG = "<em>";
    private static final String DEFAULT_POST_TAG = "</em>";
    private static final String MODEL_INFERENCE_RESULT_KEY = "highlights";
    private static final String MODEL_INFERENCE_RESULT_START_KEY = "start";
    private static final String MODEL_INFERENCE_RESULT_END_KEY = "end";

    private final MLCommonsClientAccessor mlCommonsClient;
    private final QueryTextExtractorRegistry queryTextExtractorRegistry;

    public NeuralHighlighterManager(MLCommonsClientAccessor mlCommonsClient) {
        this.mlCommonsClient = Objects.requireNonNull(mlCommonsClient, "ML Commons client cannot be null");
        this.queryTextExtractorRegistry = new QueryTextExtractorRegistry();
    }

    /**
     * Gets the field text from the document
     *
     * @param fieldContext The field highlight context
     * @return The field text
     */
    public String getFieldText(FieldHighlightContext fieldContext) {
        if (fieldContext.hitContext == null || fieldContext.hitContext.sourceLookup() == null) {
            log.error("Hit context or source lookup is null. Cannot extract field text.");
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Field %s is not found in the hit", fieldContext.fieldName));
        }
        String hitValue = (String) fieldContext.hitContext.sourceLookup().extractValue(fieldContext.fieldName, null);
        if (hitValue == null) {
            log.error("Field value is null. Cannot extract field text.");
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Field %s is not found in the document", fieldContext.fieldName));
        }
        if (hitValue.isEmpty()) {
            log.error("Field value is empty. Cannot extract field text.");
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Field %s is empty", fieldContext.fieldName));
        }
        return hitValue;
    }

    /**
     * Extracts the original query text from the search query object.
     *
     * @param query The query object from which to extract the original text
     * @param fieldName The name of the field being highlighted
     * @return The extracted original query text for highlighting
     * @throws IllegalArgumentException if the extracted query text is empty
     */
    public String extractOriginalQuery(Query query, String fieldName) {
        if (fieldName == null) {
            log.warn("Field name is null, extraction may be less accurate");
        }
        return queryTextExtractorRegistry.extractQueryText(query, fieldName);
    }

    /**
     * Gets the model ID from the options
     *
     * @param options The options map
     * @return The model ID
     */
    public String getModelId(Map<String, Object> options) {
        Object modelId = options.get(MODEL_ID_FIELD);
        if (Objects.isNull(modelId)) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Missing required option: %s", MODEL_ID_FIELD));
        }
        return modelId.toString();
    }

    /**
     * Gets highlighted text from the ML model
     *
     * @param modelId The ID of the model to use
     * @param question The search query
     * @param context The document text
     * @return Formatted text with highlighting
     */
    public String getHighlightedSentences(String modelId, String question, String context) {
        List<Map<String, Object>> result = fetchModelResults(modelId, question, context);
        if (result == null) {
            return StringUtils.EMPTY;
        }

        return applyHighlighting(context, result);
    }

    /**
     * Fetches highlighting results from the ML model
     *
     * @param modelId The ID of the model to use
     * @param question The search query
     * @param context The document text
     * @return The highlighting results
     */
    public List<Map<String, Object>> fetchModelResults(String modelId, String question, String context) {

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<List<Map<String, Object>>> resultRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        SentenceHighlightingRequest request = SentenceHighlightingRequest.builder()
            .modelId(modelId)
            .question(question)
            .context(context)
            .build();

        mlCommonsClient.inferenceSentenceHighlighting(request, ActionListener.wrap(result -> {
            resultRef.set(result);
            latch.countDown();
        }, exception -> {
            exceptionRef.set(exception);
            latch.countDown();
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OpenSearchException(
                String.format(Locale.ROOT, "Interrupted while waiting for sentence highlighting inference from model [%s]", modelId),
                e
            );
        }

        if (exceptionRef.get() != null) {
            throw new OpenSearchException("Error during sentence highlighting inference", exceptionRef.get());
        }

        return resultRef.get();
    }

    /**
     * Applies highlighting to the original context based on the ML model response
     *
     * @param context The original document text
     * @param highlightResults The highlighting results from the ML model
     * @return Formatted text with highlighting
     */
    public String applyHighlighting(String context, List<Map<String, Object>> highlightResults) {
        List<int[]> validHighlights = new ArrayList<>();

        if (highlightResults != null && !highlightResults.isEmpty()) {
            Map<String, Object> result = highlightResults.getFirst();
            if (result != null) {
                // Get the "highlights" list from the result
                Object highlightsObj = result.get(MODEL_INFERENCE_RESULT_KEY);

                if (highlightsObj instanceof List<?> highlightsList) {
                    for (Object item : highlightsList) {
                        if (item instanceof Map<?, ?> map) {
                            Map<String, Object> safeMap = new java.util.HashMap<>();
                            for (Map.Entry<?, ?> entry : map.entrySet()) {
                                safeMap.put(entry.getKey().toString(), entry.getValue());
                            }

                            // Extract and validate positions
                            Object startObj = safeMap.get(MODEL_INFERENCE_RESULT_START_KEY);
                            Object endObj = safeMap.get(MODEL_INFERENCE_RESULT_END_KEY);

                            int[] positions = validateHighlightPositions(startObj, endObj, context.length());
                            if (positions != null) {
                                validHighlights.add(positions);
                            }
                        }
                    }
                }
            }
        }

        // If no valid highlights, return the original text
        if (validHighlights.isEmpty()) {
            return context;
        }

        // Sort highlights by start position (ascending)
        validHighlights.sort(Comparator.comparingInt(pos -> pos[0]));

        return constructHighlightedText(context, validHighlights);
    }

    /**
     * Constructs highlighted text by iterating through the text once
     *
     * @param text The original text
     * @param highlights The list of valid highlight positions (sorted by start position)
     * @return The highlighted text
     */
    private String constructHighlightedText(String text, List<int[]> highlights) {
        StringBuilder result = new StringBuilder();
        int currentPos = 0;

        // Check for overlapping highlights and merge them if necessary
        List<int[]> mergedHighlights = mergeOverlappingHighlights(highlights);

        // Iterate through each highlight
        for (int[] highlight : mergedHighlights) {
            int start = highlight[0];
            int end = highlight[1];

            // Add text before the highlight
            if (start > currentPos) {
                result.append(text, currentPos, start);
            }

            // Add the highlighted text
            result.append(DEFAULT_PRE_TAG);
            result.append(text, start, end);
            result.append(DEFAULT_POST_TAG);

            // Update current position
            currentPos = end;
        }

        // Add any remaining text
        if (currentPos < text.length()) {
            result.append(text, currentPos, text.length());
        }

        return result.toString();
    }

    /**
     * Merges overlapping highlights to ensure proper nesting
     *
     * @param highlights The list of highlights sorted by start position
     * @return A list of merged highlights with no overlaps
     */
    private List<int[]> mergeOverlappingHighlights(List<int[]> highlights) {
        if (highlights.isEmpty()) {
            return highlights;
        }

        List<int[]> merged = new ArrayList<>();
        int[] current = highlights.getFirst();
        merged.add(current);

        for (int i = 1; i < highlights.size(); i++) {
            int[] next = highlights.get(i);

            // If current and next overlap
            if (next[0] <= current[1]) {
                // Extend current if next ends after current
                if (next[1] > current[1]) {
                    current[1] = next[1];
                }
                // Otherwise next is completely contained in current, so skip it
            } else {
                // No overlap, add next as a new current
                current = next;
                merged.add(current);
            }
        }

        return merged;
    }

    /**
     * Validates highlight position objects and converts them to integers
     *
     * @param startObj The start position object
     * @param endObj The end position object
     * @param textLength The length of the text being highlighted
     * @return An array containing [start, end] positions if valid, null otherwise
     */
    private int[] validateHighlightPositions(Object startObj, Object endObj, int textLength) {
        // Check for null values
        if (startObj == null || endObj == null) {
            log.error("Missing start or end position in highlight data. Skipping this highlight.");
            return null;
        }

        // Check for non-Number types
        if (!(startObj instanceof Number) || !(endObj instanceof Number)) {
            log.error(
                "Invalid highlight position types: start={} ({}), end={} ({}). Skipping this highlight.",
                startObj,
                startObj.getClass().getName(),
                endObj,
                endObj.getClass().getName()
            );
            return null;
        }

        int start = ((Number) startObj).intValue();
        int end = ((Number) endObj).intValue();

        // Validate position values
        if (start < 0 || end > textLength || start >= end) {
            log.error("Invalid highlight positions: start={}, end={}, text length={}. Skipping this highlight.", start, end, textLength);
            return null;
        }

        return new int[] { start, end };
    }
}
