/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.Query;
import org.opensearch.OpenSearchException;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;
import org.opensearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.opensearch.neuralsearch.highlight.extractor.QueryTextExtractorRegistry;
import org.opensearch.action.support.PlainActionFuture;
import lombok.NonNull;
import lombok.Builder;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Engine class for semantic highlighting operations
 */
@Log4j2
@Builder
public class SemanticHighlighterEngine {
    private static final String MODEL_ID_FIELD = "model_id";
    private static final String MODEL_INFERENCE_RESULT_KEY = "highlights";
    private static final String MODEL_INFERENCE_RESULT_START_KEY = "start";
    private static final String MODEL_INFERENCE_RESULT_END_KEY = "end";

    @NonNull
    private final MLCommonsClientAccessor mlCommonsClient;

    @NonNull
    private final QueryTextExtractorRegistry queryTextExtractorRegistry;

    /**
     * Gets the field text from the document
     *
     * @param fieldContext The field highlight context
     * @return The field text
     */
    public String getFieldText(FieldHighlightContext fieldContext) {
        if (fieldContext.hitContext == null || fieldContext.hitContext.sourceLookup() == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Field %s is not found in the hit", fieldContext.fieldName));
        }
        Object fieldTextObject = fieldContext.hitContext.sourceLookup().extractValue(fieldContext.fieldName, null);
        if (fieldTextObject == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Field %s is not found in the document", fieldContext.fieldName));
        }
        if (fieldTextObject instanceof String == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Field %s must be a string for highlighting, but was %s",
                    fieldContext.fieldName,
                    fieldTextObject.getClass().getSimpleName()
                )
            );
        }
        String fieldTextString = (String) fieldTextObject;
        if (fieldTextString.isEmpty()) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Field %s is empty", fieldContext.fieldName));
        }
        return fieldTextString;
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
        if (Objects.isNull(modelId) || (modelId instanceof String) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "%s must be a non-null string, but was %s",
                    MODEL_ID_FIELD,
                    modelId == null ? "null" : modelId.getClass().getSimpleName()
                )
            );
        }
        return (String) modelId;
    }

    /**
     * Gets highlighted text from the ML model
     *
     * @param modelId The ID of the model to use
     * @param question The search query
     * @param context The document text
     * @param preTag The pre tag to use for highlighting
     * @param postTag The post tag to use for highlighting
     * @return Formatted text with highlighting
     */
    public String getHighlightedSentences(String modelId, String question, String context, String preTag, String postTag) {
        List<Map<String, Object>> results = fetchModelResults(modelId, question, context);
        if (results == null || results.isEmpty()) {
            return null;
        }

        return applyHighlighting(context, results.getFirst(), preTag, postTag);
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
        PlainActionFuture<List<Map<String, Object>>> future = PlainActionFuture.newFuture();

        SentenceHighlightingRequest request = SentenceHighlightingRequest.builder()
            .modelId(modelId)
            .question(question)
            .context(context)
            .build();

        mlCommonsClient.inferenceSentenceHighlighting(request, future);

        try {
            return future.actionGet();
        } catch (Exception e) {
            log.error(
                "Error during sentence highlighting inference - modelId: [{}], question: [{}], context: [{}]",
                modelId,
                question,
                context,
                e
            );
            throw new OpenSearchException(
                String.format(Locale.ROOT, "Error during sentence highlighting inference from model [%s]", modelId),
                e
            );
        }
    }

    /**
     * Applies highlighting to the original context based on the ML model response
     *
     * @param context The original document text
     * @param highlightResult The highlighting result from the ML model
     * @param preTag The pre tag to use for highlighting
     * @param postTag The post tag to use for highlighting
     * @return Formatted text with highlighting
     * @throws IllegalArgumentException if highlight positions are invalid
     */
    public String applyHighlighting(String context, Map<String, Object> highlightResult, String preTag, String postTag) {
        // Get the "highlights" list from the result
        Object highlightsObj = highlightResult.get(MODEL_INFERENCE_RESULT_KEY);

        if (!(highlightsObj instanceof List<?> highlightsList)) {
            log.error(String.format(Locale.ROOT, "No valid highlights found in model inference result, highlightsObj: %s", highlightsObj));
            return null;
        }

        if (highlightsList.isEmpty()) {
            // No highlights found, return context as is
            return context;
        }

        // Pre-allocate size * 2 since we store start and end positions as consecutive pairs
        // Format: [start1, end1, start2, end2, start3, end3, ...]
        ArrayList<Integer> validHighlights = new ArrayList<>(highlightsList.size() * 2);

        for (Object item : highlightsList) {
            Map<String, Number> map = getHighlightsPositionMap(item);

            Number start = map.get(MODEL_INFERENCE_RESULT_START_KEY);
            Number end = map.get(MODEL_INFERENCE_RESULT_END_KEY);

            if (start == null || end == null) {
                throw new OpenSearchException("Missing start or end position in highlight data");
            }

            // Validate positions and add them as a pair to maintain the start-end relationship
            validateHighlightPositions(start.intValue(), end.intValue(), context.length());
            validHighlights.add(start.intValue());  // Even indices (0,2,4,...) store start positions
            validHighlights.add(end.intValue());    // Odd indices (1,3,5,...) store end positions
        }

        // Verify highlights are sorted by start position (ascending)
        // We start from i=2 (second start position) and compare with previous start position (i-2)
        // Using i+=2 to skip end positions and only compare start positions with each other
        for (int i = 2; i < validHighlights.size(); i += 2) {
            // Compare current start position with previous start position
            if (validHighlights.get(i) < validHighlights.get(i - 2)) {
                log.error(String.format(Locale.ROOT, "Highlights are not sorted: %s", validHighlights));
                throw new OpenSearchException("Internal error while applying semantic highlight: received unsorted highlights from model");
            }
        }

        return constructHighlightedText(context, validHighlights, preTag, postTag);
    }

    /**
     * Validates highlight position values
     *
     * @param start The start position
     * @param end The end position
     * @param textLength The length of the text being highlighted
     * @throws OpenSearchException if positions are invalid
     */
    private void validateHighlightPositions(int start, int end, int textLength) {
        if (start < 0 || end > textLength || start >= end) {
            throw new OpenSearchException(
                String.format(
                    Locale.ROOT,
                    "Invalid highlight positions: start=%d, end=%d, textLength=%d. Positions must satisfy: 0 <= start < end <= textLength",
                    start,
                    end,
                    textLength
                )
            );
        }
    }

    /**
     * Constructs highlighted text by iterating through the text once
     *
     * @param text The original text
     * @param highlights The list of valid highlight positions in pairs [start1, end1, start2, end2, ...]
     * @param preTag The pre tag to use for highlighting
     * @param postTag The post tag to use for highlighting
     * @return The highlighted text
     */
    private String constructHighlightedText(String text, List<Integer> highlights, String preTag, String postTag) {
        StringBuilder result = new StringBuilder();
        int currentPos = 0;

        // Iterate through highlight positions in pairs (start, end)
        // i increments by 2 to move from one pair to the next
        for (int i = 0; i < highlights.size(); i += 2) {
            int start = highlights.get(i);     // Get start position from even index
            int end = highlights.get(i + 1);   // Get end position from odd index

            // Add text before the highlight if there is any
            if (start > currentPos) {
                result.append(text, currentPos, start);
            }

            // Add the highlighted text with highlight tags
            result.append(preTag);
            result.append(text, start, end);
            result.append(postTag);

            // Update current position to end of this highlight
            currentPos = end;
        }

        // Add any remaining text after the last highlight
        if (currentPos < text.length()) {
            result.append(text, currentPos, text.length());
        }

        return result.toString();
    }

    /**
     * Extracts the highlight position map from a highlight item
     *
     * @param item The highlight item
     * @return The highlight position map
     * @throws OpenSearchException if the item cannot be cast to Map<String, Number>
     */
    private static Map<String, Number> getHighlightsPositionMap(Object item) {
        try {
            return (Map<String, Number>) item;
        } catch (ClassCastException e) {
            throw new OpenSearchException(String.format(Locale.ROOT, "Expect item to be map of string to number, but was: %s", item));
        }
    }
}
