/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.single;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.Query;
import org.opensearch.OpenSearchException;
import org.opensearch.neuralsearch.highlight.single.extractor.QueryTextExtractorRegistry;
import org.opensearch.neuralsearch.highlight.utils.HighlightExtractorUtils;
import org.opensearch.neuralsearch.highlight.utils.HighlightTagApplier;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;
import org.opensearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.opensearch.action.support.PlainActionFuture;
import lombok.NonNull;
import lombok.Builder;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Engine class for semantic highlighting operations
 */
@Log4j2
@Builder
public class SemanticHighlighterEngine {
    private static final String MODEL_ID_FIELD = "model_id";
    private static final String MODEL_INFERENCE_RESULT_KEY = "highlights";

    @NonNull
    private final MLCommonsClientAccessor mlCommonsClient;

    @NonNull
    private final QueryTextExtractorRegistry queryTextExtractorRegistry;

    /**
     * Gets the field text from the document
     * @deprecated Use HighlightExtractorUtils.getFieldText instead
     *
     * @param fieldContext The field highlight context
     * @return The field text
     */
    @Deprecated
    public String getFieldText(FieldHighlightContext fieldContext) {
        return HighlightExtractorUtils.getFieldText(fieldContext);
    }

    /**
     * Extracts the original query text from the search query object.
     * @deprecated Use HighlightExtractorUtils.extractOriginalQuery instead
     *
     * @param query The query object from which to extract the original text
     * @param fieldName The name of the field being highlighted
     * @return The extracted original query text for highlighting
     * @throws IllegalArgumentException if the extracted query text is empty
     */
    @Deprecated
    public String extractOriginalQuery(Query query, String fieldName) {
        return HighlightExtractorUtils.extractOriginalQuery(query, fieldName, queryTextExtractorRegistry);
    }

    /**
     * Gets the model ID from the options
     * @deprecated Use HighlightExtractorUtils.getModelId instead
     *
     * @param options The options map
     * @return The model ID
     */
    @Deprecated
    public String getModelId(Map<String, Object> options) {
        return HighlightExtractorUtils.getModelId(options);
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
            log.warn("[SEMANTIC_HIGHLIGHT] SINGLE INFERENCE ENGINE - No results from model, returning null");
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
     * @throws OpenSearchException if highlight positions are invalid, unsorted, duplicated, or overlapping
     */
    @SuppressWarnings("unchecked")
    public String applyHighlighting(String context, Map<String, Object> highlightResult, String preTag, String postTag) {
        Object highlightsObj = highlightResult.get(MODEL_INFERENCE_RESULT_KEY);

        if (!(highlightsObj instanceof List<?> highlightsList)) {
            log.error(String.format(Locale.ROOT, "No valid highlights found in model inference result, highlightsObj: %s", highlightsObj));
            return null;
        }

        if (highlightsList.isEmpty()) {
            return context;
        }

        String result = HighlightTagApplier.applyTags(context, (List<Map<String, Object>>) highlightsObj, preTag, postTag);
        return result != null ? result : context;
    }
}
