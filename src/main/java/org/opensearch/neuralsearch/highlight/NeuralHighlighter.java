/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.Query;
import org.opensearch.OpenSearchException;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.query.NeuralKNNQuery;
import org.opensearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.search.fetch.subphase.highlight.Highlighter;
import org.opensearch.core.common.text.Text;
import org.opensearch.core.action.ActionListener;
import org.opensearch.neuralsearch.processor.SentenceHighlightingRequest;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Neural highlighter that uses ML models to identify relevant text spans for highlighting
 */
@Log4j2
public class NeuralHighlighter implements Highlighter {
    public static final String NAME = "neural";
    private static final String MODEL_ID_FIELD = "model_id";
    private static final String PRE_TAG = "<em>";
    private static final String POST_TAG = "</em>";
    // Support text fields type as of now
    private static final String supportedFieldType = "text";

    private static MLCommonsClientAccessor mlCommonsClient;

    public static void initialize(MLCommonsClientAccessor mlClient) {
        NeuralHighlighter.mlCommonsClient = mlClient;
    }

    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        return supportedFieldType.equals(fieldType.typeName());
    }

    @Override
    public HighlightField highlight(FieldHighlightContext fieldContext) {
        try {
            MappedFieldType fieldType = fieldContext.fieldType;
            if (canHighlight(fieldType) == false) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Field %s is not supported for neural highlighting", fieldContext.fieldName)
                );
            }

            String fieldText = getFieldText(fieldContext);

            String searchQuery = extractOriginalQuery(fieldContext.query);

            if (fieldContext.field.fieldOptions().options() == null) {
                throw new IllegalArgumentException("Field options cannot be null");
            }

            Map<String, Object> options = fieldContext.field.fieldOptions().options();
            String modelId = getModelId(options);

            // Get highlighted text from ML model
            String highlightedText = getHighlightedText(modelId, searchQuery, fieldText);

            // Return highlight field
            Text[] fragments = new Text[] { new Text(highlightedText) };
            return new HighlightField(fieldContext.fieldName, fragments);
        } catch (Exception e) {
            throw new OpenSearchException(
                String.format(Locale.ROOT, "Failed to perform neural highlighting for field %s", fieldContext.fieldName),
                e
            );
        }
    }

    /**
     * Gets highlighted text from the ML model.
     *
     * @param modelId The ID of the model to use
     * @param question The search query
     * @param context The document text
     * @return Formatted text with highlighting
     */
    private String getHighlightedText(String modelId, String question, String context) {
        if (mlCommonsClient == null) {
            throw new IllegalStateException("ML Commons client is not initialized");
        }

        // Use CountDownLatch to wait for async response
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<List<Map<String, Object>>> resultRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        // Create SentenceHighlightingRequest
        SentenceHighlightingRequest request = SentenceHighlightingRequest.builder()
            .modelId(modelId)
            .question(question)
            .context(context)
            .build();

        // Call ML model with the request
        mlCommonsClient.inferenceSentenceHighlighting(request, ActionListener.wrap(result -> {
            resultRef.set(result);
            latch.countDown();
        }, exception -> {
            exceptionRef.set(exception);
            latch.countDown();
        }));

        // Check for exceptions
        if (exceptionRef.get() != null) {
            throw new OpenSearchException("Error during sentence highlighting inference", exceptionRef.get());
        }

        // Process result
        List<Map<String, Object>> result = resultRef.get();

        // Apply highlighting to the original context
        return applyHighlighting(context, result);
    }

    /**
     * Applies highlighting to the original context based on the ML model response.
     *
     * @param context The original document text
     * @param highlightResults The highlighting results from the ML model
     * @return Formatted text with highlighting
     */
    private String applyHighlighting(String context, List<Map<String, Object>> highlightResults) {
        if (highlightResults == null || highlightResults.isEmpty()) {
            return context;
        }

        StringBuilder highlightedText = new StringBuilder(context);

        // Process each highlight result
        for (int resultIndex = highlightResults.size() - 1; resultIndex >= 0; resultIndex--) {
            Map<String, Object> result = highlightResults.get(resultIndex);

            // Get the "highlights" list from the result
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> highlights = (List<Map<String, Object>>) result.get("highlights");

            if (highlights == null || highlights.isEmpty()) {
                log.warn("No highlights found in result: {}", result);
                continue;
            }

            // Process each highlight in reverse order to avoid position shifts
            for (int i = highlights.size() - 1; i >= 0; i--) {
                Map<String, Object> highlight = highlights.get(i);

                // Extract start and end positions
                Number startNum = (Number) highlight.get("start");
                Number endNum = (Number) highlight.get("end");

                if (startNum == null || endNum == null) {
                    log.warn("Missing start or end position in highlight: {}", highlight);
                    continue;
                }

                int start = startNum.intValue();
                int end = endNum.intValue();

                // Validate positions
                if (start < 0 || end > highlightedText.length() || start >= end) {
                    log.warn("Invalid highlight position: start={}, end={}, text length={}", start, end, highlightedText.length());
                    continue;
                }

                // Insert highlighting tags
                highlightedText.insert(end, POST_TAG);
                highlightedText.insert(start, PRE_TAG);
            }
        }

        return highlightedText.toString();
    }

    private String getModelId(Map<String, Object> options) {
        Object modelId = options.get(MODEL_ID_FIELD);
        if (Objects.isNull(modelId)) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Missing required option: %s", MODEL_ID_FIELD));
        }
        return modelId.toString();
    }

    private String getFieldText(FieldHighlightContext fieldContext) {
        // Extract each query hit's field value
        String hitValue = (String) fieldContext.hitContext.sourceLookup().extractValue(fieldContext.fieldName, null);
        if (hitValue.isEmpty()) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Field %s is empty", fieldContext.fieldName));
        }
        return hitValue;
    }

    private String extractOriginalQuery(Query query) {
        String queryText = (query instanceof NeuralKNNQuery neuralQuery)
            ? neuralQuery.getOriginalQueryText()
            : query.toString().replaceAll("\\w+:", "").replaceAll("\\s+", " ").trim();

        if (queryText.isEmpty()) {
            throw new IllegalArgumentException("Original neural query text is empty");
        }
        return queryText;
    }
}
