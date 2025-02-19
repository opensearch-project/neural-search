/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.StringUtils;
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
import java.util.concurrent.TimeUnit;

/**
 * Neural highlighter that uses ML models to identify relevant text spans for highlighting
 */
@Log4j2
public class NeuralHighlighter implements Highlighter {
    public static final String NAME = "neural";
    private static final String MODEL_ID_FIELD = "model_id";
    private static final String DEFAULT_PRE_TAG = "<em>";
    private static final String DEFAULT_POST_TAG = "</em>";
    private static final String SUPPORTED_FIELD_TYPE = "text";
    private static final String MODEL_INFERENCE_RESULT_KEY = "highlights";
    private static final String MODEL_INFERENCE_RESULT_START_KEY = "start";
    private static final String MODEL_INFERENCE_RESULT_END_KEY = "end";
    private static final int MODEL_INFERENCE_DEFAULT_TIMEOUT = 30;

    private static MLCommonsClientAccessor mlCommonsClient;

    public static void initialize(MLCommonsClientAccessor mlClient) {
        NeuralHighlighter.mlCommonsClient = mlClient;
    }

    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        return SUPPORTED_FIELD_TYPE.equals(fieldType.typeName());
    }

    @Override
    public HighlightField highlight(FieldHighlightContext fieldContext) {
        MappedFieldType fieldType = fieldContext.fieldType;
        if (canHighlight(fieldType) == false) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Field %s is not supported for neural highlighting", fieldContext.fieldName)
            );
        }

        // Extract field text and query - specific validation exceptions
        String fieldText = getFieldText(fieldContext);
        String searchQuery = extractOriginalQuery(fieldContext.query);

        // Validate options - specific validation exception
        if (fieldContext.field.fieldOptions().options() == null) {
            throw new IllegalArgumentException("Field options cannot be null");
        }

        // Get model ID - specific validation exception
        Map<String, Object> options = fieldContext.field.fieldOptions().options();
        String modelId = getModelId(options);

        // Get highlighted text from the ML model
        String highlightedSentences = getHighlightedSentences(modelId, searchQuery, fieldText);
        Text[] fragments = new Text[] { new Text(highlightedSentences) };
        return new HighlightField(fieldContext.fieldName, fragments);
    }

    /**
     * Gets highlighted text from the ML model.
     *
     * @param modelId The ID of the model to use
     * @param question The search query
     * @param context The document text
     * @return Formatted text with highlighting
     */
    private String getHighlightedSentences(String modelId, String question, String context) {
        if (mlCommonsClient == null) {
            throw new IllegalStateException("ML Commons client is not initialized in NeuralHighlighter");
        }

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
            if (!latch.await(MODEL_INFERENCE_DEFAULT_TIMEOUT, TimeUnit.SECONDS)) {
                throw new OpenSearchException("Timed out waiting for sentence highlighting inference");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OpenSearchException("Interrupted while waiting for sentence highlighting inference", e);
        }

        if (exceptionRef.get() != null) {
            throw new OpenSearchException("Error during sentence highlighting inference", exceptionRef.get());
        }

        List<Map<String, Object>> result = resultRef.get();
        if (result == null) {
            return StringUtils.EMPTY;
        }

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
        StringBuilder highlightedText = new StringBuilder(context);

        // Process each highlight result
        for (int resultIndex = highlightResults.size() - 1; resultIndex >= 0; resultIndex--) {
            Map<String, Object> result = highlightResults.get(resultIndex);
            if (result == null) {
                continue;
            }

            // Get the "highlights" list from the result
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> highlights = (List<Map<String, Object>>) result.get(MODEL_INFERENCE_RESULT_KEY);

            if (highlights == null || highlights.isEmpty()) {
                continue;
            }

            // Process each highlight in reverse order to avoid position shifts
            for (int i = highlights.size() - 1; i >= 0; i--) {
                Map<String, Object> highlight = highlights.get(i);
                if (highlight == null) {
                    continue;
                }

                // Extract start and end positions
                Object startObj = highlight.get(MODEL_INFERENCE_RESULT_START_KEY);
                Object endObj = highlight.get(MODEL_INFERENCE_RESULT_END_KEY);

                if (!(startObj instanceof Number) || !(endObj instanceof Number)) {
                    continue;
                }

                int start = ((Number) startObj).intValue();
                int end = ((Number) endObj).intValue();

                // Validate positions
                if (start < 0 || end > highlightedText.length() || start >= end) {
                    continue;
                }

                // Insert highlighting tags
                highlightedText.insert(end, DEFAULT_POST_TAG);
                highlightedText.insert(start, DEFAULT_PRE_TAG);
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
        if (fieldContext.hitContext == null || fieldContext.hitContext.sourceLookup() == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Field %s is not found in the hit", fieldContext.fieldName));
        }
        String hitValue = (String) fieldContext.hitContext.sourceLookup().extractValue(fieldContext.fieldName, null);
        if (hitValue == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Field %s is not found in the document", fieldContext.fieldName));
        }
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
