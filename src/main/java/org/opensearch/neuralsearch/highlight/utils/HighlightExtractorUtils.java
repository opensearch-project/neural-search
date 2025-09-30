/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.utils;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.Query;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.neuralsearch.highlight.single.extractor.QueryTextExtractorRegistry;
import org.opensearch.neuralsearch.processor.util.ProcessorUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants.BATCH_INFERENCE;
import static org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants.CONNECTOR_MAX_BATCH_SIZE;
import static org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants.DEFAULT_BATCH_INFERENCE;

/**
 * Consolidated utility for extracting highlight configuration from various contexts.
 * Provides unified extraction logic for both single and batch semantic highlighting.
 */
@Log4j2
public class HighlightExtractorUtils {

    /**
     * Gets the field text from the document in FieldHighlightContext
     *
     * @param fieldContext The field highlight context
     * @return The field text
     * @throws IllegalArgumentException if field is not found or not a string
     */
    public static String getFieldText(FieldHighlightContext fieldContext) {
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
     * Extracts the model ID from highlight options
     *
     * @param options The options map
     * @return The model ID
     * @throws IllegalArgumentException if model ID is missing or invalid
     */
    public static String getModelId(Map<String, Object> options) {
        Object modelId = options.get(SemanticHighlightingConstants.MODEL_ID);
        if (Objects.isNull(modelId) || (modelId instanceof String) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "model_id must be a non-null string, but was %s",
                    modelId == null ? "null" : modelId.getClass().getSimpleName()
                )
            );
        }
        return (String) modelId;
    }

    /**
     * Extracts the original query text from the search query object using QueryTextExtractorRegistry.
     *
     * @param query The query object from which to extract the original text
     * @param fieldName The name of the field being highlighted
     * @param queryTextExtractorRegistry The registry for extracting query text
     * @return The extracted original query text for highlighting
     * @throws IllegalArgumentException if the extracted query text is empty
     */
    public static String extractOriginalQuery(Query query, String fieldName, QueryTextExtractorRegistry queryTextExtractorRegistry) {
        if (fieldName == null) {
            log.warn("Field name is null, extraction may be less accurate");
        }
        return queryTextExtractorRegistry.extractQueryText(query, fieldName);
    }

    /**
     * Extract semantic field name from highlighter
     * @param highlighter the highlighter configuration
     * @return field name or null if not found
     */
    public static String extractSemanticField(HighlightBuilder highlighter) {
        List<HighlightBuilder.Field> fields = Optional.ofNullable(highlighter.fields()).orElse(Collections.emptyList());

        return fields.stream()
            .filter(field -> SemanticHighlightingConstants.HIGHLIGHTER_TYPE.equals(field.highlighterType()))
            .findFirst()
            .map(HighlightBuilder.Field::name)
            .orElse(null);
    }

    /**
     * Extract model ID from highlighter options (global or field-specific)
     * @param highlighter the highlighter configuration
     * @return model ID or null if not found
     */
    public static String extractModelId(HighlightBuilder highlighter) {
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

    /**
     * Extract query text from search request
     * @param request the search request
     * @return query text or null if not found
     */
    public static String extractQueryText(SearchRequest request) {
        return Optional.ofNullable(request.source())
            .map(SearchSourceBuilder::query)
            .map(ProcessorUtils::extractQueryTextFromBuilder)
            .orElse(null);
    }

    /**
     * Extract pre tag from highlighter (from tags or options)
     * @param highlighter the highlighter configuration
     * @return pre tag
     */
    public static String extractPreTag(HighlightBuilder highlighter) {
        String[] preTags = highlighter.preTags();
        if (preTags != null && preTags.length > 0) {
            return preTags[0];
        }

        return extractHighlightOption(highlighter, SemanticHighlightingConstants.PRE_TAG, SemanticHighlightingConstants.DEFAULT_PRE_TAG);
    }

    /**
     * Extract post tag from highlighter (from tags or options)
     * @param highlighter the highlighter configuration
     * @return post tag
     */
    public static String extractPostTag(HighlightBuilder highlighter) {
        String[] postTags = highlighter.postTags();
        if (postTags != null && postTags.length > 0) {
            return postTags[0];
        }

        return extractHighlightOption(highlighter, SemanticHighlightingConstants.POST_TAG, SemanticHighlightingConstants.DEFAULT_POST_TAG);
    }

    /**
     * Extract batch inference setting from highlighter options
     * @param highlighter the highlighter configuration
     * @return true if batch inference is enabled
     */
    public static boolean extractBatchInference(HighlightBuilder highlighter) {
        // Check global highlighter options
        Map<String, Object> options = highlighter.options();
        return extractBatchInferenceFromOptions(options);
    }

    /**
     * Extract batch inference setting from options map (for use in SemanticHighlighter)
     * @param options the options map from field options or global options
     * @return true if batch inference is enabled
     */
    public static boolean extractBatchInferenceFromOptions(Map<String, Object> options) {
        if (options != null && options.containsKey(BATCH_INFERENCE)) {
            Object value = options.get(BATCH_INFERENCE);
            if (value instanceof Boolean) {
                return (Boolean) value;
            } else if (value instanceof String) {
                return Boolean.parseBoolean((String) value);
            }
        }
        return DEFAULT_BATCH_INFERENCE;
    }

    /**
     * Extract max batch size from highlighter options
     * @param highlighter the highlighter configuration
     * @return max batch size (default is from constants)
     */
    public static int extractMaxBatchSize(HighlightBuilder highlighter) {
        // Check global highlighter options
        Map<String, Object> options = highlighter.options();
        if (options != null && options.containsKey(CONNECTOR_MAX_BATCH_SIZE)) {
            Object value = options.get(CONNECTOR_MAX_BATCH_SIZE);
            if (value instanceof Number) {
                return ((Number) value).intValue();
            } else if (value instanceof String) {
                try {
                    return Integer.parseInt((String) value);
                } catch (NumberFormatException e) {
                    log.warn("Invalid {} value: {}, using default", CONNECTOR_MAX_BATCH_SIZE, value);
                }
            }
        }

        return SemanticHighlightingConstants.DEFAULT_MAX_INFERENCE_BATCH_SIZE;
    }

    /**
     * Generic method to extract highlight option (global or field-specific)
     * @param highlighter the highlighter configuration
     * @param optionKey the option key to extract
     * @param defaultValue the default value if not found
     * @return the option value or default
     */
    private static String extractHighlightOption(HighlightBuilder highlighter, String optionKey, String defaultValue) {
        // Check global highlighter options
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
