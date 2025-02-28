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

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Neural highlighter that uses ML models to identify relevant text spans for highlighting
 */
@Log4j2
public class NeuralHighlighter implements Highlighter {
    public static final String NAME = "neural";
    private static final String MODEL_ID_FIELD = "model_id";

    private static MLCommonsClientAccessor mlCommonsClient;

    public static void initialize(MLCommonsClientAccessor mlClient) {
        NeuralHighlighter.mlCommonsClient = mlClient;
    }

    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        // TODO: Implement actual condition check in subsequent PR
        return true;
    }

    @Override
    public HighlightField highlight(FieldHighlightContext fieldContext) {
        try {
            String fieldText = getFieldText(fieldContext);
            if (fieldText.isEmpty()) {
                return null;
            }

            String searchQuery = extractOriginalQuery(fieldContext.query);
            if (searchQuery.isEmpty()) {
                return null;
            }

            Map<String, Object> options = fieldContext.field.fieldOptions().options();
            String modelId = getModelId(options);
            log.info("Using model ID: {}", modelId); // Will be replaced with actual model loading logic
            log.info("Using ML client: {}", mlCommonsClient); // Will be replaced with actual model loading logic

            // TODO: Implement actual highlighting logic in subsequent PR
            // For now, return a basic highlight of the field text
            Text[] fragments = new Text[] { new Text(formatHighlight(fieldText)) };
            return new HighlightField(fieldContext.fieldName, fragments);
        } catch (Exception e) {
            throw new OpenSearchException(
                String.format(Locale.ROOT, "Failed to perform neural highlighting for field %s", fieldContext.fieldName),
                e
            );
        }
    }

    private String getModelId(Map<String, Object> options) {
        Object modelId = options.get(MODEL_ID_FIELD);
        if (Objects.isNull(modelId)) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Missing required option: %s", MODEL_ID_FIELD));
        }
        return modelId.toString();
    }

    private String getFieldText(FieldHighlightContext fieldContext) {
        Object value = fieldContext.hitContext.sourceLookup().extractValue(fieldContext.fieldName, null);
        return value != null ? value.toString() : "";
    }

    private String formatHighlight(String text) {
        // TODO: Implement user provided format options in subsequent PR
        return "<em>" + text + "</em>";
    }

    private String extractOriginalQuery(Query query) {
        if (query instanceof NeuralKNNQuery neuralQuery) {
            String originalText = neuralQuery.getOriginalQueryText();
            if (originalText != null) {
                return originalText;
            }
        }

        return query.toString().replaceAll("\\w+:", "").replaceAll("\\s+", " ").trim();
    }
}
