/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import lombok.extern.log4j.Log4j2;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.search.fetch.subphase.highlight.Highlighter;
import org.opensearch.core.common.text.Text;

/**
 * Neural highlighter that uses ML models to identify relevant text spans for highlighting
 */
@Log4j2
public class NeuralHighlighter implements Highlighter {
    public static final String NAME = "neural";

    private NeuralHighlighterManager neuralHighlighterManager;

    public void initialize(MLCommonsClientAccessor mlClient) {
        if (neuralHighlighterManager != null) {
            throw new IllegalStateException("NeuralHighlighter has already been initialized. Multiple initializations are not permitted.");
        }
        this.neuralHighlighterManager = new NeuralHighlighterManager(mlClient);
    }

    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        return true;
    }

    /**
     * Highlights a field using neural highlighting
     *
     * @param fieldContext The field context containing the query and field information
     * @return The highlighted field or null if highlighting is not possible
     */
    @Override
    public HighlightField highlight(FieldHighlightContext fieldContext) {
        if (neuralHighlighterManager == null) {
            throw new IllegalStateException("NeuralHighlighter has not been initialized");
        }

        // Extract field text
        String fieldText = neuralHighlighterManager.getFieldText(fieldContext);

        // Get model ID
        String modelId = neuralHighlighterManager.getModelId(fieldContext.field.fieldOptions().options());

        // Try to extract query text
        String originalQueryText = neuralHighlighterManager.extractOriginalQuery(fieldContext.query, fieldContext.fieldName);

        if (originalQueryText == null || originalQueryText.isEmpty()) {
            log.warn("No query text found for field {}", fieldContext.fieldName);
            return null;
        }

        // Get highlighted text - allow any exceptions from this call to propagate
        String highlightedText = neuralHighlighterManager.getHighlightedSentences(modelId, originalQueryText, fieldText);

        // Create highlight field
        Text[] fragments = new Text[] { new Text(highlightedText) };
        return new HighlightField(fieldContext.fieldName, fragments);
    }
}
