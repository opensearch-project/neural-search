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

import java.util.Set;

/**
 * Neural highlighter that uses ML models to identify relevant text spans for highlighting
 */
@Log4j2
public class NeuralHighlighter implements Highlighter {
    public static final String NAME = "neural";

    // Set of field types that can be highlighted
    private static final Set<String> SUPPORTED_FIELD_TYPES = Set.of("text");

    private NeuralHighlighterManager neuralHighlighterManager;

    public void initialize(MLCommonsClientAccessor mlClient) {
        if (neuralHighlighterManager != null) {
            throw new IllegalStateException("NeuralHighlighter has already been initialized. Multiple initializations are not permitted.");
        }
        this.neuralHighlighterManager = new NeuralHighlighterManager(mlClient);
    }

    /**
     * Determines if this highlighter can highlight the given field type
     *
     * @param fieldType The field type to check
     * @return True if this highlighter can highlight the field
     */
    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        return SUPPORTED_FIELD_TYPES.contains(fieldType.typeName());
    }

    /**
     * Highlights a field using neural highlighting
     *
     * @param fieldContext The field context containing the query and field information
     * @return The highlighted field
     */
    @Override
    public HighlightField highlight(FieldHighlightContext fieldContext) {
        if (neuralHighlighterManager == null) {
            throw new IllegalStateException("NeuralHighlighter has not been initialized");
        }

        // Extract field text
        String fieldText = neuralHighlighterManager.getFieldText(fieldContext);

        // Extract query text
        String searchQuery = neuralHighlighterManager.extractOriginalQuery(fieldContext.query, fieldContext.fieldName);

        // Get model ID
        String modelId = neuralHighlighterManager.getModelId(fieldContext.field.fieldOptions().options());

        // Get highlighted text
        String highlightedText = neuralHighlighterManager.getHighlightedSentences(modelId, searchQuery, fieldText);

        // Create highlight field
        Text[] fragments = new Text[] { new Text(highlightedText) };
        return new HighlightField(fieldContext.fieldName, fragments);
    }
}
