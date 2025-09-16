/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.search.fetch.subphase.highlight.Highlighter;

/**
 * Minimal semantic highlighter that validates "semantic" highlighter type
 * but delegates actual highlighting to SemanticHighlightingProcessor
 */
public class SemanticHighlighter implements Highlighter {

    public static final String NAME = SemanticHighlightingConstants.HIGHLIGHTER_TYPE;

    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        return true;
    }

    @Override
    public HighlightField highlight(FieldHighlightContext fieldContext) {
        // Return null - actual highlighting is done by SemanticHighlightingProcessor
        // This highlighter only serves to validate the "semantic" type highlight
        return null;
    }
}
