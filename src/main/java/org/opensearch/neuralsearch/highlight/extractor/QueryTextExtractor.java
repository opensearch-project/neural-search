/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.extractor;

import org.apache.lucene.search.Query;

/**
 * Interface for extracting query text from different query types
 */
public interface QueryTextExtractor {
    /**
     * Extracts text from a query for highlighting
     *
     * @param query The query to extract text from
     * @param fieldName The name of the field being highlighted
     * @return The extracted query text
     */
    String extractQueryText(Query query, String fieldName);
}
