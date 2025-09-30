/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.single.extractor;

import org.apache.lucene.search.Query;

import java.util.Locale;

/**
 * Interface for extracting query text from different query types
 */
public interface QueryTextExtractor {
    /**
     * Converts a query to the expected type, throwing an exception if the type doesn't match
     *
     * @param query The query to convert
     * @param expectedType The expected query type
     * @return The query cast to the expected type
     * @throws IllegalArgumentException if the query is not of the expected type
     */
    default <T extends Query> T toQueryType(Query query, Class<T> expectedType) {
        if (!expectedType.isInstance(query)) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Expected %s but got %s", expectedType.getSimpleName(), query.getClass().getSimpleName())
            );
        }
        return expectedType.cast(query);
    }

    /**
     * Extracts text from a query for highlighting
     *
     * @param query The query to extract text from
     * @param fieldName The name of the field being highlighted
     * @return The extracted query text
     */
    String extractQueryText(Query query, String fieldName);
}
