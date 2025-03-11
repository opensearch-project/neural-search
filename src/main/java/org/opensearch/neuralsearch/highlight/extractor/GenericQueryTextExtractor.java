/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.extractor;

import org.apache.lucene.search.Query;

/**
 * Generic fallback extractor for query types without specialized extractors
 */
public class GenericQueryTextExtractor implements QueryTextExtractor {

    @Override
    public String extractQueryText(Query query, String fieldName) {
        return query.toString()
            .replaceAll("\\w+:", "") // Remove field prefixes like "field:"
            .replaceAll("\\s+", " ") // Normalize whitespace
            .trim();                 // Remove leading/trailing whitespace
    }
}
