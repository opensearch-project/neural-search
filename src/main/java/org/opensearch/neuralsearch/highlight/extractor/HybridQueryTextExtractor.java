/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.extractor;

import org.apache.lucene.search.Query;
import org.opensearch.neuralsearch.query.HybridQuery;

import java.util.HashSet;
import java.util.Set;

/**
 * Extractor for hybrid queries that combines text from all sub-queries
 */
public class HybridQueryTextExtractor implements QueryTextExtractor {

    private final QueryTextExtractorRegistry registry;

    public HybridQueryTextExtractor(QueryTextExtractorRegistry registry) {
        this.registry = registry;
    }

    @Override
    public String extractQueryText(Query query, String fieldName) {
        HybridQuery hybridQuery = toQueryType(query, HybridQuery.class);

        // Create a set to avoid duplicates
        Set<String> queryTexts = new HashSet<>();

        // Extract text from each sub-query
        for (Query subQuery : hybridQuery.getSubQueries()) {
            String extractedText = registry.extractQueryText(subQuery, fieldName);
            if (extractedText != null && extractedText.isEmpty() == false) {
                queryTexts.add(extractedText);
            }
        }

        // Join with spaces
        return String.join(" ", queryTexts).trim();
    }
}
