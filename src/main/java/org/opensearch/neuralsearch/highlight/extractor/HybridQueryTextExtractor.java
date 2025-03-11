/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.extractor;

import org.apache.lucene.search.Query;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.neuralsearch.query.NeuralKNNQuery;

/**
 * Extractor for hybrid queries
 */
public class HybridQueryTextExtractor implements QueryTextExtractor {

    private final QueryTextExtractorRegistry registry;

    public HybridQueryTextExtractor(QueryTextExtractorRegistry registry) {
        this.registry = registry;
    }

    @Override
    public String extractQueryText(Query query, String fieldName) {
        if (!(query instanceof HybridQuery hybridQuery)) {
            return "";
        }

        // First try to find a neural query
        for (Query subQuery : hybridQuery) {
            if (subQuery instanceof NeuralKNNQuery) {
                String neuralText = ((NeuralKNNQuery) subQuery).getOriginalQueryText();
                if (!neuralText.isEmpty()) {
                    return neuralText;
                }
            }
        }

        // If no neural query text found, try traditional queries
        StringBuilder combinedText = new StringBuilder();
        for (Query subQuery : hybridQuery) {
            if (!(subQuery instanceof NeuralKNNQuery)) {
                String text = registry.extractQueryText(subQuery, fieldName);
                if (!text.isEmpty()) {
                    if (combinedText.length() > 0) {
                        combinedText.append(" ");
                    }
                    combinedText.append(text);
                }
            }
        }

        return combinedText.toString();
    }
}
