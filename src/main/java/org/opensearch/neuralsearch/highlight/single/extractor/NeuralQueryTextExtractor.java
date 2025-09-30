/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.single.extractor;

import org.apache.lucene.search.Query;
import org.opensearch.neuralsearch.query.NeuralKNNQuery;

/**
 * Extractor for neural queries
 */
public class NeuralQueryTextExtractor implements QueryTextExtractor {

    @Override
    public String extractQueryText(Query query, String fieldName) {
        NeuralKNNQuery neuralQuery = toQueryType(query, NeuralKNNQuery.class);
        return neuralQuery.getOriginalQueryText();
    }
}
