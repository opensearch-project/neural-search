/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.extractor;

import org.apache.lucene.search.Query;
import org.opensearch.index.search.OpenSearchToParentBlockJoinQuery;

public class NestedQueryTextExtractor implements QueryTextExtractor {
    private final QueryTextExtractorRegistry registry;

    public NestedQueryTextExtractor(QueryTextExtractorRegistry registry) {
        this.registry = registry;
    }

    @Override
    public String extractQueryText(Query query, String fieldName) {
        OpenSearchToParentBlockJoinQuery neuralQuery = toQueryType(query, OpenSearchToParentBlockJoinQuery.class);
        return registry.extractQueryText(neuralQuery.getChildQuery(), fieldName);
    }
}
