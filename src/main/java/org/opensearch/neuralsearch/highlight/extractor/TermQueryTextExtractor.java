/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.extractor;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

/**
 * Extractor for term queries
 */
public class TermQueryTextExtractor implements QueryTextExtractor {

    @Override
    public String extractQueryText(Query query, String fieldName) {
        TermQuery termQuery = toQueryType(query, TermQuery.class);

        Term term = termQuery.getTerm();
        // Only include terms from the field we're highlighting
        if (fieldName.equals(term.field())) {
            return term.text();
        }
        return "";
    }
}
