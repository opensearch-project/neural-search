/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.extractor;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import lombok.extern.log4j.Log4j2;

/**
 * Extractor for boolean queries
 */
@Log4j2
public class BooleanQueryTextExtractor implements QueryTextExtractor {

    private final QueryTextExtractorRegistry registry;

    public BooleanQueryTextExtractor(QueryTextExtractorRegistry registry) {
        this.registry = registry;
    }

    @Override
    public String extractQueryText(Query query, String fieldName) {
        BooleanQuery booleanQuery = toQueryType(query, BooleanQuery.class);

        StringBuilder sb = new StringBuilder();

        for (BooleanClause clause : booleanQuery.clauses()) {
            // Skip MUST_NOT clauses as they represent negative terms
            if (clause.isProhibited()) {
                continue;
            }

            try {
                String clauseText = registry.extractQueryText(clause.query(), fieldName);
                if (clauseText.isEmpty() == false) {
                    if (sb.isEmpty() == false) {
                        sb.append(" ");
                    }
                    sb.append(clauseText);
                }
            } catch (IllegalArgumentException e) {
                log.warn("Failed to extract text from clause {}: {}", clause, e.getMessage(), e);
            }
        }

        return sb.toString();
    }
}
