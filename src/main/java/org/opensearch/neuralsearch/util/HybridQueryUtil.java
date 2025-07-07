/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.opensearch.index.search.NestedHelper;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.search.internal.SearchContext;

import java.util.Objects;

/**
 * Utility class for anything related to hybrid query
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HybridQueryUtil {

    /**
     * This method validates whether the query object is an instance of hybrid query
     */
    public static boolean isHybridQuery(final Query query, final SearchContext searchContext) {
        if (query instanceof HybridQuery
            || (Objects.nonNull(searchContext.parsedQuery()) && searchContext.parsedQuery().query() instanceof HybridQuery)) {
            return true;
        }
        return false;
    }

    private static boolean hasNestedFieldOrNestedDocs(final Query query, final SearchContext searchContext) {
        return searchContext.mapperService().hasNested() && new NestedHelper(searchContext.mapperService()).mightMatchNestedDocs(query);
    }

    private static boolean isWrappedHybridQuery(final Query query) {
        return query instanceof BooleanQuery
            && ((BooleanQuery) query).clauses().stream().anyMatch(clauseQuery -> clauseQuery.query() instanceof HybridQuery);
    }

    private static boolean hasAliasFilter(final Query query, final SearchContext searchContext) {
        return Objects.nonNull(searchContext.aliasFilter());
    }

    /**
     * This method checks whether hybrid query is wrapped under boolean query object
     */
    public static boolean isHybridQueryWrappedInBooleanQuery(final SearchContext searchContext, final Query query) {
        if (!(query instanceof BooleanQuery)) {
            return false;
        }

        BooleanQuery boolQuery = (BooleanQuery) query;

        // Check if there's a hybrid query in MUST clause
        boolean hasHybridQuery = boolQuery.clauses()
            .stream()
            .anyMatch(clause -> clause.occur() == BooleanClause.Occur.MUST && clause.query() instanceof HybridQuery);

        // Check if all other clauses are FILTER
        boolean onlyFilters = boolQuery.clauses()
            .stream()
            .filter(clause -> !(clause.query() instanceof HybridQuery))
            .allMatch(clause -> clause.occur() == BooleanClause.Occur.FILTER);

        return hasHybridQuery
            && onlyFilters
            && (hasAliasFilter(query, searchContext) || hasNestedFieldOrNestedDocs(query, searchContext) || !boolQuery.clauses().isEmpty());
    }
}
