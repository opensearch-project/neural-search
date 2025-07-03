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

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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

    private static boolean hasAliasFilter(final SearchContext searchContext) {
        return Objects.nonNull(searchContext.aliasFilter());
    }

    /**
     * This method checks whether hybrid query is wrapped under boolean query object
     */
    public static boolean isHybridQueryWrappedInBooleanQuery(final SearchContext searchContext, final Query query) {
        return ((hasAliasFilter(searchContext) || hasNestedFieldOrNestedDocs(query, searchContext))
            && isWrappedHybridQuery(query)
            && !((BooleanQuery) query).clauses().isEmpty());
    }

    public static Query extractHybridQuery(final SearchContext searchContext) {
        Query query = searchContext.query();
        if (isHybridQueryWrappedInBooleanQuery(searchContext, query)) {
            List<BooleanClause> booleanClauses = ((BooleanQuery) query).clauses();
            if (!(booleanClauses.get(0).query() instanceof HybridQuery)) {
                throw new IllegalArgumentException("hybrid query must be a top level query and cannot be wrapped into other queries");
            }
            HybridQuery hybridQuery = (HybridQuery) booleanClauses.get(0).query();
            List<BooleanClause> filterQueries = booleanClauses.stream().skip(1).collect(Collectors.toList());
            HybridQuery hybridQueryWithFilter = new HybridQuery(hybridQuery.getSubQueries(), hybridQuery.getQueryContext(), filterQueries);
            return hybridQueryWithFilter;
        }
        return query;
    }

    public static void validateHybridQuery(final HybridQuery query) {
        for (Query innerQuery : query) {
            if (innerQuery instanceof HybridQuery) {
                throw new IllegalArgumentException("hybrid query cannot be nested in another hybrid query");
            }
        }
    }
}
