/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
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

    public static boolean isHybridQuery(final Query query, final SearchContext searchContext) {
        if (query instanceof HybridQuery) {
            return true;
        } else if (isWrappedHybridQuery(query)) {
            /* Checking if this is a hybrid query that is wrapped into a Bool query by core Opensearch code
            https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/search/DefaultSearchContext.java#L367-L370.
            main reason for that is performance optimization, at time of writing we are ok with loosing on performance if that's unblocks
            hybrid query for indexes with nested field types.
            in such case we consider query a valid hybrid query. Later in the code we will extract it and execute as a main query for
            this search request.
            below is sample structure of such query:

            Boolean {
                should: {
                       hybrid: {
                           sub_query1 {}
                           sub_query2 {}
                       }
                }
                filter: {
                       exists: {
                           field: "_primary_term"
                       }
                }
            }
            */
            // we have already checked if query in instance of Boolean in higher level else if condition
            return hasNestedFieldOrNestedDocs(query, searchContext) || hasAliasFilter(query, searchContext);
        }
        return false;
    }

    public static boolean hasNestedFieldOrNestedDocs(final Query query, final SearchContext searchContext) {
        return searchContext.mapperService().hasNested() && new NestedHelper(searchContext.mapperService()).mightMatchNestedDocs(query);
    }

    public static boolean isWrappedHybridQuery(final Query query) {
        return query instanceof BooleanQuery
            && ((BooleanQuery) query).clauses().stream().anyMatch(clauseQuery -> clauseQuery.getQuery() instanceof HybridQuery);
    }

    public static boolean hasAliasFilter(final Query query, final SearchContext searchContext) {
        return Objects.nonNull(searchContext.aliasFilter());
    }

    public static boolean isHybridQueryWrappedInBooleanQuery(final SearchContext searchContext, final Query query) {
        return ((hasAliasFilter(query, searchContext) || hasNestedFieldOrNestedDocs(query, searchContext))
            && isWrappedHybridQuery(query)
            && !((BooleanQuery) query).clauses().isEmpty());
    }
}
