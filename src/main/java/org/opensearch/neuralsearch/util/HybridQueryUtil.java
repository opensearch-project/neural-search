/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.opensearch.index.search.NestedHelper;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.search.internal.SearchContext;

import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

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
        return isHybridQueryExtendedWithDlsRules(query, searchContext)
            || (Objects.nonNull(searchContext.parsedQuery())
                && isHybridQueryExtendedWithDlsRules(searchContext.parsedQuery().query(), searchContext));
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
        return ((hasAliasFilter(query, searchContext) || hasNestedFieldOrNestedDocs(query, searchContext))
            && isWrappedHybridQuery(query)
            && !((BooleanQuery) query).clauses().isEmpty());
    }

    /**
     * This method checks whether the query object is an instance of a HybridQuery extended with DLS rules by the
     * security plugin. The security plugin returns a boolean query where the user-submitted query is preceded by
     * ConstantScoreQuery clauses.
     */
    public static boolean isHybridQueryExtendedWithDlsRules(final Query query, final SearchContext searchContext) {
        if (query instanceof BooleanQuery booleanQuery) {
            List<BooleanClause> booleanClauses = booleanQuery.clauses();
            int hybridQueryIndex = IntStream.range(0, booleanClauses.size())
                .filter(i -> booleanClauses.get(i).query() instanceof HybridQuery)
                .findFirst()
                .orElse(-1);

            if (hybridQueryIndex == -1 || booleanClauses.get(hybridQueryIndex).occur() != BooleanClause.Occur.MUST) {
                return false;
            }

            List<BooleanClause> dlsClauses = booleanClauses.subList(0, hybridQueryIndex);

            return !dlsClauses.isEmpty() && dlsClauses.stream().allMatch(clause -> {
                if (clause.query() instanceof ConstantScoreQuery && clause.occur() == BooleanClause.Occur.SHOULD) {
                    return true;
                } else if (searchContext.mapperService().hasNested()
                    && clause.query() instanceof ToChildBlockJoinQuery toChildBlockJoinQuery
                    && clause.occur() == BooleanClause.Occur.SHOULD) {
                        // security plugin may also append ToChildBlockJoinQuery
                        // https://github.com/opensearch-project/security/blob/main/src/main/java/org/opensearch/security/privileges/dlsfls/DlsRestriction.java#L88
                        return toChildBlockJoinQuery.getParentQuery() instanceof ConstantScoreQuery;
                    } else {
                        return false;
                    }
            });
        }
        return false;
    }

    /**
     * This method checks whether the query object is an instance of a hybrid query extended with DLS rules
     * by the security plugin, and wrapped in another boolean query object
     */
    public static boolean isHybridQueryExtendedWithDlsRulesAndWrappedInBoolQuery(final SearchContext searchContext, final Query query) {
        return ((hasAliasFilter(query, searchContext) || hasNestedFieldOrNestedDocs(query, searchContext))
            && query instanceof BooleanQuery booleanQuery
            && booleanQuery.clauses().stream().anyMatch(clause -> isHybridQueryExtendedWithDlsRules(clause.query(), searchContext)));
    }
}
