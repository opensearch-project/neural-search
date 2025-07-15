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
        if (!(query instanceof BooleanQuery)) {
            return false;
        }

        BooleanQuery boolQuery = (BooleanQuery) query;

        return isHybridQueryWrappedInBooleanMustQueryWithFilters(boolQuery.clauses())
            || ((hasAliasFilter(query, searchContext) || hasNestedFieldOrNestedDocs(query, searchContext))
                && isWrappedHybridQuery(query)
                && !((BooleanQuery) query).clauses().isEmpty());
    }

    /**
     * This method checks if the hybrid query is in a MUST clause with additional FILTER clauses
     * This format is used when inner hits are passed within the collapse parameter
     * @param booleanClauses a list of boolean clauses from a boolean query
     * @return true if the clauses represent a hybrid query wrapped in a boolean must clause with the rest of the clauses being filters
     */
    public static boolean isHybridQueryWrappedInBooleanMustQueryWithFilters(List<BooleanClause> booleanClauses) {
        boolean isFirstClauseMustHybrid = !booleanClauses.isEmpty()
            && booleanClauses.get(0).occur() == BooleanClause.Occur.MUST
            && booleanClauses.get(0).query() instanceof HybridQuery;

        boolean areRemainingClausesFilters = booleanClauses.size() <= 1
            || booleanClauses.stream()
                .skip(1)  // Skip the first clause
                .allMatch(clause -> clause.occur() == BooleanClause.Occur.FILTER);

        return isFirstClauseMustHybrid && areRemainingClausesFilters;
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

    /**
     * Unwraps a HybridQuery from a direct query, a nested BooleanQuery, a query extended with DLS rules, or a nested query extended with DLS rules.
     */
    public static HybridQuery extractHybridQuery(final SearchContext searchContext) {
        HybridQuery hybridQuery;
        Query query = searchContext.query();
        if (isHybridQueryExtendedWithDlsRules(query, searchContext)) {
            BooleanQuery booleanQuery = (BooleanQuery) query;
            hybridQuery = unwrapHybridQueryWrappedInSecurityDlsRules(booleanQuery);
        } else if (isHybridQueryExtendedWithDlsRulesAndWrappedInBoolQuery(searchContext, query)) {
            BooleanQuery booleanQuery = (BooleanQuery) query;
            hybridQuery = booleanQuery.clauses()
                .stream()
                .filter(clause -> isHybridQueryExtendedWithDlsRules(clause.query(), searchContext))
                .findFirst()
                .map(BooleanClause::query)
                .map(BooleanQuery.class::cast)
                .map(HybridQueryUtil::unwrapHybridQueryWrappedInSecurityDlsRules)
                .orElseThrow(() -> new IllegalArgumentException("Given query does not contain a HybridQuery clause with DLS rules"));

        } else if (isHybridQueryWrappedInBooleanQuery(searchContext, searchContext.query())) {
            // In case of nested fields and alias filter, hybrid query is wrapped under bool query and lies in the first clause.
            List<BooleanClause> booleanClauses = ((BooleanQuery) query).clauses();
            if (!(booleanClauses.get(0).query() instanceof HybridQuery)) {
                throw new IllegalArgumentException("hybrid query must be a top level query and cannot be wrapped into other queries");
            }
            hybridQuery = (HybridQuery) booleanClauses.get(0).query();
        } else {
            hybridQuery = (HybridQuery) query;
        }
        return hybridQuery;
    }

    private static HybridQuery unwrapHybridQueryWrappedInSecurityDlsRules(BooleanQuery booleanQuery) {
        return booleanQuery.clauses()
            .stream()
            .map(BooleanClause::query)
            .filter(clauseQuery -> clauseQuery instanceof HybridQuery)
            .map(HybridQuery.class::cast)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Given boolean query does not contain a HybridQuery clause"));
    }

}
