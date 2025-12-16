/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import com.google.common.annotations.VisibleForTesting;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.opensearch.index.search.NestedHelper;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Utility class for anything related to hybrid query
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Log4j2
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

    /**
     * This method checks whether hybrid query is wrapped under boolean query object
     */
    public static boolean isHybridQueryWrappedInBooleanQuery(final SearchContext searchContext, final Query query) {
        if (query instanceof BooleanQuery == false) {
            return false;
        }

        BooleanQuery boolQuery = (BooleanQuery) query;
        return ((hasAliasFilter(searchContext) || hasNestedFieldOrNestedDocs(query, searchContext))
            && isWrappedHybridQuery(query)
            && boolQuery.clauses().isEmpty() == false);
    }

    /**
     * This method checks if the hybrid query is in a MUST clause with additional FILTER or MUST_NOT clauses
     * This format is used when inner hits are passed within the collapse parameter
     * @param booleanClauses list of clauses from the main query
     * @return query if the clauses represent a hybrid query wrapped in a boolean must clause with the rest of the clause being a filter or must_not
     */
    public static Query isHybridQueryWrappedInBooleanMustQueryWithFilters(List<BooleanClause> booleanClauses) {
        if (booleanClauses.isEmpty()) {
            return null;
        }
        HybridQuery hybridQuery = (booleanClauses.getFirst().occur() == BooleanClause.Occur.MUST
            && booleanClauses.getFirst().query() instanceof HybridQuery) ? (HybridQuery) booleanClauses.getFirst().query() : null;

        // Either the boolean query will contain a filter clause or must_not clause depending on the field present in the hit.
        // SourceCode:
        // https://github.com/opensearch-project/OpenSearch/blob/3.2/server/src/main/java/org/opensearch/action/search/ExpandSearchPhase.java#L94
        BooleanClause filterOrMustNotClause = null;
        for (BooleanClause booleanClause : booleanClauses.stream().skip(1).toList()) {
            if (booleanClause.occur() == BooleanClause.Occur.FILTER || booleanClause.occur() == BooleanClause.Occur.MUST_NOT) {
                filterOrMustNotClause = booleanClause;
                break;
            }
        }

        return createBoolQueryFromHybridQuery(hybridQuery, filterOrMustNotClause);
    }

    /**
     * This method creates bool query from the the hybrid query
     * This format is used when inner hits are passed within the collapse parameter
     * @param hybridQuery HQ from which subqueries need to be extracted
     * @param filterOrMustNotClause boolean clause which can be either filter or must_not
     * @return true if the clauses represent a hybrid query wrapped in a boolean must clause with the rest of the clauses being filters
     * {
     *     "bool": {
     *         "must": [
     *             {
     *                 "bool": {
     *                     "should" :[
     *                        //subqueries that were present in hybrid clause.
     *                     ]
     *                 }
     *             }
     *         ],
     *         "filter": [
     *             ...
     *         ]
     *     }
     * }
     */
    private static Query createBoolQueryFromHybridQuery(HybridQuery hybridQuery, BooleanClause filterOrMustNotClause) {
        if (hybridQuery != null && filterOrMustNotClause != null) {
            Collection<Query> subQueries = hybridQuery.getSubQueries();
            List<BooleanClause> clauses = new ArrayList<>();
            subQueries.forEach(subQuery -> {
                BooleanClause booleanClause = new BooleanClause(subQuery, BooleanClause.Occur.SHOULD);
                clauses.add(booleanClause);
            });
            BooleanQuery innerBooleanQuery = new BooleanQuery.Builder().add(clauses).build();
            return new BooleanQuery.Builder().add(innerBooleanQuery, BooleanClause.Occur.MUST).add(filterOrMustNotClause).build();
        }
        return null;
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
        return ((hasAliasFilter(searchContext) || hasNestedFieldOrNestedDocs(query, searchContext))
            && query instanceof BooleanQuery booleanQuery
            && booleanQuery.clauses().stream().anyMatch(clause -> isHybridQueryExtendedWithDlsRules(clause.query(), searchContext)));
    }

    @VisibleForTesting
    public static Query extractHybridQuery(final SearchContext searchContext, final Query query) {
        HybridQuery hybridQuery = extractHybridQuery(searchContext);
        if (isHybridQueryExtendedWithDlsRules(query, searchContext)) {
            return HybridQuery.fromQueryExtendedWithDlsRules((BooleanQuery) query, hybridQuery, List.of());
        }
        if (isHybridQueryExtendedWithDlsRulesAndWrappedInBoolQuery(searchContext, query)) {
            List<BooleanClause> booleanClauses = ((BooleanQuery) query).clauses();
            BooleanQuery queryWithDls = booleanClauses.stream()
                .filter(clause -> isHybridQueryExtendedWithDlsRules(clause.query(), searchContext))
                .findFirst()
                .map(BooleanClause::query)
                .map(BooleanQuery.class::cast)
                .orElseThrow(
                    () -> new IllegalArgumentException("Given boolean query does not contain a HybridQuery clause with DLS rules")
                );
            List<BooleanClause> filterQueries = booleanClauses.stream()
                .filter(clause -> !isHybridQueryExtendedWithDlsRules(clause.query(), searchContext))
                .toList();
            return HybridQuery.fromQueryExtendedWithDlsRules(queryWithDls, hybridQuery, filterQueries);
        }
        if (isHybridQueryWrappedInBooleanQuery(searchContext, query)) {
            List<BooleanClause> booleanClauses = ((BooleanQuery) query).clauses();
            List<BooleanClause> filterQueries = booleanClauses.stream().skip(1).collect(Collectors.toList());
            return new HybridQuery(hybridQuery.getSubQueries(), hybridQuery.getQueryContext(), filterQueries);
        }
        return query;
    }

    /**
     * Unwraps a HybridQuery from a direct query, a nested BooleanQuery, a query extended with DLS rules, or a nested query extended with DLS rules.
     */
    private static HybridQuery extractHybridQuery(final SearchContext searchContext) {
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
            if (!(booleanClauses.getFirst().query() instanceof HybridQuery)) {
                throw new IllegalArgumentException("hybrid query must be a top level query and cannot be wrapped into other queries");
            }
            hybridQuery = (HybridQuery) booleanClauses.getFirst().query();
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

    public static void validateHybridQuery(final HybridQuery query) {
        for (Query innerQuery : query.getSubQueries()) {
            if (innerQuery instanceof HybridQuery) {
                throw new IllegalArgumentException("hybrid query cannot be nested in another hybrid query");
            }
        }
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
}
