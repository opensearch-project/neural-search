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
import org.opensearch.neuralsearch.util.dto.HybridQueryWithDLSRulesDTO;
import org.opensearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Utility class for anything related to hybrid query
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Log4j2
public class HybridQueryUtil {

    //isPureHybridQuery
    //isHybridQueryWrappedBooleanQuery
    //isHybridQueryExtendedWithDlsRules
    /**
     * This method validates whether the query object is an instance of hybrid query
     */
    public static boolean isHybridQuery(final Query query, final SearchContext searchContext) {
        if (query instanceof HybridQuery
            || (Objects.nonNull(searchContext.parsedQuery()) && searchContext.parsedQuery().query() instanceof HybridQuery)) {
            return true;
        }
        return (isHybridQueryExtendedWithDlsRules(query, searchContext)
            || (Objects.nonNull(searchContext.parsedQuery())
                && isHybridQueryExtendedWithDlsRules(searchContext.parsedQuery().query(), searchContext))
            || isHybridQueryWrappedInBooleanQuery(searchContext, query));
    }

    public static Optional<Query> demo(final Query query, final SearchContext searchContext){
        //check if hybrid query is a top level query then return it
        if (query instanceof HybridQuery) {
            return Optional.of(query);
        }else if (Objects.nonNull(searchContext.parsedQuery()) && searchContext.parsedQuery().query() instanceof HybridQuery){
            return Optional.of(searchContext.parsedQuery().query());
        }

//        //Simple hybrid query
//        if (query instanceof HybridQuery
//                || (Objects.nonNull(searchContext.parsedQuery()) && searchContext.parsedQuery().query() instanceof HybridQuery)) {
//            return true;
//        }

//        BooleanQuery booleanQuery;
//        //if the query is not a boolean query then hybrid query is not present. Because there are currently only 2 known compound queries in OpenSearch: Bool and hybrid
//        if (query instanceof BooleanQuery) {
//            booleanQuery = (BooleanQuery) query;
//        }else if (searchContext.parsedQuery().query() instanceof BooleanQuery) {
//            booleanQuery = (BooleanQuery) searchContext.parsedQuery().query();
//        }else {
//            return false;
//        }

        BooleanQuery booleanQuery;
        //if the query is not a boolean query then hybrid query is not present. Because there are currently only 2 known compound queries in OpenSearch: Bool and hybrid
        if (query instanceof BooleanQuery) {
            booleanQuery = (BooleanQuery) query;
        }else if (searchContext.parsedQuery().query() instanceof BooleanQuery) {
            booleanQuery = (BooleanQuery) searchContext.parsedQuery().query();
        }else {
            return Optional.empty();
        }

        List<BooleanClause> booleanClauses = booleanQuery.clauses();
        //if clauses are empty then return null
        if (booleanClauses.isEmpty()) {
            return Optional.empty();
        }

        Optional<Query> hybridQueryExtractedFromDlsRules = demo2(searchContext, booleanQuery);
        if (hybridQueryExtractedFromDlsRules.isPresent()) {
            return hybridQueryExtractedFromDlsRules;
        }


        HybridQueryWithDLSRulesDTO hybridQueryWithDLSRulesDTO = demo5(searchContext, booleanQuery);
        if (hybridQueryWithDLSRulesDTO.getQuery().isPresent()) {
            BooleanQuery firstClauseWithBooleanQuery = hybridQueryWithDLSRulesDTO.getFirstClauseWithBooleanQuery();
            if (firstClauseWithBooleanQuery == null) {
                throw new IllegalArgumentException("Given boolean query does not contain a HybridQuery clause with DLS rules");
            }
            List<BooleanClause> filterQueries = new ArrayList<>();
            for (BooleanClause clause : booleanClauses) {
                if (clause.query() instanceof BooleanQuery) {
                    Optional<Query> hybridQueryOptional=demo2(searchContext, (BooleanQuery) clause.query());
                    if (hybridQueryOptional.isEmpty()) {
                        filterQueries.add(clause);
                    }
                }else{
                    filterQueries.add(clause);
                }
            }
            HybridQuery hybridQuery = HybridQuery.fromQueryExtendedWithDlsRules(firstClauseWithBooleanQuery, (HybridQuery) hybridQueryWithDLSRulesDTO.getQuery().get(), filterQueries);
            return Optional.of(hybridQuery);
        }

        if (demo3(searchContext,query, booleanClauses)){
            if (!(booleanClauses.getFirst().query() instanceof HybridQuery hybridQueryExtractedFromBoolQuery)) {
                throw new IllegalArgumentException("hybrid query must be a top level query and cannot be wrapped into other queries");
            }
            List<BooleanClause> filterQueries = booleanClauses.stream().skip(1).collect(Collectors.toList());
            HybridQuery hybridQuery = new HybridQuery(hybridQueryExtractedFromBoolQuery.getSubQueries(), hybridQueryExtractedFromBoolQuery.getQueryContext(), filterQueries);
            return Optional.of(hybridQuery);
        }

        return Optional.empty();
    }

    private static int demo1(final List<BooleanClause> booleanClauses) {
        return IntStream.range(0, booleanClauses.size())
                .filter(i -> booleanClauses.get(i).query() instanceof HybridQuery)
                .findFirst()
                .orElse(-1);
    }

    public static Optional<Query> demo2(final SearchContext searchContext, final BooleanQuery booleanQuery) {
        List<BooleanClause> booleanClauses = booleanQuery.clauses();
        //if clauses are empty then return null
        if (booleanClauses.isEmpty()) {
            return Optional.empty();
        }
        int hybridQueryIndex = demo1(booleanClauses);
        //Check if hybrid query is present in any of the clauses of boolean query
        if (hybridQueryIndex == -1 || booleanClauses.get(hybridQueryIndex).occur() != BooleanClause.Occur.MUST) {
            return Optional.empty();
        }

        List<BooleanClause> dlsClauses = booleanClauses.subList(0, hybridQueryIndex);

        if (demo4(dlsClauses,searchContext)){
            HybridQuery hybridQuery = unwrapHybridQueryWrappedInSecurityDlsRules(booleanQuery);
            return Optional.of(HybridQuery.fromQueryExtendedWithDlsRules(booleanQuery, hybridQuery, List.of()));
        }
        return Optional.empty();
    }

    public static boolean demo3(final SearchContext searchContext, final Query query, final List<BooleanClause> booleanClauses) {
        return isHybridQueryWrappedInBooleanMustQueryWithFilters(booleanClauses)
                || (hasAliasFilter(searchContext) || hasNestedFieldOrNestedDocs(query, searchContext) && isWrappedHybridQuery(query));
    }

    public static boolean demo4(final List<BooleanClause> dlsClauses, final SearchContext searchContext){
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

    public static HybridQueryWithDLSRulesDTO demo5(final SearchContext searchContext, final BooleanQuery booleanQuery) {
        if (hasAliasFilter(searchContext) || hasNestedFieldOrNestedDocs(booleanQuery, searchContext)){
            for (BooleanClause clause : booleanQuery.clauses()) {
                if (clause.query() instanceof BooleanQuery){
                    Optional<Query> hybridQueryOptional=demo2(searchContext, (BooleanQuery) clause.query());
                    if (hybridQueryOptional.isPresent()) {
                        return HybridQueryWithDLSRulesDTO.builder().firstClauseWithBooleanQuery((BooleanQuery) clause.query()).query(hybridQueryOptional).build();
                    }
                }
            }
        }
        return HybridQueryWithDLSRulesDTO.builder().build();
    }

    /**
     * This method checks whether hybrid query is wrapped under boolean query object
     */
    public static boolean isHybridQueryWrappedInBooleanQuery(final SearchContext searchContext, final Query query) {
        if (query instanceof BooleanQuery == false) {
            return false;
        }

        BooleanQuery boolQuery = (BooleanQuery) query;

        return isHybridQueryWrappedInBooleanMustQueryWithFilters(boolQuery.clauses())
            || ((hasAliasFilter(searchContext) || hasNestedFieldOrNestedDocs(query, searchContext))
                && isWrappedHybridQuery(query)
                && boolQuery.clauses().isEmpty() == false);
    }

    /**
     * This method checks if the hybrid query is in a MUST clause with additional FILTER clauses
     * This format is used when inner hits are passed within the collapse parameter
     * @param booleanClauses a list of boolean clauses from a boolean query
     * @return true if the clauses represent a hybrid query wrapped in a boolean must clause with the rest of the clauses being filters
     */
    public static boolean isHybridQueryWrappedInBooleanMustQueryWithFilters(List<BooleanClause> booleanClauses) {
        boolean isFirstClauseMustHybrid = !booleanClauses.isEmpty()
            && booleanClauses.getFirst().occur() == BooleanClause.Occur.MUST
            && booleanClauses.getFirst().query() instanceof HybridQuery;

        boolean areRemainingClausesFilters = booleanClauses.size() <= 1
            || booleanClauses.stream()
                .skip(1)  // Skip the first clause since we checked it above for a hybrid within a must clause
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
