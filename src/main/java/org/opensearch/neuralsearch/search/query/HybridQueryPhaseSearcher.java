/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import lombok.NoArgsConstructor;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryCollectorContext;
import org.opensearch.search.query.QueryPhase;
import org.opensearch.search.query.QueryPhaseSearcherWrapper;

import lombok.extern.log4j.Log4j2;

import static org.opensearch.neuralsearch.util.HybridQueryUtil.isHybridQuery;
import static org.opensearch.neuralsearch.util.HybridQueryUtil.validateHybridQuery;
import static org.opensearch.neuralsearch.util.HybridQueryUtil.extractHybridQuery;
import static org.opensearch.neuralsearch.util.HybridQueryUtil.isHybridQueryWrappedInBooleanMustQueryWithFilters;

/**
 * Custom search implementation to be used at {@link QueryPhase} for Hybrid Query search. For queries other than Hybrid the
 * upstream standard implementation of searcher is called.
 */
@Log4j2
@NoArgsConstructor
public class HybridQueryPhaseSearcher extends QueryPhaseSearcherWrapper {

    /**
     * This method is called from the QueryPhase. Depending on the query we delegate the call to the hybrid query.
     * @param searchContext      search context
     * @param searcher           context index searcher
     * @param query              query
     * @param collectors         list of collectors
     * @param hasFilterCollector boolean flag for filterCollector
     * @param hasTimeout         "true" if timeout was set, "false" otherwise
     * @return
     * @throws IOException
     */
    public boolean searchWith(
        final SearchContext searchContext,
        final ContextIndexSearcher searcher,
        final Query query,
        final LinkedList<QueryCollectorContext> collectors,
        final boolean hasFilterCollector,
        final boolean hasTimeout
    ) throws IOException {
        Query phaseQuery = query;
        if (isHybridQuery(query, searchContext) == false) {
            validateQuery(searchContext, query);
        } else {
            phaseQuery = extractHybridQuery(searchContext, query);
            validateHybridQuery((HybridQuery) phaseQuery);
        }
        return super.searchWith(searchContext, searcher, phaseQuery, collectors, hasFilterCollector, hasTimeout);
    }

    /**
     * Validate the query from neural-search plugin point of view. Current main goal for validation is to block cases
     * when hybrid query is wrapped into other compound queries.
     * For example, if we have Bool query like below we need to throw an error
     * bool: {
     *   should: [
     *      match: {},
     *      hybrid: {
     *        sub_query1 {}
     *        sub_query2 {}
     *      }
     *   ]
     * }
     * TODO add similar validation for other compound type queries like constant_score, function_score etc.
     *
     * @param query query to validate
     */
    private void validateQuery(final SearchContext searchContext, final Query query) {
        if (query instanceof BooleanQuery) {
            List<BooleanClause> booleanClauses = ((BooleanQuery) query).clauses();

            // Allow hybrid query in MUST clause with additional FILTER clauses
            // This format is used when inner hits are passed within the collapse parameter
            if (isHybridQueryWrappedInBooleanMustQueryWithFilters(booleanClauses) == false) {
                for (BooleanClause booleanClause : booleanClauses) {
                    validateNestedBooleanQuery(booleanClause.query(), getMaxDepthLimit(searchContext));
                }
            }
        } else if (query instanceof DisjunctionMaxQuery) {
            for (Query disjunct : (DisjunctionMaxQuery) query) {
                validateNestedDisJunctionQuery(disjunct, getMaxDepthLimit(searchContext));
            }
        }
    }

    private void validateNestedBooleanQuery(final Query query, final int level) {
        if (query instanceof HybridQuery) {
            throw new IllegalArgumentException("hybrid query must be a top level query and cannot be wrapped into other queries");
        }
        if (level <= 0) {
            // ideally we should throw an error here but this code is on the main search workflow path and that might block
            // execution of some queries. Instead, we're silently exit and allow such query to execute and potentially produce incorrect
            // results in case hybrid query is wrapped into such bool query
            log.error("reached max nested query limit, cannot process bool query with that many nested clauses");
            return;
        }
        if (query instanceof BooleanQuery) {
            for (BooleanClause booleanClause : ((BooleanQuery) query).clauses()) {
                validateNestedBooleanQuery(booleanClause.query(), level - 1);
            }
        }
    }

    private void validateNestedDisJunctionQuery(final Query query, final int level) {
        if (query instanceof HybridQuery) {
            throw new IllegalArgumentException("hybrid query must be a top level query and cannot be wrapped into other queries");
        }
        if (level <= 0) {
            // ideally we should throw an error here but this code is on the main search workflow path and that might block
            // execution of some queries. Instead, we're silently exit and allow such query to execute and potentially produce incorrect
            // results in case hybrid query is wrapped into such dis_max query
            log.error("reached max nested query limit, cannot process dis_max query with that many nested clauses");
            return;
        }
        if (query instanceof DisjunctionMaxQuery) {
            for (Query disjunct : (DisjunctionMaxQuery) query) {
                validateNestedDisJunctionQuery(disjunct, level - 1);
            }
        }
    }

    private int getMaxDepthLimit(final SearchContext searchContext) {
        Settings indexSettings = searchContext.getQueryShardContext().getIndexSettings().getSettings();
        return MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.get(indexSettings).intValue();
    }

    @Override
    public AggregationProcessor aggregationProcessor(SearchContext searchContext) {
        AggregationProcessor coreAggProcessor = super.aggregationProcessor(searchContext);
        // In case of single shard only we initialize HybridAggregationProcessor.
        // We need HybridAggregationProcessor to update size in SearchContext with scoreDocs length during the postProcess.
        if (isHybridQuery(searchContext.query(), searchContext) && searchContext.numberOfShards() == 1) {
            return new HybridAggregationProcessor(coreAggProcessor);
        }
        // In case of Hybrid query with multiple shards and all other queries we need to delegate the call to
        // either defaultAggregationProcessor or ConcurrentAggregationProcessor.
        return coreAggProcessor;
    }
}
