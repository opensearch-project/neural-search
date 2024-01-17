/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHitCountCollector;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.search.NestedHelper;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryCollectorContext;
import org.opensearch.search.query.QueryCollectorManagerContext;
import org.opensearch.search.query.QueryPhase;
import org.opensearch.search.query.QueryPhaseSearcherWrapper;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;
import org.opensearch.search.rescore.RescoreContext;

import com.google.common.annotations.VisibleForTesting;

import lombok.extern.log4j.Log4j2;

/**
 * Custom search implementation to be used at {@link QueryPhase} for Hybrid Query search. For queries other than Hybrid the
 * upstream standard implementation of searcher is called.
 */
@Log4j2
public class HybridQueryPhaseSearcher extends QueryPhaseSearcherWrapper {

    private final AggregationProcessor aggregationProcessor = new HybridAggregationProcessor();

    public HybridQueryPhaseSearcher() {
        super();
    }

    public boolean searchWith(
        final SearchContext searchContext,
        final ContextIndexSearcher searcher,
        final Query query,
        final LinkedList<QueryCollectorContext> collectors,
        final boolean hasFilterCollector,
        final boolean hasTimeout
    ) throws IOException {
        if (isHybridQuery(query, searchContext)) {
            Query hybridQuery = extractHybridQuery(searchContext, query);
            return searchWithCollector(searchContext, searcher, hybridQuery, collectors, hasFilterCollector, hasTimeout);
        }
        validateQuery(searchContext, query);
        return super.searchWith(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
    }

    private boolean isHybridQuery(final Query query, final SearchContext searchContext) {
        if (query instanceof HybridQuery) {
            return true;
        } else if (isWrappedHybridQuery(query) && hasNestedFieldOrNestedDocs(query, searchContext)) {
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
            TODO Need to add logic for passing hybrid sub-queries through the same logic in core to ensure there is no latency regression */
            // we have already checked if query in instance of Boolean in higher level else if condition
            return ((BooleanQuery) query).clauses()
                .stream()
                .filter(clause -> clause.getQuery() instanceof HybridQuery == false)
                .allMatch(clause -> {
                    return clause.getOccur() == BooleanClause.Occur.FILTER
                        && clause.getQuery() instanceof FieldExistsQuery
                        && SeqNoFieldMapper.PRIMARY_TERM_NAME.equals(((FieldExistsQuery) clause.getQuery()).getField());
                });
        }
        return false;
    }

    private boolean hasNestedFieldOrNestedDocs(final Query query, final SearchContext searchContext) {
        return searchContext.mapperService().hasNested() && new NestedHelper(searchContext.mapperService()).mightMatchNestedDocs(query);
    }

    private boolean isWrappedHybridQuery(final Query query) {
        return query instanceof BooleanQuery
            && ((BooleanQuery) query).clauses().stream().anyMatch(clauseQuery -> clauseQuery.getQuery() instanceof HybridQuery);
    }

    private Query extractHybridQuery(final SearchContext searchContext, final Query query) {
        if (hasNestedFieldOrNestedDocs(query, searchContext)
            && isWrappedHybridQuery(query)
            && ((BooleanQuery) query).clauses().size() > 0) {
            // extract hybrid query and replace bool with hybrid query
            List<BooleanClause> booleanClauses = ((BooleanQuery) query).clauses();
            if (booleanClauses.isEmpty() || booleanClauses.get(0).getQuery() instanceof HybridQuery == false) {
                throw new IllegalStateException("cannot process hybrid query due to incorrect structure of top level bool query");
            }
            return booleanClauses.get(0).getQuery();
        }
        return query;
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
     * TODO add similar validation for other compound type queries like dis_max, constant_score etc.
     *
     * @param query query to validate
     */
    private void validateQuery(final SearchContext searchContext, final Query query) {
        if (query instanceof BooleanQuery) {
            List<BooleanClause> booleanClauses = ((BooleanQuery) query).clauses();
            for (BooleanClause booleanClause : booleanClauses) {
                validateNestedBooleanQuery(booleanClause.getQuery(), getMaxDepthLimit(searchContext));
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
                validateNestedBooleanQuery(booleanClause.getQuery(), level - 1);
            }
        }
    }

    @VisibleForTesting
    protected boolean searchWithCollector(
        final SearchContext searchContext,
        final ContextIndexSearcher searcher,
        final Query query,
        final LinkedList<QueryCollectorContext> collectors,
        final boolean hasFilterCollector,
        final boolean hasTimeout
    ) throws IOException {
        log.debug("searching with custom doc collector, shard {}", searchContext.shardTarget().getShardId());

        HybridCollectorManager collectorManager = HybridCollectorManager.createHybridCollectorManager(searchContext);
        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> collectorManagersByManagerClass = searchContext
            .queryCollectorManagers();
        collectorManagersByManagerClass.put(HybridCollectorManager.class, collectorManager);

        if (searchContext.size() == 0) {
            final TotalHitCountCollector collector = new TotalHitCountCollector();
            searcher.search(query, collector);
            return false;
        }
        final IndexReader reader = searchContext.searcher().getIndexReader();
        int totalNumDocs = Math.max(0, reader.numDocs());
        int numDocs = Math.min(searchContext.from() + searchContext.size(), totalNumDocs);
        final boolean shouldRescore = !searchContext.rescore().isEmpty();
        if (shouldRescore) {

            for (RescoreContext rescoreContext : searchContext.rescore()) {
                numDocs = Math.max(numDocs, rescoreContext.getWindowSize());
            }
        }

        final QuerySearchResult queryResult = searchContext.queryResult();

        final List<CollectorManager<?, ReduceableSearchResult>> collectorManagers = new ArrayList<>(
            collectorManagersByManagerClass.values()
        );

        final ReduceableSearchResult result = searcher.search(
            query,
            QueryCollectorManagerContext.createMultiCollectorManager(collectorManagers)
        );

        if (searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER && queryResult.terminatedEarly() == null) {
            queryResult.terminatedEarly(false);
        }
        result.reduce(queryResult);

        updateQueryResult(queryResult, searchContext);

        return shouldRescore;
    }

    private void updateQueryResult(final QuerySearchResult queryResult, final SearchContext searchContext) {
        boolean isSingleShard = searchContext.numberOfShards() == 1;
        if (isSingleShard) {
            searchContext.size(queryResult.queryResult().topDocs().topDocs.scoreDocs.length);
        }
    }

    private int getMaxDepthLimit(final SearchContext searchContext) {
        Settings indexSettings = searchContext.getQueryShardContext().getIndexSettings().getSettings();
        return MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.get(indexSettings).intValue();
    }
}
