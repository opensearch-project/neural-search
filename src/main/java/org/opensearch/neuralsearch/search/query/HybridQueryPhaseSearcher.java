/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.NoArgsConstructor;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.common.lucene.search.function.ScriptScoreQuery;
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

import static org.opensearch.neuralsearch.util.HybridQueryUtil.extractHybridQuery;
import static org.opensearch.neuralsearch.util.HybridQueryUtil.isHybridQuery;
import static org.opensearch.neuralsearch.util.HybridQueryUtil.validateHybridQuery;
import static org.opensearch.neuralsearch.util.HybridQueryUtil.transformHybridQueryWrappedInBooleanMustQuery;

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
        Query phaseQuery;
        if (isHybridQuery(query, searchContext) == false) {
            phaseQuery = validateAndTransformQuery(searchContext, query);
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
     * @param searchContext search context
     * @param query query to validate
     */
    private Query validateAndTransformQuery(final SearchContext searchContext, final Query query) {
        if (query instanceof BooleanQuery) {
            List<BooleanClause> booleanClauses = ((BooleanQuery) query).clauses();
            // If Collapse with Inner hits is applied with HybridQuery then it for each collapsed group,
            // opensearch will fan out new query in the form of HybridQuery wrapped under Boolean Query with filters.
            // if the field is not present in the document, then it will contain a must_not clause for the inner_hits condition. Either of
            // filter clause or must not clause can exist.
            // In this case, the bulkScorer will be DefaultBulkScorer and the scorer will be hybridQueryScorer.
            // Therefore, we need to create a new Boolean query by removing hybrid query and add it subqueries in should clause to bring
            // docIdIterator and BulkScorer in sync.
            Query transformedBooleanQuery = transformHybridQueryWrappedInBooleanMustQuery(booleanClauses);
            if (transformedBooleanQuery != null) {
                return transformedBooleanQuery;
            }
        }

        // Only check for nested hybrid if the query is a compound type that could contain one.
        // Simple leaf queries (term, match, etc.) cannot contain hybrid queries.
        if (isCompoundQuery(query) && containsHybridQuery(query, getMaxDepthLimit(searchContext))) {
            throw new IllegalArgumentException(
                "hybrid query must be a top level query and cannot be wrapped into other queries."
                    + " To use scoring wrapper queries (function_score, script_score, etc.) with hybrid sub-queries,"
                    + " replace the hybrid clause with a bool query using should clauses containing the same sub-queries"
            );
        }
        return query;
    }

    /**
     * Check if a query is a compound type that could potentially contain a nested HybridQuery.
     */
    private boolean isCompoundQuery(final Query query) {
        return query instanceof BooleanQuery
            || query instanceof DisjunctionMaxQuery
            || query instanceof FunctionScoreQuery
            || query instanceof ConstantScoreQuery
            || query instanceof BoostQuery
            || query instanceof ScriptScoreQuery;
    }

    /**
     * Recursively check if a query tree contains a HybridQuery at any depth.
     * Traverses all compound query types uniformly: BooleanQuery, DisjunctionMaxQuery,
     * FunctionScoreQuery, ConstantScoreQuery, BoostQuery, and ScriptScoreQuery.
     */
    private boolean containsHybridQuery(final Query query, final int depth) {
        if (query instanceof HybridQuery) {
            return true;
        }
        if (depth <= 0) {
            log.error("reached max nested query limit while checking for hybrid query");
            return false;
        }

        if (query instanceof BooleanQuery booleanQuery) {
            for (BooleanClause clause : booleanQuery.clauses()) {
                if (containsHybridQuery(clause.query(), depth - 1)) {
                    return true;
                }
            }
        } else if (query instanceof DisjunctionMaxQuery disjunctionMaxQuery) {
            for (Query disjunct : disjunctionMaxQuery) {
                if (containsHybridQuery(disjunct, depth - 1)) {
                    return true;
                }
            }
        } else if (query instanceof FunctionScoreQuery functionScoreQuery) {
            return containsHybridQuery(functionScoreQuery.getSubQuery(), depth - 1);
        } else if (query instanceof ConstantScoreQuery constantScoreQuery) {
            return containsHybridQuery(constantScoreQuery.getQuery(), depth - 1);
        } else if (query instanceof BoostQuery boostQuery) {
            return containsHybridQuery(boostQuery.getQuery(), depth - 1);
        } else if (query instanceof ScriptScoreQuery) {
            // ScriptScoreQuery has no public getter for its inner query, use QueryVisitor
            AtomicBoolean found = new AtomicBoolean(false);
            query.visit(new org.apache.lucene.search.QueryVisitor() {
                @Override
                public org.apache.lucene.search.QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                    if (parent instanceof HybridQuery) {
                        found.set(true);
                    }
                    return this;
                }
            });
            return found.get();
        }

        return false;
    }

    private int getMaxDepthLimit(final SearchContext searchContext) {
        Settings indexSettings = searchContext.getQueryShardContext().getIndexSettings().getSettings();
        return MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.get(indexSettings).intValue();
    }

    @Override
    public AggregationProcessor aggregationProcessor(SearchContext searchContext) {
        AggregationProcessor coreAggProcessor = super.aggregationProcessor(searchContext);
        return new HybridAggregationProcessor(coreAggProcessor);
    }
}
