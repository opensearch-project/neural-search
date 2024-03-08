/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import lombok.AllArgsConstructor;
import org.apache.lucene.search.CollectorManager;
import org.opensearch.search.aggregations.AggregationInitializationException;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryPhaseExecutionException;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;

import java.io.IOException;
import java.util.List;

import static org.opensearch.neuralsearch.search.query.HybridQueryPhaseSearcher.isHybridQuery;

/**
 * Defines logic for pre- and post-phases of document scores collection. Responsible for registering custom
 * collector manager for hybris query (pre phase) and reducing results (post phase)
 */
@AllArgsConstructor
public class HybridAggregationProcessor implements AggregationProcessor {

    private final AggregationProcessor delegateAggsProcessor;

    @Override
    public void preProcess(SearchContext context) {
        delegateAggsProcessor.preProcess(context);

        if (isHybridQuery(context.query(), context)) {
            // adding collector manager for hybrid query
            CollectorManager collectorManager;
            try {
                collectorManager = HybridCollectorManager.createHybridCollectorManager(context);
            } catch (IOException exception) {
                throw new AggregationInitializationException("could not initialize hybrid aggregation processor", exception);
            }
            context.queryCollectorManagers().put(HybridCollectorManager.class, collectorManager);
        }
    }

    @Override
    public void postProcess(SearchContext context) {
        if (isHybridQuery(context.query(), context)) {
            // for case when concurrent search is not enabled (default as of 2.12 release) reduce for collector
            // managers is not called
            // (https://github.com/opensearch-project/OpenSearch/blob/2.12/server/src/main/java/org/opensearch/search/query/QueryPhase.java#L333-L373)
            // and we have to call it manually. This is required as we format final
            // result of hybrid query in {@link HybridTopScoreCollector#reduce}
            // when concurrent search is enabled then reduce method is called as part of the search {@see
            // ConcurrentQueryPhaseSearcher#searchWithCollectorManager}
            // corresponding call in Lucene
            // https://github.com/apache/lucene/blob/branch_9_10/lucene/core/src/java/org/apache/lucene/search/IndexSearcher.java#L700
            if (!context.shouldUseConcurrentSearch()) {
                reduceCollectorResults(context);
            }
            updateQueryResult(context.queryResult(), context);
        }

        delegateAggsProcessor.postProcess(context);
    }

    private void reduceCollectorResults(SearchContext context) {
        CollectorManager<?, ReduceableSearchResult> collectorManager = context.queryCollectorManagers().get(HybridCollectorManager.class);
        try {
            collectorManager.reduce(List.of()).reduce(context.queryResult());
        } catch (IOException e) {
            throw new QueryPhaseExecutionException(context.shardTarget(), "failed to execute hybrid query aggregation processor", e);
        }
    }

    private void updateQueryResult(final QuerySearchResult queryResult, final SearchContext searchContext) {
        boolean isSingleShard = searchContext.numberOfShards() == 1;
        if (isSingleShard) {
            searchContext.size(queryResult.queryResult().topDocs().topDocs.scoreDocs.length);
        }
    }
}
