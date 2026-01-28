/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.internal.SearchContext;
import static org.opensearch.neuralsearch.util.HybridQueryUtil.isHybridQuery;

/**
 * Defines logic for pre- and post-phases of hybrid query aggregation processor.
 */
@AllArgsConstructor
@Log4j2
public class HybridAggregationProcessor implements AggregationProcessor {

    private final AggregationProcessor delegateAggsProcessor;

    @Override
    public void preProcess(SearchContext context) {
        // Simply delegate the call
        delegateAggsProcessor.preProcess(context);

        if (context.minimumScore() != null && isHybridQuery(context.query(), context)) {
            // unset min_score, so it will not work for when executing sub-queries,
            // and we will retrieve it from search source to filter out final results after normalization and combination
            context.minimumScore(Float.NEGATIVE_INFINITY);
        }
    }

    @Override
    public void postProcess(SearchContext context) {
        if (context.numberOfShards() == 1 && isHybridQuery(context.query(), context)) {
            // In case of Hybrid Query single shard, the normalization process would run after the fetch phase execution.
            // The fetch phase will run right after the Query Phase and therefore need the right number size of docIds to be loaded.
            // As we add delimiter in the topdocs to segregate multiple query results,
            // therefore the right number of size will be calculated by scoreDocs length present in the topDocs.
            context.size(context.queryResult().queryResult().topDocs().topDocs.scoreDocs.length);
        }
        delegateAggsProcessor.postProcess(context);
    }
}
