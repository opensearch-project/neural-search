/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import org.opensearch.core.action.ActionListener;

import java.util.Locale;
import java.util.Objects;

import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.tasks.Task;

import lombok.extern.log4j.Log4j2;

/**
 * An ActionFilter that automatically disables batched reduction for hybrid queries.
 *
 * This filter intercepts all search requests and checks if they contain a hybrid query.
 * If a hybrid query is detected, it unconditionally sets batchedReduceSize to Integer.MAX_VALUE
 * to disable batched reduction, regardless of any user-specified value.
 *
 * This prevents the "topDocs already consumed" error that occurs when:
 * 1. Hybrid query is executed
 * 2. Batched reduction triggers (QueryPhaseResultConsumer.consume)
 * 3. TopDocs are consumed before NormalizationProcessor can access them
 *
 * Note: The batched_reduce_size parameter is not honored for hybrid queries because
 * batched reduction is fundamentally incompatible with hybrid query processing.
 * The NormalizationProcessor requires access to all shard results simultaneously
 * to perform score normalization and combination.
 *
 * This filter works transparently without any pipeline or query configuration.
 *
 */
@Log4j2
public class HybridQuerySearchRequestFilter implements ActionFilter {

    /**
     * Value to disable batched reduction.
     * Setting batchedReduceSize to Integer.MAX_VALUE effectively disables batched reduction
     * since the buffer will never reach this threshold.
     */
    private static final int DISABLE_BATCHED_REDUCE = Integer.MAX_VALUE;

    /**
     * Order of this filter in the filter chain.
     * Lower values execute first. We use 0 to ensure this runs early.
     */
    @Override
    public int order() {
        return 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Request extends org.opensearch.action.ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        // only intercept search actions
        if (SearchAction.NAME.equals(action) && request instanceof SearchRequest) {
            SearchRequest searchRequest = (SearchRequest) request;

            // unconditionally disable batched reduction for hybrid queries
            // batched reduction is incompatible with hybrid query processing
            if (containsHybridQuery(searchRequest)) {
                if (searchRequest.getBatchedReduceSize() != DISABLE_BATCHED_REDUCE) {
                    log.debug(
                        String.format(
                            Locale.ROOT,
                            "Hybrid query detected, disabling batched reduction to prevent 'topDocs already consumed' error. "
                                + "Original batched_reduce_size: %d, new value: %d. "
                                + "Note: batched_reduce_size is not honored for hybrid queries.",
                            searchRequest.getBatchedReduceSize(),
                            DISABLE_BATCHED_REDUCE
                        )
                    );
                    searchRequest.setBatchedReduceSize(DISABLE_BATCHED_REDUCE);
                }
            }
        }
        chain.proceed(task, action, request, listener);
    }

    /**
     * Check if the search request contains a hybrid query.
     *
     * @param searchRequest the search request to check
     * @return true if the request contains a hybrid query
     */
    private boolean containsHybridQuery(SearchRequest searchRequest) {
        if (Objects.isNull(searchRequest.source())) {
            return false;
        }

        QueryBuilder query = searchRequest.source().query();
        if (Objects.isNull(query)) {
            return false;
        }

        // direct check for HybridQueryBuilder
        return query instanceof HybridQueryBuilder;
    }
}
