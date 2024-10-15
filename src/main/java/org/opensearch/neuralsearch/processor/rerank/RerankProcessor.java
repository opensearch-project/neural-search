/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.rerank;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.neuralsearch.processor.rerank.context.ContextSourceFetcher;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Abstract base class for reranking processors
 */
@AllArgsConstructor
public abstract class RerankProcessor implements SearchResponseProcessor {

    public static final String TYPE = "rerank";

    protected final RerankType subType;
    @Getter
    private final String description;
    @Getter
    private final String tag;
    @Getter
    private final boolean ignoreFailure;
    protected List<ContextSourceFetcher> contextSourceFetchers;
    static final protected List<RerankType> processorsWithNoContext = List.of(RerankType.BY_FIELD);

    /**
     * Generate the information that this processor needs in order to rerank.
     * Concurrently hit all contextSourceFetchers
     * @param searchRequest the search query
     * @param searchResponse the search results, in case they're relevant
     * @param listener be async
     */
    public void generateRerankingContext(
        final SearchRequest searchRequest,
        final SearchResponse searchResponse,
        final ActionListener<Map<String, Object>> listener
    ) {
        // Processors that don't require context, result on a listener infinitely waiting for a response without this check
        if (!processorRequiresContext(subType)) {
            listener.onResponse(Map.of());
        }

        Map<String, Object> overallContext = new ConcurrentHashMap<>();
        AtomicInteger successfulContexts = new AtomicInteger(contextSourceFetchers.size());
        for (ContextSourceFetcher csf : contextSourceFetchers) {
            csf.fetchContext(searchRequest, searchResponse, ActionListener.wrap(context -> {
                overallContext.putAll(context);
                if (successfulContexts.decrementAndGet() == 0) {
                    listener.onResponse(overallContext);
                }
            }, e -> { listener.onFailure(e); }));
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Given the scoring context generated by the processor and the search results,
     * rerank the search results. Do so asynchronously.
     * @param searchResponse the search results to rerank
     * @param rerankingContext the information this processor needs in order to rerank
     * @param listener be async
     */
    public abstract void rerank(
        final SearchResponse searchResponse,
        final Map<String, Object> rerankingContext,
        final ActionListener<SearchResponse> listener
    );

    @Override
    public SearchResponse processResponse(final SearchRequest request, final SearchResponse response) throws Exception {
        throw new UnsupportedOperationException("Use asyncProcessResponse unless you can guarantee to not deadlock yourself");
    }

    @Override
    public void processResponseAsync(
        final SearchRequest request,
        final SearchResponse response,
        final PipelineProcessingContext ctx,
        final ActionListener<SearchResponse> responseListener
    ) {
        try {
            generateRerankingContext(
                request,
                response,
                ActionListener.wrap(context -> { rerank(response, context, responseListener); }, e -> {
                    responseListener.onFailure(e);
                })
            );
        } catch (Exception e) {
            responseListener.onFailure(e);
        }
    }

    /**
     * There are scenarios where ranking occurs without needing context. Currently, these are the processors don't require
     * the context mapping
     * <ul>
     *     <li>
     *         ByFieldRerankProcessor - Uses the search response to get value to rescore by
     *     </li>
     * </ul>
     * @param subType The kind of rerank processor
     * @return Whether a rerank subtype needs context to perform the rescore search response action.
     */
    public static boolean processorRequiresContext(RerankType subType) {
        return !processorsWithNoContext.contains(subType);
    }
}
