/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import java.util.Objects;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHits;

/**
 * Carries a {@code hits.total} the fused rewrite derived from its leg sub-searches onto the response, for the requests
 * where that let round 2 run without the Tail.
 *
 * <p>The Tail (the non-scoring {@code bool{should: legs}} filter round 2 otherwise carries) exists, for a request that
 * sets nothing else, only so {@code hits.total} counts the whole leg union rather than the fused window. Core caps that
 * count at the request's {@code track_total_hits} threshold: whenever the union has more matches than the threshold the
 * response says {@code {threshold, gte}} and nothing more — see {@code SearchPhaseController.TopDocsStats#getTotalHits}.
 * A leg's own total is a lower bound on the union's and is capped the same way, so a leg that reports {@code gte} at the
 * threshold already proves what the Tail would have reported. The rewrite then drops the Tail and hands that
 * {@code {threshold, gte}} here; round 2 reports its own (window-sized) total, and {@link #getMergedResponse} puts the
 * derived one in its place on the way out.
 *
 * <p>The derived total describes round 1's view of the index, a moment before round 2 ran; the two say the same thing
 * whenever both rounds reach the same shards and see the same reader state — the assumption the two-round design already
 * makes about the ranked window. Under {@code allow_partial_search_results} a shard lost in round 2 alone is not reflected
 * in the derived bound, and a leg whose query resolves relative to the clock ({@code now}) or to another document
 * (terms lookup) may not count exactly what round 2 would have; in both the derived value remains a true statement about
 * the index the legs searched.
 *
 * <p>Request-scoped, like {@code FusedLegTimeoutMerger}: created by {@link HybridQuerySearchRequestFilter} for a request
 * whose top-level query is a fused hybrid, handed to it as a consumer, and read once when the response passes back
 * through the filter. A rewrite that kept the Tail (some other feature needed it, or no leg reached the threshold) reports
 * {@code null}, and the response is returned as it came. The consumer's presence is also what permits the rewrite to drop
 * the Tail at all: where the filter is not registered nothing is attached, so round 2 keeps the Tail and the response is
 * exactly what it was before this class existed.
 */
public final class FusedTotalHitsMerger {

    private volatile TotalHits derived;

    /** Receives the {@code hits.total} the rewrite derived from the legs, or {@code null} when round 2's own total stands. */
    public interface TotalHitsConsumer {
        void accept(TotalHits totalHits);
    }

    public TotalHitsConsumer consumer() {
        return totalHits -> derived = totalHits;
    }

    /**
     * The response with its {@code hits.total} replaced by the derived one — same hits, same {@code max_score}, same
     * sort fields and collapse metadata — or the response itself when nothing was derived.
     */
    public SearchResponse getMergedResponse(final SearchResponse response) {
        TotalHits totalHits = derived;
        if (Objects.isNull(totalHits) || Objects.isNull(response.getHits())) {
            return response;
        }
        SearchHits hits = response.getHits();
        SearchHits rebuilt = new SearchHits(
            hits.getHits(),
            totalHits,
            hits.getMaxScore(),
            hits.getSortFields(),
            hits.getCollapseField(),
            hits.getCollapseValues()
        );
        return FusedResponseRebuilder.rebuild(response, null, response.isTimedOut(), rebuilt);
    }
}
