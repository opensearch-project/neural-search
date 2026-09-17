/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import java.util.Objects;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHits;

/**
 * Carries the hits a fused hybrid assembled on the coordinator — from its leg sub-searches alone, with no second round
 * — onto the response.
 *
 * <p>In fused mode round 2 exists to turn the coordinator's fused window back into a page of documents: a query that
 * re-finds each window document by id, scored at its fused score, then fetched. For a request that needs nothing the
 * shard must compute over the full match set (no aggregations, highlight, sort, collapse, rescore, …), that round adds
 * only latency: the legs can fetch the requested fields themselves, and the coordinator already holds every window
 * document, its fused score and its order. The fused rewrite then self-erases into {@code match_none} — round 2 runs
 * empty — and hands the page it assembled here; {@link #getMergedResponse} puts it on the response on the way out, in
 * place of round 2's empty hits.
 *
 * <p>Request-scoped, like {@code FusedLegTimeoutMerger} and {@code FusedTotalHitsMerger}: created by
 * {@link HybridQuerySearchRequestFilter} for a request whose top-level query is a fused hybrid of a shape the fast path
 * can answer, handed to it as a consumer, and read once when the response passes back through the filter. A rewrite
 * that took the two-round path (a shape check failed once the legs had answered, or the request needed the Tail) reports
 * {@code null}, and the response is returned as it came. The consumer's presence is also what permits the fast path at
 * all: where the filter is not registered nothing is attached, round 2 runs as before, and nothing changes.
 *
 * <p>The swap runs <b>first</b> among the filter's response-side steps, so everything that annotates hits afterwards —
 * the fused explanation merger in particular — annotates the hits that are actually returned.
 */
public final class FusedHitsMerger {

    private volatile SearchHits assembled;

    /** Receives the hits the rewrite assembled from the legs, or {@code null} when round 2's own hits stand. */
    public interface HitsConsumer {
        void accept(SearchHits hits);
    }

    public HitsConsumer consumer() {
        return hits -> assembled = hits;
    }

    /** The response with the assembled hits in place of its own, or the response itself when nothing was assembled. */
    public SearchResponse getMergedResponse(final SearchResponse response) {
        SearchHits hits = assembled;
        if (Objects.isNull(hits)) {
            return response;
        }
        return FusedResponseRebuilder.rebuild(response, null, response.isTimedOut(), hits);
    }
}
