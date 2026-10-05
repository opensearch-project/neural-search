/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.explain;

import java.util.Objects;

import org.apache.lucene.search.Explanation;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHit;

/**
 * Coordinator-side {@code explain} for a fused ({@code fusion}) hybrid query: replaces each returned hit's explanation
 * with the fused breakdown the rewrite collected.
 *
 * <p>Request-scoped, and the exact counterpart of {@code FusedLegProfileMerger}: created by
 * {@code HybridQuerySearchRequestFilter}, handed to the fused {@code hybrid} the request asks about, filled in by the leg
 * fan-out callback, read once when the response comes back. It needs no search pipeline and no pipeline component —
 * {@code hybrid_score_explanation} cannot serve this path at all, because it reads its input from a
 * {@code PipelineProcessingContext} attribute that only a phase-results processor writes, and fused mode has no
 * phase-results processor and no handle on that context from the rewrite.
 *
 * <p>Round 2's own explanation is what gets replaced. It is not wrong — the {@code constant_score} clause carries the
 * fused score, so the number matches — but it describes the query the rewrite <b>substituted</b> rather than the hybrid
 * the user wrote, so on its own it reads as if an {@code _id} lookup had produced the ranking. The one part of it worth
 * keeping is a {@code rescore}: core explains that the same way for every query, and the fused breakdown replaces the
 * substituted first pass in that tree, so a rescored hit reads as any rescored query does — see
 * {@link FusedDocExplanations#explain(String, float, Explanation)}. A document round 2 returned that fusion never
 * ranked (one the Tail surfaced, at {@code 0.0}) keeps its own explanation untouched: it truthfully says the document
 * matched a non-scoring clause, and there is no fused breakdown to put there.
 *
 * <p>Unlike classic hybrid's response processor this correlates by document identity — {@code _index} plus {@code _id},
 * the same key fusion ranks by — rather than by position within a shard's hit list, so it needs no per-shard counter, no
 * detail-count assertion and no descent past core-inserted wrapper queries.
 *
 * <p>Published on the leg MultiSearch response thread and read on the response-listener thread; the reference is
 * {@code volatile} for that hand-off. The two are ordered anyway by the rewrite completing before the search phases
 * start, so what is read is always what was collected.
 */
public final class FusedExplanationMerger {

    /** What the rewrite collected, or {@code null} until it publishes (and for a request that collected nothing). */
    private volatile FusedDocExplanations explanations;

    /** A handle for the one fused hybrid this merger describes, handed to its {@code HybridQueryBuilder}. */
    public interface FusedExplanationConsumer {
        void accept(FusedDocExplanations collected);
    }

    /** The consumer to attach to the fused hybrid. Nothing collected means nothing published, and the response is untouched. */
    public FusedExplanationConsumer consumer() {
        return collected -> {
            if (Objects.isNull(collected) || collected.isEmpty()) {
                return;
            }
            explanations = collected;
        };
    }

    public boolean isEmpty() {
        return Objects.isNull(explanations) || explanations.isEmpty();
    }

    /**
     * The response with every ranked hit's explanation replaced by its fused breakdown, or the response itself when
     * nothing was collected.
     *
     * <p>Hits are mutated in place rather than rebuilt: {@code SearchHit#explanation(Explanation)} is a public setter
     * that core's own fetch phase uses, and the hits reachable here are the response's own, so there is no copy to keep
     * in sync. That is also how classic hybrid's {@code ExplanationResponseProcessor} writes its tree.
     */
    public SearchResponse getMergedResponse(final SearchResponse response) {
        if (isEmpty() || Objects.isNull(response.getHits())) {
            return response;
        }
        SearchHit[] hits = response.getHits().getHits();
        if (Objects.isNull(hits)) {
            return response;
        }
        FusedDocExplanations collected = explanations;
        for (SearchHit hit : hits) {
            if (Objects.isNull(hit.getIndex()) || Objects.isNull(hit.getId())) {
                continue;
            }
            String key = FusedDocExplanations.documentKey(hit.getIndex(), hit.getId());
            // The hit's score is handed over as it is, NaN included: a request that sorts without tracking scores has no
            // final score for the tree to describe, and {@link FusedDocExplanations#explain} is what decides that. Round 2's
            // own tree goes with it so that a rescore's explanation can be kept around the fused breakdown.
            Explanation fused = collected.explain(key, hit.getScore(), hit.getExplanation());
            if (Objects.nonNull(fused)) {
                hit.explanation(fused);
            }
        }
        return response;
    }
}
