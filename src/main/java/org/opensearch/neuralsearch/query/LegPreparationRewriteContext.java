/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.Objects;

import org.opensearch.index.query.QueryCoordinatorContext;
import org.opensearch.index.query.QueryRewriteContext;

/**
 * A coordinator rewrite context that marks the subtree being rewritten as a fused hybrid's <b>leg being prepared</b> for
 * the fan-out: brought to the fixpoint its own coordinator rewrite would reach — a {@code neural} leg's vector resolved, a
 * {@code neural_sparse} leg's tokens, a {@code terms} lookup fetched, a {@code wrapper} parsed — but not executed.
 *
 * <p>Its one user is {@link FusedLegResolver}, which {@link HybridQueryBuilder#doRewriteFused} runs over the legs ahead of
 * the leg {@code MultiSearch}. Without the preparation every leg reached its fixpoint <i>inside</i> its leg sub-search, on a
 * copy the orchestrator never saw, so a leg that filled the window was carried into round 2's Tail as the user's original
 * builder and did its asynchronous work a second time — for a {@code neural} leg, a second model inference per request.
 * Preparing the legs once, up front, gives the fan-out, the Tail and the count round the same resolved builders.
 *
 * <p>The marker exists for one builder: a leg that is itself a fused {@link HybridQueryBuilder}. Its {@code doRewriteFused}
 * would otherwise take the preparation for a request of its own, fan out its legs on this coordinator and self-erase into
 * a standard query before the enclosing fan-out had fired — a different execution model from the one it has today, where it
 * runs as its leg sub-search's own request query (and resolves <i>its</i> legs there) and contributes its match set to the
 * enclosing Tail. Under this context a fused hybrid returns itself unchanged.
 *
 * <p>Like {@link MatchSetRewriteContext}, the marker is a <b>hint, not a restriction</b>: every {@link QueryRewriteContext}
 * method delegates unchanged, so every other builder rewrites exactly as it does under the context it wraps, and the async
 * actions it registers land on the wrapped context's queue.
 */
final class LegPreparationRewriteContext extends QueryCoordinatorContext {

    /**
     * Delegates every method to the coordinator context being wrapped — which is itself a {@link QueryRewriteContext}, so it
     * can stand in for the {@code rewriteContext} the superclass delegates to, and the request it reports is the one the
     * legs are prepared for.
     */
    private LegPreparationRewriteContext(final QueryCoordinatorContext delegate) {
        super(delegate, delegate.getSearchRequest());
    }

    /**
     * Mark {@code queryRewriteContext} as preparing legs for the subtree about to be rewritten with the returned context.
     * Returned unchanged when there is nothing to mark (a shard-side rewrite, where no fan-out can happen anyway) or when the
     * mark is already in place.
     */
    static QueryRewriteContext wrap(final QueryRewriteContext queryRewriteContext) {
        QueryCoordinatorContext coordinatorContext = queryRewriteContext.convertToCoordinatorContext();
        if (Objects.isNull(coordinatorContext) || coordinatorContext instanceof LegPreparationRewriteContext) {
            return queryRewriteContext;
        }
        return new LegPreparationRewriteContext(coordinatorContext);
    }

    /** Whether the query being rewritten under this context is a fused hybrid's leg being prepared for the fan-out. */
    static boolean isPreparingLegs(final QueryRewriteContext queryRewriteContext) {
        return queryRewriteContext.convertToCoordinatorContext() instanceof LegPreparationRewriteContext;
    }
}
