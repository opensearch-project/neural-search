/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.Objects;

import org.opensearch.index.query.QueryCoordinatorContext;
import org.opensearch.index.query.QueryRewriteContext;

/**
 * A coordinator rewrite context that marks the subtree being rewritten as needed for its <b>match set only</b>: nothing
 * rewritten under it will be asked to score, so a query that would fan out sub-searches purely to produce scores must
 * contribute what it matches instead.
 *
 * <p>Its one user is {@link HybridFusionQueryBuilder}, which rewrites the Tail (and the inner_hits sources) under this
 * context. Those hold the enclosing query's original leg builders, so a leg that is itself a fused
 * {@link HybridQueryBuilder} would otherwise reach {@code doRewriteFused} a <i>second</i> time and fire its legs again —
 * once as the enclosing query's leg sub-search, and once more here. That doubling compounds per nesting level: a chain
 * of fused hybrids {@code D} deep costs {@code 2^(D+1) - 2} leg sub-searches while its request body grows only linearly,
 * and near {@code D = 8} the extra rewrite rounds trip core's {@code Rewriteable.MAX_REWRITE_ROUNDS} and the user gets a
 * 500 instead of an answer. Under this context the nested fused hybrid returns {@code bool{should: legs}} — the same set
 * of documents it matches, at no fan-out — and cost becomes linear in the number of hybrids the body actually spells out
 * (see {@link HybridQueryBuilder#matchSetQuery()}).
 *
 * <p>The marker is a <b>hint, not a restriction</b>: every {@link QueryRewriteContext} method delegates unchanged, so a
 * builder that has real work to do in a rewrite (a {@code neural} query resolving its embedding through
 * {@code registerAsyncAction}, say) behaves exactly as it does under the context it wraps. Only a builder that opts in by
 * checking {@link #isMatchSetOnly} sees any difference.
 */
class MatchSetRewriteContext extends QueryCoordinatorContext {

    /**
     * Delegates every method to the coordinator context being wrapped — which is itself a {@link QueryRewriteContext},
     * so the wrapped context can stand in for the {@code rewriteContext} the superclass delegates to, and the async
     * actions registered under the marker land on the real queue.
     */
    private MatchSetRewriteContext(final QueryCoordinatorContext delegate) {
        super(delegate, delegate.getSearchRequest());
    }

    /**
     * Mark {@code queryRewriteContext} as match-set-only for the subtree about to be rewritten with the returned context.
     * Returned unchanged when there is nothing to mark (a shard-side rewrite, where no fan-out can happen anyway) or when
     * the mark is already in place (a Tail within a Tail).
     */
    static QueryRewriteContext wrap(final QueryRewriteContext queryRewriteContext) {
        QueryCoordinatorContext coordinatorContext = queryRewriteContext.convertToCoordinatorContext();
        if (Objects.isNull(coordinatorContext) || coordinatorContext instanceof MatchSetRewriteContext) {
            return queryRewriteContext;
        }
        return new MatchSetRewriteContext(coordinatorContext);
    }

    /** Whether the query being rewritten under this context is needed for its match set only. */
    static boolean isMatchSetOnly(final QueryRewriteContext queryRewriteContext) {
        return queryRewriteContext.convertToCoordinatorContext() instanceof MatchSetRewriteContext;
    }
}
