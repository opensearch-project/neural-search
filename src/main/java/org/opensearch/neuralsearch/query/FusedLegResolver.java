/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.UnaryOperator;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.NotifyOnceListener;
import org.opensearch.index.query.BaseQueryRewriteContext;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryCoordinatorContext;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.Rewriteable;
import org.opensearch.neuralsearch.search.profile.FusedCoordinatorTimings;
import org.opensearch.transport.client.Client;

/**
 * Brings a fused hybrid's legs to the fixpoint their own coordinator rewrite would reach, once, ahead of running them — so
 * that round 1's leg sub-searches, round 2's Tail and the union count round all carry the same <b>resolved</b> builders — and
 * runs each leg the moment it is resolved, so a leg with nothing to resolve is not kept waiting on one that is inferring.
 *
 * <p><b>Why.</b> Every leg used to reach that fixpoint inside its leg sub-search, on a copy the orchestrator never saw: a
 * {@code neural} leg's {@code doRewrite} returns a <i>new</i> builder holding the vector it infers, and the user's original —
 * the instance the orchestrator holds — keeps a {@code null} supplier. A leg that filled the window is carried into the Tail
 * verbatim ({@code HybridFusionOrchestrator#legInTailForm}), so when the coordinator went on rewriting the substituted query
 * the original re-entered its inference branch and <b>called the model a second time</b> for the same text (measured: a
 * fused request needing the Tail with a window-filling {@code neural} leg incremented
 * {@code neural_query_against_knn_requests} by two). The same held for anything a leg does asynchronously on the coordinator
 * — a {@code neural_sparse} leg's inference, a semantic-field {@code neural} leg's, a {@code terms} lookup's fetch. Resolving
 * the legs here, before anything executes them, makes each of those happen once per request, and the leg sub-search's own
 * coordinator rewrite becomes a no-op for a builder that is already at its fixpoint.
 *
 * <p><b>Why here and not in core's rewrite loop.</b> The textbook shape — rewrite the legs in {@code doRewriteFused}, return a
 * copy of the hybrid when one changed and let {@link Rewriteable#rewriteAndFetch} iterate — is what classic mode does for its
 * sub-queries, and it is wrong for fused mode. Everything the filter attaches to the request's hybrid
 * ({@code HybridQuerySearchRequestFilter}: the profile, totals, hits, page and explain consumers) and every check that asks
 * whether this hybrid <i>is</i> the request's query ({@code isRequestQuery}, the reachability refusal, the rescore refusal)
 * compares <b>instances</b> against {@code source().query()}, which core keeps as the parsed object until the whole loop
 * has run. A copy returned before the fan-out is a different instance: the consumers go quiet and the refusals fire. It would
 * also spend two of core's shared {@code MAX_REWRITE_ROUNDS} per inferring leg on a budget nested fused hybrids already
 * press on. So the legs are resolved <i>inside</i> the fan-out's async action, on child contexts of their own, and the hybrid
 * being rewritten never changes identity.
 *
 * <p><b>Why each leg runs as soon as it is resolved, rather than all legs after the last one.</b> Running the legs as one
 * {@code MultiSearch} after every leg had resolved would put a {@code neural} leg's inference ahead of the lexical leg it used
 * to overlap with: round 1 went from {@code max(L, I + N)} to {@code I + max(L, N)} — measured on this plugin as +5 ms (1.33x)
 * at p50 on the common {@code match} + {@code neural} shape where no second inference was ever paid (default {@code k} and
 * totals, fast path), against the −11 ms the fix buys on the Tail shape. Dispatching a leg the moment it is resolved — the
 * lexical leg at once, the {@code neural} leg when its vector is back — keeps the old overlap while still holding every
 * resolved builder for the Tail and the count. The legs' responses are gathered into the same {@link MultiSearchResponse}
 * shape the fan-out always produced, in leg order, so nothing downstream changes.
 *
 * <p><b>How.</b> Each leg gets its own {@link QueryCoordinatorContext} built the way core builds a request's
 * ({@code IndicesService#getRewriteContext} → {@link BaseQueryRewriteContext}, then wrapped for the request), wrapped once
 * more in {@link LegPreparationRewriteContext} so that a leg that is itself a fused hybrid is left alone rather than fanned
 * out here. Core's own {@link Rewriteable#rewriteAndFetch} drives each leg to its fixpoint — executing the async actions it
 * registers between rounds exactly as it does for a request — and the legs resolve concurrently. A leg that fails to resolve,
 * or whose search fails, is reported as that leg's {@link MultiSearchResponse.Item} failure, which
 * {@code HybridFusionOrchestrator#groupLegHits} turns into the same {@code [hybrid] fused-mode sub-query N failed: …} with
 * the leg's own status that a failed leg sub-search always produced, so the failure surface is unchanged.
 *
 * <p><b>The request each leg context carries is the leg's own</b> — {@link CandidateScope#newLegRequest} for that leg, the
 * request its sub-search will run — and not the enclosing request. A rewrite may read more than the indices off it:
 * k-NN's {@code KNNQueryBuilder#doRewrite} resolves a radial query's ({@code max_distance}/{@code min_score}, no {@code k})
 * per-shard result cap from {@code getSearchRequest().source().size()} and bakes it into the builder. The leg sub-search
 * sizes its source at {@code window_size}; the enclosing request is sized at the user's page. Handing a leg the enclosing
 * request would have resolved that cap to the page (10 by default) instead of the window, silently shrinking a radial leg's
 * candidate set, and the leg sub-search's own rewrite could not have corrected it — a builder with its size already set is
 * left alone. With the leg's own request, every read a resolving builder makes — indices, options, size — agrees with what
 * the sub-search would have read, which is the invariant this whole step rests on.
 */
final class FusedLegResolver {

    private FusedLegResolver() {}

    /** What the legs came back as: the resolved builders, in leg order, and their responses in the same order. */
    record LegsRun(List<QueryBuilder> resolvedLegs, MultiSearchResponse responses) {
    }

    /**
     * Resolve each leg to its coordinator fixpoint and run it the moment it is resolved; hand every leg's resolved form and
     * response to {@code listener} once the last has answered, both in leg order.
     *
     * <p>The three fan-out spans of {@code timings} are closed here, as leaves that sum to the time this method spans:
     * {@code legsResolveWait} from the start until the <b>last</b> leg was dispatched (the resolution critical path — zero
     * when no leg had anything to resolve, every leg being dispatched at once), {@code fanOutBuild} the per-leg request
     * builds summed, and {@code fanOutWait} from the last dispatch until the last response. A leg that answers while another
     * is still resolving is counted in the first span, which is therefore elapsed time the legs overlap with, not time spent
     * resolving alone.
     *
     * @param legs       the hybrid's sub-queries as the user wrote them (never empty: parsing refuses an empty array)
     * @param parent     the context the hybrid is being rewritten under; its registries and clock are what the leg contexts
     *                   inherit
     * @param client     the client core handed to the fan-out's async action; what the leg contexts run their own async
     *                   actions with, and what the legs are searched through
     * @param scope      what each leg inherits from the request, which builds the request a leg is resolved against and runs as
     * @param windowSize the window each leg sub-search is sized to
     * @param fanOutForm the form a resolved leg runs as — the resolved builder itself, or a copy carrying what the enclosing
     *                   query hands down to a nested fused leg ({@code HybridQueryBuilder#projectResolvedConfigOntoLeg})
     * @param timings    receives the three fan-out spans
     * @param listener   receives the resolved legs and their responses, or a failure of this method's own machinery (a leg's
     *                   failure is an item failure in the responses, never a failure here)
     */
    static void resolveAndRun(
        final List<QueryBuilder> legs,
        final QueryRewriteContext parent,
        final Client client,
        final CandidateScope scope,
        final int windowSize,
        final UnaryOperator<QueryBuilder> fanOutForm,
        final FusedCoordinatorTimings timings,
        final ActionListener<LegsRun> listener
    ) {
        final long start = System.nanoTime();
        final int legCount = legs.size();
        final AtomicReferenceArray<QueryBuilder> resolved = new AtomicReferenceArray<>(legCount);
        final AtomicReferenceArray<MultiSearchResponse.Item> items = new AtomicReferenceArray<>(legCount);
        final AtomicLong lastDispatchNanos = new AtomicLong(start);
        final AtomicLong buildNanos = new AtomicLong();
        final GroupedActionListener<Integer> allAnswered = new GroupedActionListener<>(ActionListener.wrap(answered -> {
            long end = System.nanoTime();
            long lastDispatch = lastDispatchNanos.get();
            timings.legsResolveWaitNanos(lastDispatch - start).fanOutBuildNanos(buildNanos.get()).fanOutWaitNanos(end - lastDispatch);
            List<QueryBuilder> resolvedLegs = new ArrayList<>(legCount);
            MultiSearchResponse.Item[] responses = new MultiSearchResponse.Item[legCount];
            for (int legIndex = 0; legIndex < legCount; legIndex++) {
                resolvedLegs.add(resolved.get(legIndex));
                responses[legIndex] = items.get(legIndex);
            }
            listener.onResponse(
                new LegsRun(
                    Collections.unmodifiableList(resolvedLegs),
                    new MultiSearchResponse(responses, TimeUnit.NANOSECONDS.toMillis(end - start))
                )
            );
        }, listener::onFailure), legCount);
        for (int legIndex = 0; legIndex < legCount; legIndex++) {
            final int leg = legIndex;
            final QueryBuilder asWritten = legs.get(leg);
            resolveLeg(asWritten, parent, client, scope.newLegRequest(asWritten, windowSize), new NotifyOnceListener<>() {
                @Override
                protected void innerOnResponse(final QueryBuilder resolvedLeg) {
                    resolved.set(leg, resolvedLeg);
                    SearchRequest legSearch;
                    try {
                        long buildStart = System.nanoTime();
                        legSearch = scope.newLegRequest(fanOutForm.apply(resolvedLeg), windowSize);
                        buildNanos.addAndGet(System.nanoTime() - buildStart);
                    } catch (RuntimeException failure) {
                        answer(leg, new MultiSearchResponse.Item(null, failure));
                        return;
                    }
                    lastDispatchNanos.accumulateAndGet(System.nanoTime(), Math::max);
                    // Dispatched the moment this leg is resolved: a leg with nothing to resolve runs while a sibling infers.
                    // A failed search is this leg's item failure, as it was as a MultiSearch item — the request fails on it
                    // in groupLegHits with the leg's own status, after every leg has answered.
                    try {
                        client.search(
                            legSearch,
                            ActionListener.wrap(
                                response -> answer(leg, new MultiSearchResponse.Item(response, null)),
                                failure -> answer(leg, new MultiSearchResponse.Item(null, failure))
                            )
                        );
                    } catch (RuntimeException failure) {
                        answer(leg, new MultiSearchResponse.Item(null, failure));
                    }
                }

                @Override
                protected void innerOnFailure(final Exception failure) {
                    // The leg could not be resolved — a model that is not deployed, a wrapper that does not parse. Carried as
                    // written (the Tail is never built over a failed fan-out) and reported as this leg's failure.
                    resolved.set(leg, asWritten);
                    lastDispatchNanos.accumulateAndGet(System.nanoTime(), Math::max);
                    answer(leg, new MultiSearchResponse.Item(null, failure));
                }

                private void answer(final int answeredLeg, final MultiSearchResponse.Item item) {
                    items.set(answeredLeg, item);
                    allAnswered.onResponse(answeredLeg);
                }
            });
        }
    }

    /**
     * Resolve {@code legs} to their coordinator fixpoints, concurrently, and hand the resolved list to {@code listener} in leg
     * order, running nothing. The list is the original one when no leg changed, so a caller can tell. A leg that fails to
     * resolve fails the whole resolution, after every leg has answered, with the failing leg's index in the message
     * ({@link #legFailure}).
     */
    static void resolve(
        final List<QueryBuilder> legs,
        final QueryRewriteContext parent,
        final Client client,
        final CandidateScope scope,
        final int windowSize,
        final ActionListener<List<QueryBuilder>> listener
    ) {
        if (legs.isEmpty()) {
            listener.onResponse(legs);
            return;
        }
        // Indexed by leg, because GroupedActionListener collects in completion order and the legs complete in any order;
        // what it collects is the indexes of the legs that answered, read for nothing but the join.
        final AtomicReferenceArray<QueryBuilder> resolved = new AtomicReferenceArray<>(legs.size());
        final GroupedActionListener<Integer> allLegs = new GroupedActionListener<>(ActionListener.wrap(answered -> {
            List<QueryBuilder> resolvedLegs = new ArrayList<>(legs.size());
            boolean changed = false;
            for (int legIndex = 0; legIndex < legs.size(); legIndex++) {
                QueryBuilder leg = resolved.get(legIndex);
                changed |= leg != legs.get(legIndex);
                resolvedLegs.add(leg);
            }
            listener.onResponse(changed ? Collections.unmodifiableList(resolvedLegs) : legs);
        }, listener::onFailure), legs.size());
        for (int legIndex = 0; legIndex < legs.size(); legIndex++) {
            final int leg = legIndex;
            resolveLeg(legs.get(leg), parent, client, scope.newLegRequest(legs.get(leg), windowSize), new NotifyOnceListener<>() {
                @Override
                protected void innerOnResponse(final QueryBuilder resolvedLeg) {
                    resolved.set(leg, resolvedLeg);
                    allLegs.onResponse(leg);
                }

                @Override
                protected void innerOnFailure(final Exception failure) {
                    allLegs.onFailure(legFailure(leg, failure));
                }
            });
        }
    }

    /**
     * One leg to its fixpoint on a context of its own. The listener is notified exactly once (a {@link NotifyOnceListener}):
     * the joins above count each answer, so a second one — an answer followed by a routed failure, say — would complete a
     * join early with a leg missing.
     */
    private static void resolveLeg(
        final QueryBuilder leg,
        final QueryRewriteContext parent,
        final Client client,
        final SearchRequest legRequest,
        final NotifyOnceListener<QueryBuilder> listener
    ) {
        try {
            Rewriteable.rewriteAndFetch(leg, legContext(parent, client, legRequest), listener);
        } catch (RuntimeException failure) {
            // rewriteAndFetch routes IOException, IllegalArgumentException and ParsingException to the listener and lets
            // every other exception out — a builder's own IllegalStateException, say. This catch fails the leg the same way
            // for the SYNCHRONOUS part of the loop: the rounds run before the first async action, and the whole loop for a
            // leg with none. Once a leg's async action has completed on another thread, core continues the loop from that
            // thread (BaseQueryRewriteContext#executeAsyncActions → rewriteAndFetch), where an escaping exception is core's
            // to route, exactly as it is for the request's own rewrite — the leg sub-search this replaces ran the same loop.
            listener.onFailure(failure);
        }
    }

    /**
     * The context one leg is resolved under: a fresh {@link BaseQueryRewriteContext} carrying the parent's registries and
     * clock — the same shape core gives a request — wrapped for the leg's own request and then marked as preparing legs.
     */
    private static QueryRewriteContext legContext(final QueryRewriteContext parent, final Client client, final SearchRequest legRequest) {
        BaseQueryRewriteContext base = new BaseQueryRewriteContext(
            parent.getXContentRegistry(),
            parent.getWriteableRegistry(),
            client,
            parent::nowInMillis
        );
        return LegPreparationRewriteContext.wrap(new QueryCoordinatorContext(base, legRequest));
    }

    /**
     * The failure a leg sub-search would have reported for the same cause — identical text and status to
     * {@code HybridFusionOrchestrator#groupLegHits}, so a client sees one failure surface whether a leg fails while being
     * resolved or while running. The leg's own status is kept: a bad bound is the user's 400, a missing model the ML client's
     * 404, a rejection a retryable 429.
     */
    static OpenSearchStatusException legFailure(final int legIndex, final Exception cause) {
        return new OpenSearchStatusException(
            String.format(Locale.ROOT, "[hybrid] fused-mode sub-query %d failed: %s", legIndex, cause.getMessage()),
            ExceptionsHelper.status(cause),
            cause
        );
    }
}
