/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import org.opensearch.core.action.ActionListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.neuralsearch.query.FusedWindowGuardRescorerBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.search.explain.FusedExplanationMerger;
import org.opensearch.neuralsearch.search.profile.FusedLegProfileMerger;
import org.opensearch.neuralsearch.util.HybridQueryUtil;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;

import lombok.extern.log4j.Log4j2;

/**
 * An ActionFilter that automatically disables batched reduction for hybrid queries, and attaches per-leg profiling to
 * fused hybrids.
 *
 * This filter intercepts all search requests and checks if they contain a hybrid query.
 * If a hybrid query is detected with search_type=dfs_query_then_fetch, the request is rejected.
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
 * The same seam carries fused mode's coordinator-side reporting — per-leg profiling, {@code explain}, and a leg cut short
 * by a soft {@code timeout}: a fused hybrid runs its legs as sub-searches during the coordinator rewrite, and this is the
 * one place that both sees the query as the user submitted it and still owns the response listener, so it can hand the legs
 * somewhere to publish what they know and merge it into the response on the way out. See {@link #attachFusedReporting}.
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
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        // only intercept search actions
        if (SearchAction.NAME.equals(action) && request instanceof SearchRequest) {
            SearchRequest searchRequest = (SearchRequest) request;

            // These workarounds apply to CLASSIC hybrid only. Resolver (fused) mode self-erases at the coordinator into
            // a standard bool query and never produces the sentinel CompoundTopDocs format, so it is DFS-compatible and
            // safe with batched reduction — the mode-aware check below skips it.
            if (containsClassicHybridQuery(searchRequest)) {
                if (searchRequest.searchType() == SearchType.DFS_QUERY_THEN_FETCH) {
                    listener.onFailure(new IllegalArgumentException(HybridQueryUtil.HYBRID_QUERY_DFS_SEARCH_TYPE_NOT_SUPPORTED_MESSAGE));
                    return;
                }
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
        chain.proceed(task, action, request, attachFusedReporting(action, request, listener));
    }

    /**
     * The response listener to proceed with: when a request carries at least one fused {@code hybrid} and there is something
     * for it to report, hand the relevant hybrids consumers writing into request-scoped mergers — the legs' profile trees,
     * what the coordinator itself spent fanning them out and fusing them, the per-leg breakdown behind each fused score, and
     * whether a soft {@code timeout} truncated a leg — and wrap the listener so what those mergers collected reaches the
     * response. Otherwise the caller's own listener, unchanged.
     *
     * <p>No side channel is needed: the {@link HybridQueryBuilder} instances reachable from {@code source().query()} here
     * are the same instances rewrite round 1 runs on, so attaching to them directly is enough. Nothing happens when there is
     * nothing to report, and nothing happens to the response when nothing was ever collected.
     *
     * <p>The three reports attach on different conditions, because they answer to different things:
     * <ul>
     *   <li>{@code profile} — every fused hybrid the request fans out, since each gets its own labelled entry in the
     *       profile section.</li>
     *   <li>{@code explain} — only when the fused hybrid <b>is</b> the request's top-level query, because a hit's
     *       explanation is a single tree describing that hit's score: for a fused hybrid nested inside a container (or one of
     *       several siblings) the fused score is an input to the enclosing query's own scoring, not the hit's score, and
     *       replacing the tree would describe the wrong thing. Those requests keep core's own explanation, which correctly
     *       describes the query round 2 ran.</li>
     *   <li>{@code timed_out} — every fused hybrid, and <b>not</b> conditional on the request having asked for a diagnostic:
     *       a leg the timeout truncated narrows the window every client's ranking comes out of, so it is part of the answer
     *       rather than part of the instrumentation. It is instead conditional on {@link #mayHitASoftTimeout}, so that a
     *       request which cannot time out costs nothing.</li>
     * </ul>
     *
     * <p>This runs before search request processors do, so it reports the hybrids the request carries <b>as submitted, with
     * {@code profile}, {@code explain} and {@code timeout} as submitted</b>. A pipeline that replaces the query — or changes
     * any of them — is
     * outside that boundary: the request still answers correctly, it simply carries no fused detail.
     */
    @SuppressWarnings("unchecked")
    private <Request extends ActionRequest, Response extends ActionResponse> ActionListener<Response> attachFusedReporting(
        final String action,
        final Request request,
        final ActionListener<Response> listener
    ) {
        if (SearchAction.NAME.equals(action) == false || (request instanceof SearchRequest) == false) {
            return listener;
        }
        SearchSourceBuilder source = ((SearchRequest) request).source();
        if (Objects.isNull(source) || Objects.isNull(source.query())) {
            return listener;
        }
        boolean profiled = source.profile();
        boolean mayTimeOut = mayHitASoftTimeout(source);
        // explain() is a nullable Boolean on the source — unset means "not explained", so it cannot be unboxed.
        boolean explained = Boolean.TRUE.equals(source.explain());
        // A fused request carrying a rescore reaches the coordinator with unranked documents demoted to the guard's
        // sentinel score; that has to be mapped back before the response is handed on. See FusedRescoreScoreNormalizer.
        boolean rescored = Objects.nonNull(source.rescores()) && source.rescores().isEmpty() == false;
        if (profiled == false && mayTimeOut == false && explained == false && rescored == false) {
            return listener;
        }
        FusedHybridFinder finder = new FusedHybridFinder();
        // accept, not visit: the finder has to be the only thing driving the descent, or a builder that recurses in its own
        // visit would carry the walk past the fused hybrid the finder just stopped at.
        finder.accept(source.query());
        if (finder.found.isEmpty()) {
            return listener;
        }
        FusedLegProfileMerger legProfileMerger = null;
        if (profiled) {
            legProfileMerger = new FusedLegProfileMerger();
        }
        FusedLegTimeoutMerger timeoutMerger = null;
        if (mayTimeOut) {
            timeoutMerger = new FusedLegTimeoutMerger();
        }
        for (int i = 0; i < finder.found.size(); i++) {
            HybridQueryBuilder hybrid = finder.found.get(i);
            if (Objects.nonNull(legProfileMerger)) {
                // One label per hybrid, shared by both consumers: the legs' entries and the coordinator's entry for the same
                // hybrid have to name it the same way for the profile section to read as one query.
                String hybridLabel = String.format(Locale.ROOT, "hybrid_%d", i);
                hybrid.legProfileConsumer(legProfileMerger.forHybrid(hybridLabel));
                hybrid.fusionTimingConsumer(legProfileMerger.forHybridTiming(hybridLabel));
            }
            if (Objects.nonNull(timeoutMerger)) {
                hybrid.legTimeoutConsumer(timeoutMerger.consumer());
            }
        }
        FusedExplanationMerger explanationMerger = null;
        // Identity, not instanceof: the finder stops at a fused hybrid, so a top-level one is necessarily the only one found,
        // and the check is exactly "the hit's score is this hybrid's fused score".
        if (explained && finder.found.get(0) == source.query()) {
            explanationMerger = new FusedExplanationMerger();
            finder.found.get(0).fusedExplanationConsumer(explanationMerger.consumer());
        }
        // Passing the gate above is not the same as having something to report: an explained request whose fused hybrid is
        // nested reaches here with no merger attached at all, since explain deliberately declines a hybrid that is not the
        // request's own query. Hand the caller's listener back untouched rather than wrapping it to do nothing.
        // The guard is installed only for a fused hybrid that IS the request's own query — the position guard in
        // HybridQueryBuilder refuses a rescore alongside any other shape — so that identity test is also the exact
        // condition under which a sentinel can appear in the hits.
        final boolean normalizeSentinel = rescored && finder.found.get(0) == source.query();
        if (Objects.isNull(legProfileMerger)
            && Objects.isNull(timeoutMerger)
            && Objects.isNull(explanationMerger)
            && normalizeSentinel == false) {
            return listener;
        }
        final FusedLegProfileMerger profiles = legProfileMerger;
        final FusedLegTimeoutMerger timeouts = timeoutMerger;
        final FusedExplanationMerger explanations = explanationMerger;
        return ActionListener.wrap(response -> {
            if ((response instanceof SearchResponse) == false) {
                listener.onResponse(response);
                return;
            }
            SearchResponse merged = (SearchResponse) response;
            // One rebuild carrying both overrides, rather than one per report: each rebuild has to re-pass every field of
            // the response it is replacing, so chaining them would multiply the places a newly-added core field could be
            // dropped from. Never clears timed_out — the response's own flag is an input to the OR, not something to
            // overwrite: round 2 can time out on its own.
            merged = FusedResponseRebuilder.rebuild(
                merged,
                Objects.nonNull(profiles) ? profiles.mergedProfileResults(merged) : null,
                merged.isTimedOut() || (Objects.nonNull(timeouts) && timeouts.anyLegTimedOut())
            );
            // After the rebuild, not before: the explanation merger writes onto the hits the response carries, and the
            // rebuild above may have replaced the response around them — so the explanations have to land on the one that is
            // actually returned. It passes the SearchHits through by reference, so this reaches the same hit instances.
            if (Objects.nonNull(explanations)) {
                merged = explanations.getMergedResponse(merged);
            }
            // Last, and on the response that is actually returned: the steps above can replace the response around the
            // hits, and the sentinel has to be gone from the one the caller sees.
            if (normalizeSentinel) {
                merged = FusedRescoreScoreNormalizer.normalize(merged, FusedWindowGuardRescorerBuilder.UNRANKED_SCORE);
            }
            listener.onResponse((Response) merged);
        }, listener::onFailure);
    }

    /**
     * Whether a leg of this request could be cut short by a soft {@code timeout}, and therefore whether it is worth walking
     * the query to find fused hybrids to watch. False for the overwhelming majority of searches, which set no timeout at all
     * — and that is the point: without this gate every search reaching this filter would pay a query-tree walk to discover
     * something that could not have happened.
     *
     * <p>Reads the request's own {@code timeout} only. The cluster-wide {@code search.default_search_timeout} would apply to
     * a leg sub-search as much as to any other search and is <b>not</b> covered here: it is unset by default, and consulting
     * it means holding cluster settings and re-reading them per request on a path whose whole justification is that it is
     * free. A cluster that sets it and a request that relies on it will see the response's own {@code timed_out} for round 2
     * and no fused contribution — the behavior as it stands today, not a new gap.
     */
    private static boolean mayHitASoftTimeout(final SearchSourceBuilder source) {
        return Objects.nonNull(source.timeout());
    }

    /**
     * Collects the fused {@code hybrid} queries this request fans out itself, in walk order. Descends for itself and keys
     * by identity, because core's {@code visit} implementations disagree about recursing — see
     * {@code HybridQueryBuilder.LegSearchCounter}, which uses the same shape for the fan-out ceiling.
     *
     * <p>The walk stops at a fused hybrid rather than descending into it. A fused hybrid below another one is a leg of
     * that one, and a leg sub-search is a search action of its own: it re-enters this filter, gets its own merger, and
     * labels its legs itself, so the outer request must not number it too. That is what makes {@code hybrid_N} mean "the
     * Nth fused hybrid this request fans out" — a contiguous numbering over siblings, with nesting expressed by the label
     * path a leg's entries carry rather than by the index. Classic hybrids are walked through, since they fan nothing out
     * and may still contain a fused one. Stopping is only sound while this visitor owns the whole descent, so it has to be
     * entered by handing the query to {@link #accept} — {@code query.visit(finder)} would let the query's own {@code visit}
     * carry on into its children after {@code accept} declined to.
     */
    private static final class FusedHybridFinder implements QueryBuilderVisitor {
        private final Set<QueryBuilder> entered = Collections.newSetFromMap(new IdentityHashMap<>());
        private final List<HybridQueryBuilder> found = new ArrayList<>();

        @Override
        public void accept(final QueryBuilder queryBuilder) {
            if (entered.add(queryBuilder) == false) {
                return;
            }
            if (queryBuilder instanceof HybridQueryBuilder && Objects.nonNull(((HybridQueryBuilder) queryBuilder).fusion())) {
                found.add((HybridQueryBuilder) queryBuilder);
                return;
            }
            queryBuilder.visit(this);
        }

        @Override
        public QueryBuilderVisitor getChildVisitor(final BooleanClause.Occur occur) {
            return this;
        }
    }

    /**
     * Check if the search request's top-level query is a CLASSIC hybrid query (i.e. a {@link HybridQueryBuilder} with no
     * {@code fusion} block). Resolver (fused) mode is intentionally excluded — it needs neither the DFS rejection nor
     * the batched-reduce disable, since it self-erases into a standard query.
     *
     * @param searchRequest the search request to check
     * @return true if the request's top-level query is a classic (non-fused) hybrid query
     */
    private boolean containsClassicHybridQuery(SearchRequest searchRequest) {
        if (Objects.isNull(searchRequest.source())) {
            return false;
        }

        QueryBuilder query = searchRequest.source().query();
        if ((query instanceof HybridQueryBuilder) == false) {
            return false;
        }

        // Fused mode self-erases at the coordinator; the classic-only workarounds must not apply to it.
        return Objects.isNull(((HybridQueryBuilder) query).fusion());
    }
}
