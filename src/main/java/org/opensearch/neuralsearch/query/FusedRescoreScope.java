/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.apache.lucene.search.Query;
import org.opensearch.common.SetOnce;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.rescore.RescorerBuilder;

/**
 * Confines every {@code rescore} in a fused-mode request to the fused window, so a rescore query cannot match a document
 * the fusion never ranked and therefore cannot lift one above the documents it did rank.
 *
 * <p><b>What this does not do, stated first because the distinction is easy to lose.</b> It bounds what the rescore
 * query can <b>match</b>. It does not bound the arithmetic core applies to the scores afterwards, and ranked documents
 * are separated from the non-scoring Tail by nothing but {@link HybridFusionOrchestrator#MIN_RANKED_SCORE}. Core's
 * {@code QueryRescorer} multiplies the first-pass score of every document in the rescore window by
 * {@code query_weight} whether the rescore query matched it or not, does the same beyond that window, and then
 * re-sorts with a comparator whose score ties break by <b>ascending Lucene doc id</b>. So a request whose own weights
 * land a ranked document on exactly {@code 0.0f} makes it indistinguishable from a Tail-only document, and the page
 * can contain documents fusion did not rank — MEASURED at 20 shards with no window named anywhere in the request:
 * {@code query_weight: 0} returned two documents from outside a default 100-document window on a six-hit page, and
 * {@code query_weight: -1} returned a page of which none were ranked. The same request on one shard was unaffected,
 * because the exposure is the per-shard shortfall: core sizes the rescore window from the shard's own reader while the
 * fused window is coordinator-global.
 *
 * <p><b>Where that is closed.</b> Not here, and not by refusing the weights — they are legal core parameters and core
 * applies the identical arithmetic to an ordinary non-hybrid query (measured: a plain {@code function_score} at 20
 * shards returns mid-ranked documents for {@code query_weight: 0} and low-ranked ones for {@code -1}). It is closed one
 * layer down, on the shard, by {@link FusedWindowGuardRescorer}: it reads which documents fusion ranked from the pristine
 * pool, lets the request's own rescorers run over that pool untouched, and then <b>demotes</b> the unranked ones into a
 * score band strictly below every ranked document — so no arithmetic the request chose can order one above the window.
 * {@code FusedRescoreScoreNormalizer} maps that band's sentinel back to {@code 0.0} on the coordinator, the score those
 * documents already carry without a rescore, so the band never reaches the response. This class therefore bounds what the
 * rescore query can <b>match</b> and the guard bounds what it can <b>promote</b>; together they make the guarantee hold
 * for any weights.
 *
 * <p>The guard demotes rather than removes, and that is not a preference: core's {@code RescoreProcessor} reads
 * {@code scoreDocs[0].score} on the rescorer's <i>output</i> with no length check, so a rescorer that returned an
 * <b>empty</b> array would raise an {@code ArrayIndexOutOfBoundsException} as a shard failure. Empty is the whole hazard —
 * index 0 is the only one read, so a shorter but non-empty return trips nothing. Demotion also costs the paradigm
 * nothing on its own terms — nothing leaves the pool, so {@code total_hits} and aggregations, computed over the full leg
 * union, cannot be affected at all.
 *
 * <p><b>Why this is needed at all.</b> {@code rescore} is {@link CandidateScope.Disposition#NOT_PROPAGATED} — it is never
 * pushed down to a leg, because a leg is a plain search whose scores are about to be normalized away. So it runs where
 * core always runs it, once per shard against the query round 2 actually executes: the self-erased
 * {@code bool{Top + Tail}}. The Tail is a {@code filter}, which means every document any leg matched is a candidate
 * there, not only the fused window — and core's {@code QueryRescorer} re-scores the shard's top {@code window_size}
 * documents, ordering them by their round-2 score. A Tail-only document sits at the floor, but it is still <i>in</i> that
 * pool, so a rescore query that matches it lifts it above the fused window and returns a document fusion never ranked.
 * Two request-independent multipliers make that easy to hit: a {@code rescore.window_size} larger than the fused window
 * admits Tail-only documents by construction, and core sizes the rescore window from the <i>shard's</i> reader while the
 * fused window is coordinator-global, so an index with more shards than {@code window_size / rescore window} admits them
 * at stock defaults.
 *
 * <p><b>The fix.</b> Each {@code rescore.query} is rewritten to {@code bool{must: <the user's query>, filter: <the fused
 * window>}} — {@link HybridFusionQueryBuilder#fusedWindowFilter}. A document outside the window then matches nothing to
 * be lifted by, whatever the rescore window is, which closes both multipliers rather than narrowing them: neither can
 * widen what the rescore query matches. The user's query keeps its scoring contribution untouched — it is the sole
 * {@code must}, so the bool's score is exactly its score, and the window rides along as a non-scoring {@code filter}.
 * Rewriting the query rather than the page is also what makes one fix cover core's <i>other</i> application site:
 * {@code TopHitsAggregator} widens every {@code top_hits} bucket's collector by {@code rescore.window_size} and rescores
 * that bucket with the request's own rescore contexts.
 *
 * <p><b>Why the window arrives late, and why that dictates this shape.</b> The window is not known until the leg
 * {@code MultiSearch} comes back, which is an async action — so the obvious implementation (fuse, then edit
 * {@code source.rescores()} from the callback) writes to the rescore list <i>after</i> core has already snapshotted it.
 * {@code SearchSourceBuilder#rewrite} rewrites the query <i>before</i> the rescorer list, both within its one pass, and
 * {@code Rewriteable#rewrite(List, ...)} returns a <i>new</i> {@code ArrayList} the moment any element changed
 * identity — which {@code QueryRescorerBuilder#rewrite} does whenever the user's own rescore query rewrote
 * ({@code wrapper}, {@code neural}, {@code neural_sparse}, a {@code terms} lookup, ...). A callback-time edit then lands
 * on a list no longer connected to the request being dispatched, and confines nothing at all, silently.
 *
 * <p>So the wrapping happens in <b>round 1</b> of {@code HybridQueryBuilder#doRewriteFused} — inside that same query
 * rewrite, therefore strictly before the rescorer list is snapshotted — and what is installed as the filter is a
 * {@link WindowPlaceholder}, a query builder that resolves to the real window on a later rewrite pass. Since
 * the placeholder <i>object</i> travels inside whatever list core builds, list identity stops mattering: core carries it
 * into every {@code shallowCopy}, and the pass after the async action rewrites it into the window filter, in place, in
 * core's own copy.
 *
 * <p><b>Fail-closed.</b> Installation still assumes {@code searchRequest.source()} is the source core is rewriting on
 * this pass — the same assumption every other fused-mode read of the request makes. If it ever breaks (a
 * search-pipeline request processor handing back a different {@code SearchRequest} whose source is a deep copy), the
 * placeholders are never rewritten, which {@link #requireReachedTheExecutedRequest} detects at round 2 and turns into a
 * failed request. The one thing that must not happen — returning results with an unconfined rescore — is the one thing
 * that cannot.
 *
 * <p><b>Only sound for a top-level fused hybrid, and enforced upstream.</b> This confines the rescore to the fused
 * window, which is the request's ranking only when the hybrid <i>is</i> the request's {@code query}. Composed into another
 * query — a {@code bool} {@code should}/{@code must}/{@code must_not}, or alongside a second fused hybrid — the window is a
 * different set from what the request ranks, so the confinement would silently misapply the rescore: inert over a
 * {@code must_not}'s survivors (the window is the excluded set, disjoint from the result), partial over a {@code should}'s
 * siblings, or the intersection of two windows for two fused hybrids. Those shapes are refused before this is installed, in
 * {@code HybridQueryBuilder#doRewriteFused} — so by the time this runs the fused hybrid is the request's {@code query} and
 * its window is exactly the set the rescore may touch. The general, position-aware confinement is a separate feature.
 */
final class FusedRescoreScope {

    /** Named in the refusal below, and not reachable from here: {@code HybridQueryBuilder.FUSION_FIELD} is private. */
    private static final String FUSION = "fusion";

    /** The fused window, once the legs have come back. Shared by every placeholder this scope installed. */
    private final SetOnce<QueryBuilder> window = new SetOnce<>();
    /** Every placeholder installed into the request, so round 2 can verify core actually rewrote them. */
    private final List<WindowPlaceholder> placeholders = new ArrayList<>();

    /**
     * Wrap every rescore query in the request so that it will be confined to this hybrid's fused window once that window
     * is known. Call this from round 1, before the leg fan-out is registered — see the class javadoc for why the timing is
     * the whole design.
     *
     * @param source the request's source, mutated in place — but only once the whole chain has been accepted
     * @return the scope to {@link #resolve} from the async callback, or {@code null} when the request has no rescore and
     *         there is nothing to confine
     */
    static FusedRescoreScope install(final SearchSourceBuilder source) {
        if (Objects.isNull(source) || Objects.isNull(source.rescores()) || source.rescores().isEmpty()) {
            return null;
        }
        FusedRescoreScope scope = new FusedRescoreScope();
        List<RescorerBuilder> rescores = source.rescores();
        // Every rescorer in the chain, not just the first: core applies them in sequence and each one can lift.
        List<QueryRescorerBuilder> confined = new ArrayList<>(rescores.size());
        for (RescorerBuilder<?> declared : rescores) {
            confined.add(scope.confined(requireQueryRescorer(declared)));
        }
        // Written back only once the whole chain has been accepted, so a refusal leaves the request's own rescore list as
        // the user wrote it rather than half-replaced with placeholders that can never resolve. The whole chain collapses
        // to ONE element — the guard — which applies the confined delegates in order and then restores the
        // ranked/unranked separation their arithmetic can destroy; see FusedWindowGuardRescorer for why the guard has to
        // read membership once, ahead of the chain, rather than once per element.
        rescores.set(0, new FusedWindowGuardRescorerBuilder(confined));
        while (rescores.size() > 1) {
            rescores.remove(rescores.size() - 1);
        }
        return scope;
    }

    /**
     * Hand the fused window to every placeholder this scope installed. Called from the leg {@code MultiSearch} callback,
     * once per fused hybrid.
     *
     * <p>Anything other than a {@link HybridFusionQueryBuilder} means nothing fused, so round 2 is a {@code match_none}
     * with no hits for a rescore to reorder — but the placeholders still have to resolve to something, since an
     * unresolved one would travel to the shards. An empty window is exactly {@code match_none}, which is also what
     * {@link HybridFusionQueryBuilder#fusedWindowFilter} returns for an empty window, so the two cases agree.
     *
     * @param fusedQuery what {@link HybridFusionOrchestrator#buildFusedQuery} produced for this hybrid
     */
    void resolve(final QueryBuilder fusedQuery) {
        window.set(fusedQuery instanceof HybridFusionQueryBuilder fused ? fused.fusedWindowFilter() : new MatchNoneQueryBuilder());
    }

    /**
     * Verify that the placeholders installed at round 1 are the ones core is rewriting — the check that makes an
     * unconfined rescore impossible rather than merely unlikely.
     *
     * <p>Sound because of the order core works in: {@code SearchSourceBuilder#rewrite} rewrites the rescore list during
     * the pass that started this hybrid's fan-out, which is <i>before</i> that pass tests
     * {@code QueryRewriteContext#hasAsyncActions}, and this runs on the pass after the async action. So by the time it is
     * called, core has already had its one chance to visit every placeholder on the path it is actually dispatching. One
     * that was not visited is one installed into a list core is not using, which is the only way the request could reach
     * the shards with a rescore that is not confined.
     */
    void requireReachedTheExecutedRequest() {
        for (WindowPlaceholder placeholder : placeholders) {
            if (placeholder.visited == false) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "[%s] query [%s] cannot confine the request's [rescore] to the fused window: the [rescore] this "
                            + "coordinator rewrote is not the one being executed, so a rescore query could lift a document the "
                            + "fusion never ranked. Failing the request rather than answering it — remove the [rescore], or run "
                            + "the query without a [%s] block",
                        HybridQueryBuilder.NAME,
                        FUSION,
                        FUSION
                    )
                );
            }
        }
    }

    /**
     * The declared rescorer with a window placeholder intersected into its query, and everything else about it preserved:
     * {@code query_weight}, {@code rescore_query_weight} and {@code score_mode} decide how the rescore combines with the
     * fused score, and {@code window_size} how deep it reaches — none of which this rewrite has any business changing.
     * {@code windowSize()} is nullable (unset means core's default), so it is only carried over when the user set it.
     *
     * <p>{@link QueryRescorerBuilder} has no setter for its query ({@code queryBuilder} is {@code private final}), hence a
     * replacement builder rather than an in-place edit. Core's own {@code QueryRescorerBuilder#rewrite} copies these same
     * four fields when the placeholder resolves, so the rescorer that reaches the shards carries them either way.
     */
    private QueryRescorerBuilder confined(final QueryRescorerBuilder declared) {
        WindowPlaceholder placeholder = new WindowPlaceholder(window);
        placeholders.add(placeholder);
        BoolQueryBuilder scoped = new BoolQueryBuilder().must(declared.getRescoreQuery()).filter(placeholder);
        QueryRescorerBuilder replacement = new QueryRescorerBuilder(scoped).setQueryWeight(declared.getQueryWeight())
            .setRescoreQueryWeight(declared.getRescoreQueryWeight())
            .setScoreMode(declared.getScoreMode());
        if (Objects.nonNull(declared.windowSize())) {
            replacement.windowSize(declared.windowSize());
        }
        return replacement;
    }

    /**
     * Fused mode confines a rescore by rewriting its query, which only core's {@code query} rescorer exposes. A rescorer
     * type registered by another plugin is refused rather than passed through: passing it through would leave exactly the
     * defect this class exists to close, silently and only for that rescorer.
     *
     * <p>Refused at install time, so the refusal depends on the request alone — not on whether this particular fusion
     * happened to rank a document.
     */
    private static QueryRescorerBuilder requireQueryRescorer(final RescorerBuilder<?> declared) {
        if (declared instanceof QueryRescorerBuilder queryRescorer) {
            return queryRescorer;
        }
        throw new IllegalArgumentException(
            String.format(
                Locale.ROOT,
                "[%s] fused mode does not support rescorer [%s]: it confines a rescore to the fused window by "
                    + "rewriting the rescore query, and only the [%s] rescorer exposes one",
                HybridQueryBuilder.NAME,
                declared.getWriteableName(),
                QueryRescorerBuilder.NAME
            )
        );
    }

    /**
     * A stand-in for the fused window inside a rescore query, resolved by the rewrite pass that follows the leg fan-out.
     *
     * <p>Purely a coordinator-rewrite vehicle: it is deliberately <b>not</b> registered in the plugin's query registry, is
     * never parsed from a request, and is never serialized — by the time the request leaves the coordinator, core has
     * rewritten it into the window filter. {@link #doToQuery} and {@link #doWriteTo} therefore throw rather than
     * degrading, since either being reached means the query left the coordinator unresolved.
     *
     * <p>Not registered, and not equal to anything but itself, so it cannot be confused with a user-supplied clause.
     */
    static final class WindowPlaceholder extends AbstractQueryBuilder<WindowPlaceholder> {

        static final String NAME = "hybrid_fused_window";

        private final SetOnce<QueryBuilder> window;
        /**
         * Whether core has rewritten this placeholder — written on the pass that installed it, read on the pass after the
         * async action, so {@code volatile} rather than relying on the listener hand-off for visibility.
         */
        private volatile boolean visited;

        private WindowPlaceholder(final SetOnce<QueryBuilder> window) {
            this.window = window;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        /**
         * Resolve to the fused window once it is known, and stay put until then. Returning {@code this} while unresolved is
         * what keeps the placeholder in the tree across the pass that fires the legs — a rewrite loop only continues while
         * the builder's identity changes, and there is nothing to resolve to yet.
         */
        @Override
        protected QueryBuilder doRewrite(final QueryRewriteContext queryRewriteContext) {
            visited = true;
            QueryBuilder resolved = window.get();
            return Objects.isNull(resolved) ? this : resolved;
        }

        @Override
        protected Query doToQuery(final QueryShardContext context) {
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "[%s] the fused window placeholder reached query building unresolved — it is a coordinator rewrite "
                        + "artifact and must never be executed",
                    NAME
                )
            );
        }

        @Override
        protected void doWriteTo(final StreamOutput out) {
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "[%s] the fused window placeholder is not serializable — it is resolved on the coordinator, before the "
                        + "request is dispatched",
                    NAME
                )
            );
        }

        /**
         * Renders harmlessly rather than throwing: anything that prints a request source mid-rewrite — a task description,
         * for one — would otherwise fail on it. Never parsed back, so the form only has to be valid and honest about which
         * state the placeholder is in.
         */
        @Override
        protected void doXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject(NAME);
            builder.field("resolved", Objects.nonNull(window.get()));
            printBoostAndQueryName(builder);
            builder.endObject();
        }

        /**
         * Identity, not value: each placeholder is one install site in one request, so two of them are never
         * interchangeable. {@link AbstractQueryBuilder#equals} has already answered {@code true} for the same instance by
         * the time this is called.
         */
        @Override
        protected boolean doEquals(final WindowPlaceholder other) {
            return false;
        }

        @Override
        protected int doHashCode() {
            return System.identityHashCode(this);
        }
    }
}
