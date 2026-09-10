/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.search.Query;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.rescore.RescorerBuilder;

/**
 * Carries the request's own rescorer chain to the shard wrapped in {@link FusedWindowGuardRescorer}, so that the
 * guarantee "a rescore may reorder the hybrid's hits but may not change which documents are eligible" survives any
 * weights the request used. See {@link FusedWindowGuardRescorer} for why the guard is needed and why it demotes rather
 * than removes.
 *
 * <p><b>Not user-typeable, deliberately.</b> This is registered as a named WRITEABLE only — via
 * {@code NeuralSearch#getNamedWriteables()} under the {@link RescorerBuilder} category — and NOT through
 * {@code SearchPlugin#getRescorers()}. That matters: {@code SearchModule#registerRescorer} adds a
 * {@code NamedXContentRegistry} entry in the same call as the writeable one, and the XContent entry is what makes a
 * rescorer name parseable from a request body. Registering the writeable alone gives the shard what it needs to
 * deserialize this builder while adding no new request syntax for a user to discover, mistype, or come to depend on.
 * {@code fromXContent} therefore does not exist; {@link #doXContent} is render-only, for the slow log and
 * {@code _tasks?detailed}.
 *
 * <p><b>One guard wraps the whole chain.</b> The delegates are held in the order the user declared them and are applied
 * in that order, so per-element {@code window_size} semantics are preserved: each delegate keeps its own
 * {@link RescoreContext} built from its own builder. This builder's own {@code window_size} is the maximum over the
 * chain, because core sizes the shard's first-pass pool from the largest window across all rescore contexts and this
 * builder is the only context core can see.
 *
 * <p><b>Why this needs no version gate of its own.</b> It is a new {@code NamedWriteable} on the shard wire, so the
 * obvious question is what a node that cannot resolve the name does — {@code NamedWriteableRegistry} throws while the
 * node deserializes the shard request, which comes back as a failure for that shard alone, so with
 * {@code allow_partial_search_results} at its default the client gets a 200 carrying {@code _shards.failed} and an
 * {@code Unknown NamedWriteable} reason (and a copy of the shard on an upgraded node clears it on retry). A reported
 * wrong answer rather than a silent one, but a wrong answer. That is already the exact hazard
 * {@code HybridQueryBuilder#requireClusterSupportsFusedMode} exists for: it refuses fused mode cluster-wide, at rewrite
 * and before any fan-out, unless every node can resolve {@code hybrid_fusion}. This builder is only ever installed on a
 * request that has passed that gate, and it ships in the same release as fused mode itself — which has never shipped, so
 * there is no released build that has fused mode and lacks this. The gate therefore covers both names with one check.
 *
 * <p><b>The dependency that makes that true, and it is not yet satisfied:</b>
 * {@code MinClusterVersionUtil.MINIMAL_SUPPORTED_VERSION_FUSED_MODE_IN_HYBRID_QUERY} must name the release that actually
 * contains fused mode. It currently reads {@code V_3_8_0}, which is released and contains none of it — so the constant
 * has to be corrected before either name reaches a mixed-version cluster. The exposure is not confined to fused mode:
 * {@code HybridQueryBuilder#doWriteTo} writes the {@code fusion} presence flag to every peer at stream version
 * {@code V_3_8_0} or later whether the query uses fused mode or not, and no cluster setting gates the serializer, so a
 * plain classic {@code hybrid} desynchronises the stream against a released 3.8.0 node as well. Tracked by
 * <a href="https://github.com/opensearch-project/neural-search/issues/2002">issue 2002</a>, where the value is decided at
 * the point the feature branch merges, and held to meanwhile by
 * {@code HybridQueryFusedFanOutTests#testFusedModeMinimumVersion_isNotBehindTheVersionUnderDevelopment}, which fails as
 * soon as the branch's core version moves past the constant. Do not add a second gate here to work around it, because a
 * per-name gate would answer a fused rescore UNGUARDED on a cluster that failed it, which is worse than refusing.
 *
 * <p><b>Installed at coordinator rewrite round 1</b> by {@code FusedRescoreScope#install}, which replaces the request's
 * rescore list with a single element holding the already-window-confined delegates. A replacement made at round 1
 * reaches the shards: {@code SearchSourceBuilder#rewrite} rewrites the query before it reads the rescorer list, so the
 * list core snapshots is the one this builder is in.
 */
public class FusedWindowGuardRescorerBuilder extends RescorerBuilder<FusedWindowGuardRescorerBuilder> {

    public static final String NAME = "hybrid_fused_window_guard";

    /**
     * The score unranked documents are demoted to. Finite on purpose: a non-finite score is quoted as a JSON string by
     * the response serializer, so {@code -Infinity} would turn {@code _score} into a string for every client.
     *
     * <p>Public because the coordinator has to recognise it again — {@code FusedRescoreScoreNormalizer} maps it back to
     * {@code 0.0} on every response and every {@code top_hits} bucket before the response leaves.
     */
    public static final float UNRANKED_SCORE = -Float.MAX_VALUE;

    /**
     * The lowest score a ranked document may report, one float above {@link #UNRANKED_SCORE}. Nothing is representable
     * between the two, so the ranked and unranked bands cannot meet however extreme the request's weights are.
     */
    public static final float RANKED_FLOOR = Math.nextUp(-Float.MAX_VALUE);

    /**
     * What a document sitting on {@link #RANKED_FLOOR} is reported as. Both band values are decoded by the same rule —
     * the score this document would have carried with no rescore in the request at all: an unranked document scores
     * exactly {@code 0.0f} because it matches only the non-scoring Tail, and a ranked document scores at least
     * {@code HybridFusionOrchestrator#MIN_RANKED_SCORE}, which every fused score is already floored to. Reporting
     * {@code RANKED_FLOOR} itself would hand a user a sentinel-adjacent float, and would print a ranked hit BELOW the
     * {@code 0.0f} of the unranked hits it outranks.
     *
     * <p>Deliberately the same number as that floor rather than a distinguishable marker: a document that saturated and
     * a document whose fused score was already at the floor are both "ranked, lowest band", and nothing downstream
     * branches on the value. {@code FusedWindowGuardRescorerBuilderTests} pins the two constants equal.
     */
    public static final float RANKED_FLOOR_NORMALIZED = 1e-30f;

    private final List<QueryRescorerBuilder> delegates;

    FusedWindowGuardRescorerBuilder(final List<QueryRescorerBuilder> delegates) {
        if (Objects.isNull(delegates) || delegates.isEmpty()) {
            throw new IllegalArgumentException("[" + NAME + "] requires at least one rescorer to guard");
        }
        this.delegates = List.copyOf(delegates);
        // Core sizes the first-pass pool from the largest window across the request's rescore contexts, and after the
        // wrap this builder is the only context it sees — so it has to stand in for the whole chain's depth.
        //
        // Over EFFECTIVE windows, not declared ones: a delegate that names no window_size is not windowless, it is
        // DEFAULT_WINDOW_SIZE, which RescorerBuilder#buildContext substitutes for it. Skipping such a delegate would let
        // a chain of [window_size: 5, unset] report 5, and since this builder is the only context core can see, the pool
        // would be collected 5 deep where the unwrapped chain would have collected 10 — the defaulting delegate silently
        // stops reaching documents it is supposed to rescore. When NO delegate declares one the window stays unset, which
        // is not the same as setting it to the default: core applies the same default to this builder, and leaving it
        // unset keeps that out of the rendered request.
        Integer widest = null;
        for (QueryRescorerBuilder delegate : delegates) {
            if (Objects.nonNull(delegate.windowSize())) {
                widest = Objects.isNull(widest) ? delegate.windowSize() : Math.max(widest, delegate.windowSize());
            }
        }
        if (Objects.nonNull(widest)) {
            for (QueryRescorerBuilder delegate : delegates) {
                if (Objects.isNull(delegate.windowSize())) {
                    widest = Math.max(widest, DEFAULT_WINDOW_SIZE);
                    break;
                }
            }
            windowSize(widest);
        }
    }

    public FusedWindowGuardRescorerBuilder(final StreamInput in) throws IOException {
        super(in);
        this.delegates = List.copyOf(in.readList(QueryRescorerBuilder::new));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWriteTo(final StreamOutput out) throws IOException {
        out.writeList(delegates);
    }

    @Override
    protected void doXContent(final XContentBuilder builder, final Params params) throws IOException {
        // Render-only: this builder is never parsed back from a request. Named so that an operator who finds it in a slow
        // log or a task description can tell it is the plugin's own wrapper rather than something they wrote.
        builder.startObject(NAME);
        builder.startArray("rescorers");
        for (QueryRescorerBuilder delegate : delegates) {
            delegate.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
    }

    @Override
    protected RescoreContext innerBuildContext(final int windowSize, final QueryShardContext context) throws IOException {
        List<RescoreContext> delegateContexts = new ArrayList<>(delegates.size());
        for (QueryRescorerBuilder delegate : delegates) {
            // buildContext is final on RescorerBuilder and applies each delegate's own window size, so the chain keeps
            // the depth semantics the user declared per element.
            delegateContexts.add(delegate.buildContext(context));
        }
        return new ChainRescoreContext(windowSize, delegateContexts);
    }

    /**
     * The guard's own {@link RescoreContext}, which reports the chain's queries as if the chain were still registered
     * directly.
     *
     * <p>Necessary because two core phases read the request's rescore contexts for their <b>queries</b> rather than for
     * their rescorers, and after the wrap this context is the only one they can see. {@code MatchedQueriesPhase} collects
     * {@code getParsedQueries().namedFilters()}, so a {@code _name} on a rescore query would vanish from
     * {@code matched_queries}; {@code DfsPhase} collects term statistics from the same list, so under
     * {@code dfs_query_then_fetch} the rescore query would score on local rather than global IDF and reorder the ranked
     * hits. A bare {@code RescoreContext} answers both with an empty list, which is why the queries have to be forwarded
     * explicitly — the wrap is meant to change which documents a rescore may return, and nothing else about it.
     */
    private static final class ChainRescoreContext extends RescoreContext {

        private final List<RescoreContext> delegates;

        ChainRescoreContext(final int windowSize, final List<RescoreContext> delegates) {
            super(windowSize, new FusedWindowGuardRescorer(delegates));
            this.delegates = delegates;
        }

        @Override
        public List<Query> getQueries() {
            List<Query> queries = new ArrayList<>();
            for (RescoreContext delegate : delegates) {
                queries.addAll(delegate.getQueries());
            }
            return queries;
        }

        @Override
        public List<ParsedQuery> getParsedQueries() {
            List<ParsedQuery> parsedQueries = new ArrayList<>();
            for (RescoreContext delegate : delegates) {
                parsedQueries.addAll(delegate.getParsedQueries());
            }
            return parsedQueries;
        }
    }

    @Override
    public RescorerBuilder<FusedWindowGuardRescorerBuilder> rewrite(final QueryRewriteContext ctx) throws IOException {
        List<QueryRescorerBuilder> rewritten = new ArrayList<>(delegates.size());
        boolean changed = false;
        for (QueryRescorerBuilder delegate : delegates) {
            RescorerBuilder<QueryRescorerBuilder> next = delegate.rewrite(ctx);
            changed |= next != delegate;
            rewritten.add((QueryRescorerBuilder) next);
        }
        if (changed == false) {
            return this;
        }
        FusedWindowGuardRescorerBuilder replacement = new FusedWindowGuardRescorerBuilder(rewritten);
        if (Objects.nonNull(windowSize())) {
            replacement.windowSize(windowSize());
        }
        return replacement;
    }

    /** The chain this guard wraps, in the order the user declared it. */
    List<QueryRescorerBuilder> delegates() {
        return delegates;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (Objects.isNull(other) || getClass() != other.getClass()) {
            return false;
        }
        FusedWindowGuardRescorerBuilder that = (FusedWindowGuardRescorerBuilder) other;
        return Objects.equals(windowSize(), that.windowSize()) && Objects.equals(delegates, that.delegates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowSize(), delegates);
    }
}
