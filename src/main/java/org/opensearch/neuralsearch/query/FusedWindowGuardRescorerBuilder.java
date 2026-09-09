/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
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
 * obvious question is what a node that cannot resolve the name does — it fails while deserializing the shard request,
 * and with {@code allow_partial_search_results} at its default the client still gets a 200 with those shards' documents
 * silently missing. That is already the exact hazard {@code HybridQueryBuilder#requireClusterSupportsFusedMode} exists
 * for: it refuses fused mode cluster-wide, at rewrite and before any fan-out, unless every node can resolve
 * {@code hybrid_fusion}. This builder is only ever installed on a request that has passed that gate, and it ships in the
 * same release as fused mode itself — which has never shipped, so there is no released build that has fused mode and
 * lacks this. The gate therefore covers both names with one check.
 *
 * <p><b>The dependency that makes that true, and it is not yet satisfied:</b>
 * {@code MinClusterVersionUtil.MINIMAL_SUPPORTED_VERSION_FUSED_MODE_IN_HYBRID_QUERY} must name the release that actually
 * contains fused mode. It currently reads {@code V_3_8_0}, which is already released and contains none of it — so the
 * constant has to be corrected before either name reaches a mixed-version cluster. That correction is tracked
 * separately; do not add a second gate here to work around it, because a per-name gate would answer a fused rescore
 * UNGUARDED on a cluster that failed it, which is worse than refusing.
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
     * {@code 0.0} before the response leaves, so the sentinel is never user-visible.
     */
    public static final float UNRANKED_SCORE = -Float.MAX_VALUE;

    /**
     * The lowest score a ranked document may report, one float above {@link #UNRANKED_SCORE}. Nothing is representable
     * between the two, so the ranked and unranked bands cannot meet however extreme the request's weights are.
     */
    public static final float RANKED_FLOOR = Math.nextUp(-Float.MAX_VALUE);

    private final List<QueryRescorerBuilder> delegates;

    FusedWindowGuardRescorerBuilder(final List<QueryRescorerBuilder> delegates) {
        if (Objects.isNull(delegates) || delegates.isEmpty()) {
            throw new IllegalArgumentException("[" + NAME + "] requires at least one rescorer to guard");
        }
        this.delegates = List.copyOf(delegates);
        // Core sizes the first-pass pool from the largest window across the request's rescore contexts, and after the
        // wrap this builder is the only context it sees — so it has to stand in for the whole chain's depth.
        Integer widest = null;
        for (QueryRescorerBuilder delegate : delegates) {
            if (Objects.nonNull(delegate.windowSize()) && (Objects.isNull(widest) || delegate.windowSize() > widest)) {
                widest = delegate.windowSize();
            }
        }
        if (Objects.nonNull(widest)) {
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
        return new RescoreContext(windowSize, new FusedWindowGuardRescorer(delegateContexts));
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
