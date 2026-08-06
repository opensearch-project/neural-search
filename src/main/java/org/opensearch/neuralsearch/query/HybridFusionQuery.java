/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.search.Query;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.InnerHitContextBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;

/**
 * Internal query produced by {@link HybridQueryBuilder} when the resolver (fused) mode is enabled via the {@code fusion}
 * parameter, after coordinator-level fusion. It realizes the "Top + Tail" pattern using standard OpenSearch query
 * builders, so all downstream search features (sort, collapse, aggregations, pagination, highlight, min_score) operate
 * on a plain query with no hybrid-specific special-casing:
 *
 * <ul>
 *   <li><b>Top</b> — one {@code constant_score(ids: [id])^fusedScore} clause per ranked document. These are the
 *       scoring {@code should} clauses, so the fused window is returned in fused-score order.</li>
 *   <li><b>Tail</b> — a {@code bool{ should: [sourceQuery...] }} added as a non-scoring {@code filter}. It matches the
 *       full set of documents any sub-query matched, so {@code total_hits} and aggregations cover all matches (not just
 *       the ranked window) and the highlighter has the sub-queries' terms available. Included only when the request
 *       needs the full match set; omitted (Top-only) for plain top-K and for a nested fused query.</li>
 * </ul>
 *
 * <p>This query is created internally by the coordinator self-erase and is never parseable from a search request.
 */
public class HybridFusionQuery extends AbstractQueryBuilder<HybridFusionQuery> {

    public static final String NAME = "hybrid_fusion";

    private final String[] ids;
    private final float[] scores;
    private final List<QueryBuilder> sourceQueries;

    public HybridFusionQuery(String[] ids, float[] scores, List<QueryBuilder> sourceQueries) {
        this.ids = ids;
        this.scores = scores;
        this.sourceQueries = Objects.isNull(sourceQueries) ? new ArrayList<>() : sourceQueries;
    }

    public HybridFusionQuery(StreamInput in) throws IOException {
        super(in);
        this.ids = in.readStringArray();
        this.scores = in.readFloatArray();
        this.sourceQueries = in.readNamedWriteableList(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeStringArray(ids);
        out.writeFloatArray(scores);
        out.writeNamedWriteableList(sourceQueries);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        boolean changed = false;
        List<QueryBuilder> rewritten = new ArrayList<>(sourceQueries.size());
        for (QueryBuilder q : sourceQueries) {
            QueryBuilder r = q.rewrite(queryRewriteContext);
            rewritten.add(r);
            changed |= r != q;
        }
        if (changed) {
            HybridFusionQuery rewrittenBuilder = new HybridFusionQuery(ids, scores, rewritten);
            rewrittenBuilder.boost(boost());
            rewrittenBuilder.queryName(queryName());
            return rewrittenBuilder;
        }
        return this;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        return buildSelfErasedQuery().toQuery(context);
    }

    /**
     * Build the self-erased {@code bool} query (Top + optional Tail) as a standard {@link BoolQueryBuilder}, before it
     * is compiled to a Lucene query. Kept separate from {@link #doToQuery} so the structural contract — one scoring
     * {@code should} per ranked id, and a single non-scoring {@code filter} Tail iff any source query is present — is
     * unit-testable without a shard context.
     */
    BoolQueryBuilder buildSelfErasedQuery() {
        // Top: constant_score(IdsQuery)^fusedScore per ranked doc — the scoring should-clauses that return the fused
        // window in fused-score order.
        BoolQueryBuilder composite = new BoolQueryBuilder();
        for (int i = 0; i < ids.length; i++) {
            composite.should(new ConstantScoreQueryBuilder(new IdsQueryBuilder().addIds(ids[i])).boost(scores[i]));
        }
        // Tail: all source-query matches as a non-scoring filter -> total hits and aggregations cover the full match
        // set, and highlighting has the sub-queries' terms available. Non-window docs match at score 0 and sort below
        // the fused window, so a request with size <= window_size returns exactly the fused window.
        if (sourceQueries.isEmpty() == false) {
            BoolQueryBuilder tail = new BoolQueryBuilder();
            for (QueryBuilder q : sourceQueries) {
                tail.should(q);
            }
            composite.filter(tail);
        }
        return composite;
    }

    /**
     * Recurse into the Tail sub-queries so that inner_hits declared on a leg (e.g. a {@code nested} or {@code has_child}
     * sub-query) are registered and fetched per returned parent hit. Mirrors
     * {@link HybridQueryBuilder#extractInnerHitBuilders}. Without this override the self-erased query would silently drop
     * leg-level inner_hits, because {@link AbstractQueryBuilder}'s default implementation is a no-op and the coordinator
     * self-erase replaces the original {@code hybrid} builder with this one before the shard extracts inner_hits. The
     * leg builders survive intact in {@code sourceQueries} (only KNN/neural legs are materialized to ids, and those do
     * not support inner_hits), and the Tail is retained whenever a leg declares inner_hits (see
     * {@code HybridFusionOrchestrator#buildFusedQuery}), so the definition is always reachable here.
     */
    @Override
    protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
        for (QueryBuilder sourceQuery : sourceQueries) {
            InnerHitContextBuilder.extractInnerHits(sourceQuery, innerHits);
        }
    }

    @Override
    protected boolean doEquals(HybridFusionQuery other) {
        return Arrays.equals(ids, other.ids) && Arrays.equals(scores, other.scores) && Objects.equals(sourceQueries, other.sourceQueries);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(Arrays.hashCode(ids), Arrays.hashCode(scores), sourceQueries);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        // Internal query; representation is informational only.
        builder.startObject(NAME);
        builder.field("fused_docs_count", ids.length);
        builder.endObject();
    }

    public static HybridFusionQuery fromXContent(XContentParser parser) {
        throw new UnsupportedOperationException(
            String.format(
                Locale.ROOT,
                "[%s] is created internally by the hybrid query fused mode and cannot be parsed from a request",
                NAME
            )
        );
    }
}
