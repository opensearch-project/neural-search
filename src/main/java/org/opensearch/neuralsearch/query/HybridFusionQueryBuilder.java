/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
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
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;

/**
 * Internal query produced by {@link HybridQueryBuilder} when the resolver (fused) mode is enabled via the {@code fusion}
 * parameter, after coordinator-level fusion. It realizes the "Top + Tail" pattern using standard OpenSearch query
 * builders, so all downstream search features (sort, collapse, aggregations, pagination, highlight, min_score) operate
 * on a plain query with no hybrid-specific special-casing:
 *
 * <ul>
 *   <li><b>Top</b> — one {@code constant_score(...)^fusedScore} clause per ranked document. These are the scoring
 *       {@code should} clauses, so the fused window is returned in fused-score order.</li>
 *   <li><b>Tail</b> — a {@code bool{ should: [tailQuery...] }} added as a non-scoring {@code filter}. It matches the
 *       full set of documents any sub-query matched, so {@code total_hits} and aggregations cover all matches (not just
 *       the ranked window) and the highlighter has the sub-queries' terms available. It is present <b>by default</b>:
 *       an accurate {@code total_hits} is itself a Tail trigger, and requests do not set {@code track_total_hits} unless
 *       they mean to give that up. Top-only is the opt-out — {@code track_total_hits} at or below the window, with no
 *       aggregations, highlighting, collapse expansion or non-{@code _score} sort. So the default cost of fused mode is
 *       the legs in round 1 plus the legs re-matched (not re-ranked) inside round 2's filter; see
 *       {@code HybridFusionOrchestrator#needsTail} for the exact trigger set.</li>
 * </ul>
 *
 * <p><b>Document identity.</b> Addressing is defined once, in {@link #addressDocuments}, and used by both halves: the Top
 * addresses one ranked document, the Tail addresses each index group of a leg materialized to its returned ids. A ranked
 * doc is addressed by its {@code _index} and {@code _id} together, always.
 * {@code _id} is unique only within an index, so two different documents in two indices can share one; without the
 * {@code _index} filter each would match the other's Top clause and inherit its fused score. Qualification is not
 * conditional on the search looking like it needs it — the fused window is not evidence about which indices round 2 will
 * execute against — and it is not a trade either: {@code _index} is a constant field, so on the shard's own index the
 * added filter is a MatchAll that {@code BooleanQuery.rewrite} removes (the clause collapses to exactly
 * {@code constant_score(ids)}), and on any other index's shard the clause collapses to MatchNoDocs. {@code indices} is
 * therefore required, not optional: there is no unqualified form of this query. A coordinator-side hit always carries its
 * {@code _index} — it is set from the shard target the response came from — and {@code HybridFusionOrchestrator} asserts
 * that on every leg's hits before fusing them, so the qualified form is always constructible.
 *
 * <p><b>inner_hits registration is decoupled from the Tail.</b> inner_hits are computed in the fetch phase from the
 * registered {@link InnerHitContextBuilder}s (per returned parent doc), not from whatever ran in the query phase — so a
 * leg only needs to be <i>registered</i>, never <i>executed</i>, for its inner_hits to be returned. {@code tailQueries}
 * is therefore what gets executed, while {@code innerHitsQueries} is what gets registered; the two are populated
 * independently, which lets a Top-only query still return leg inner_hits without paying to re-run the legs.
 *
 * <p><b>{@code collapse} semantics.</b> Collapse <i>grouping</i> is identical to classic hybrid — it is a plain
 * query-phase operation over the fused ranking. {@code collapse.inner_hits} is different in kind: core's
 * {@code ExpandSearchPhase} issues one more search per returned group, whose query is this self-erased query under a
 * filter on the group key. A group's members are whatever shares the representative's collapse key, which has nothing to
 * do with the fused window, so the expansion is only as wide as what this query matches — which is why declaring
 * {@code collapse.inner_hits} forces the Tail on ({@code CandidateScope.Disposition#FORCES_TAIL}). With the Tail, every
 * member of the group comes back, matching classic's recall. Member <i>scores</i> then split by window membership:
 * <ul>
 *   <li>a member inside the fused window has its own Top clause, so it carries <b>its own fused score</b> — on the same
 *       scale as the group representative, and equal to the score it would receive in the ungrouped fused search;</li>
 *   <li>a member outside the window has no Top clause and matches only the non-scoring Tail, so it expands at
 *       <b>{@code 0.0}</b>. There is no fused score to give it: fusion ranked the window, and this document is not in it.
 *       Consequence to be aware of: within one group's {@code inner_hits}, in-window members sort above out-of-window ones
 *       regardless of their relative relevance, so an {@code inner_hits.size} smaller than the group returns the
 *       window's members first. Classic instead re-runs the real sub-queries and reports their <b>raw, un-normalized</b>
 *       scores — a consistent order among members, but on a different scale from the representative's normalized score
 *       (e.g. representative {@code 1.0} beside members {@code 198.0}). Raise {@code window_size} to cover the groups to
 *       be expanded if member ordering matters.</li>
 * </ul>
 * Leg-level {@code inner_hits} (a {@code nested}/{@code has_child} sub-query with its own {@code inner_hits} block) are
 * unaffected by any of this: core re-runs the inner query per returned parent there rather than the parent query.
 *
 * <p>This query is created internally by the coordinator self-erase and is never parseable from a search request. Its
 * wire form needs no version gate: the whole query type is new in the same version that introduced fused mode, and a
 * node predating that version cannot resolve this {@code NamedWriteable} name at all — so it fails on the query name
 * long before reading any field. Which is precisely why it must never be sent to one: that failure is a shard failure,
 * and a shard failure is a silently short answer under the default {@code allow_partial_search_results}. The coordinator
 * refuses fused mode outright while the cluster's minimum node version is below it, so this query is only ever built for
 * a cluster that can read it — see {@code HybridQueryBuilder#requireClusterSupportsFusedMode}.
 */
public class HybridFusionQueryBuilder extends AbstractQueryBuilder<HybridFusionQueryBuilder> {

    public static final String NAME = "hybrid_fusion";

    /** Metadata field used to disambiguate same-{@code _id} docs across indices. */
    private static final String INDEX_FIELD = "_index";

    private final String[] ids;
    /**
     * Parallel to {@code ids} and required: one concrete index name per ranked doc, never null and with no null elements.
     * Every Top clause is {@code _index}-qualified, so this array is what makes each one address exactly one document.
     */
    private final String[] indices;
    private final float[] scores;
    /** Legs executed in the query phase as the non-scoring Tail (empty for a Top-only query). */
    private final List<QueryBuilder> tailQueries;
    /** Legs registered for fetch-phase inner_hits extraction; never executed by this query. */
    private final List<QueryBuilder> innerHitsQueries;
    /**
     * Legs converted only so that any {@code _name} they carry is registered for {@code matched_queries}; never executed,
     * and never part of the query this builder compiles to. Non-empty only for a Top-only query whose legs are named —
     * when the Tail is built it registers the very same forms as a side effect of executing them, so the two lists are
     * never both populated. See {@link #registerNamedOnlyQueries}.
     */
    private final List<QueryBuilder> namedOnlyQueries;
    /**
     * The {@code hybrid} query this substitute was self-erased from, carried for the response phase alone. {@code null}
     * whenever it is not available — see {@link #originalQuery()} for what depends on it and why the absence is benign.
     */
    private final transient HybridQueryBuilder originalQuery;

    public HybridFusionQueryBuilder(
        String[] ids,
        String[] indices,
        float[] scores,
        List<QueryBuilder> tailQueries,
        List<QueryBuilder> innerHitsQueries,
        List<QueryBuilder> namedOnlyQueries
    ) {
        this(ids, indices, scores, tailQueries, innerHitsQueries, namedOnlyQueries, null);
    }

    public HybridFusionQueryBuilder(
        String[] ids,
        String[] indices,
        float[] scores,
        List<QueryBuilder> tailQueries,
        List<QueryBuilder> innerHitsQueries,
        List<QueryBuilder> namedOnlyQueries,
        HybridQueryBuilder originalQuery
    ) {
        requireParallel(ids, indices, scores);
        assert Arrays.stream(indices).noneMatch(Objects::isNull)
            : "indices must be fully populated — a null element NPEs in writeStringArray";
        this.ids = ids;
        this.indices = indices;
        this.scores = requireUsableAsBoosts(scores);
        this.tailQueries = Objects.isNull(tailQueries) ? new ArrayList<>() : tailQueries;
        this.innerHitsQueries = Objects.isNull(innerHitsQueries) ? new ArrayList<>() : innerHitsQueries;
        this.namedOnlyQueries = Objects.isNull(namedOnlyQueries) ? new ArrayList<>() : namedOnlyQueries;
        this.originalQuery = originalQuery;
    }

    /**
     * The {@code hybrid} query this one replaced, or {@code null} when it was not carried.
     *
     * <p>A search-pipeline <b>response</b> processor runs on the coordinator that performed the rewrite, against the very
     * {@code SearchRequest} instance whose source core overwrote with the rewritten query ({@code PipelinedRequest} is a
     * {@code SearchRequest}, and the response listener is bound to it before the rewrite). So by the time a response
     * processor reads {@code source().query()}, the user's {@code hybrid} is gone and this query is in its place — a window
     * of {@code _id}s with no query text and no leg structure in it. A processor that has to look at what the user asked
     * for then either fails the request or silently does nothing, where classic hybrid — which erases nothing — works:
     *
     * <ul>
     *   <li>batch semantic highlighting extracts the query text from the main query
     *       ({@code ProcessorUtils#extractQueryTextFromBuilder}) and collects {@code inner_hits} highlight targets by
     *       walking it with a {@link QueryBuilderVisitor} — hence {@link #visit};</li>
     *   <li>{@code rerank}'s {@code query_context.query_text_path} resolves a path into the request source rendered as
     *       XContent — hence {@link #doXContent}.</li>
     * </ul>
     *
     * <p>Never serialized and deliberately absent from {@link #doEquals}/{@link #doHashCode}, exactly like
     * {@link HybridQueryBuilder}'s own coordinator-only fields: it says nothing about what this query matches or scores,
     * and it is only ever read on the node that built it. It is therefore {@code null} on every shard copy (read from the
     * wire) and on an instance built by a test, and each consumer falls back to the behavior it had before this field
     * existed. Cross-cluster search is refused in fused mode, so a response processor never reads a wire copy.
     */
    public HybridQueryBuilder originalQuery() {
        return originalQuery;
    }

    /** Convenience for a query with no name-only registrations — the Tail, where present, registers its own. */
    public HybridFusionQueryBuilder(
        String[] ids,
        String[] indices,
        float[] scores,
        List<QueryBuilder> tailQueries,
        List<QueryBuilder> innerHitsQueries
    ) {
        this(ids, indices, scores, tailQueries, innerHitsQueries, List.of());
    }

    /**
     * The three per-document arrays describe one window between them — an {@code _id}, the {@code _index} that
     * disambiguates it, and the score fusion gave it — so they have to stay parallel. Every consumer walks them by a
     * single index bounded by {@code ids.length}: {@link #buildSelfErasedQuery} reads {@code scores[i]} and
     * {@code indices[i]} per Top clause, and {@link #fusedWindowFilter} groups the same ids by the same indices. A short
     * array is therefore an {@code ArrayIndexOutOfBoundsException} raised inside query construction and reported per
     * shard, with nothing in it naming the coordinator that built the mismatch.
     *
     * <p>A real check in both constructors for the same reason {@link #requireUsableAsBoosts} is one: the wire
     * constructor reads all three from a peer, and an {@code assert} is absent on a production JVM. It replaces an assert
     * that compared {@code indices} against {@code ids} only — {@code scores} was never length-checked at all, and it is
     * the array whose truncation silently drops documents from the window rather than failing.
     *
     * <p>{@code IllegalArgumentException} rather than an {@code IllegalStateException}: for the object constructor this
     * is a caller contract, and for the wire one it is a peer's payload, which is the same class of fault the score check
     * next door reports. Neither is a broken invariant of this object's own.
     */
    private static void requireParallel(final String[] ids, final String[] indices, final float[] scores) {
        Objects.requireNonNull(ids, "ids is required: a fused window is a list of documents, empty at the least");
        Objects.requireNonNull(indices, "indices is required: a fused document is addressed by its _index and _id together");
        Objects.requireNonNull(scores, "scores is required: every ranked document carries the score fusion gave it");
        if (ids.length != indices.length || ids.length != scores.length) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] the fused window's arrays must be parallel — one _index and one score per _id — but got [%d] "
                        + "ids, [%d] indices and [%d] scores",
                    NAME,
                    ids.length,
                    indices.length,
                    scores.length
                )
            );
        }
    }

    /**
     * Every fused score becomes a clause boost in {@link #buildSelfErasedQuery}, so the array has to hold values a boost
     * accepts: finite and non-negative. Checked in both constructors, and the wire one is the reason it is a real check
     * rather than an assert — {@code scores} is the only boost-bearing field on this builder, and the base class re-validates
     * its own {@code boost} on deserialization for the same reason ({@code AbstractQueryBuilder(StreamInput)} calls
     * {@code checkNegativeBoost}). Left unchecked, a bad value survives to {@link #doToQuery} and fails <i>per shard</i>: a
     * negative score dies in {@code AbstractQueryBuilder#boost} and a non-finite one slips past that guard entirely
     * ({@code Float.compare(NaN, 0f) > 0}) to die inside Lucene's {@code BoostQuery} with a Lucene-internal message. Both
     * read as an engine bug rather than as the coordinator handing down something it should never have built.
     *
     * <p>Deliberately the boost contract, not fusion's stronger one. The coordinator additionally guarantees every fused
     * score is <i>strictly</i> positive so a ranked document cannot tie the non-scoring Tail — that is
     * {@code HybridFusionOrchestrator#scoreAboveTail}'s job, and it belongs there, where the ranking is decided. This
     * constructor only refuses what cannot be turned into a query at all.
     */
    private static float[] requireUsableAsBoosts(final float[] scores) {
        for (float score : scores) {
            // Float.compare rather than `<`, matching core's own checkNegativeBoost: -0.0f is not less than 0.0f under `<`
            // but compares as negative, and boost() rejects it — so this check has to reject it too or it lets through the
            // one value that would fail per shard.
            if (Float.isFinite(score) == false || Float.compare(score, 0.0f) < 0) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "[%s] fused scores must all be finite and non-negative — each one is used as a clause boost — but "
                            + "one was [%s]",
                        NAME,
                        score
                    )
                );
            }
        }
        return scores;
    }

    /** Convenience for the common shape where the Tail legs are also the inner_hits source. */
    public HybridFusionQueryBuilder(String[] ids, String[] indices, float[] scores, List<QueryBuilder> tailQueries) {
        this(ids, indices, scores, tailQueries, tailQueries, List.of());
    }

    public HybridFusionQueryBuilder(StreamInput in) throws IOException {
        super(in);
        // All three are read before either check runs: the lengths cannot be compared until every array is off the stream,
        // and the read order is the wire format. A refusal here leaves the remaining lists unread, exactly as the score
        // check below already does — the request fails either way, and no caller reuses the stream.
        String[] wireIds = in.readStringArray();
        String[] wireIndices = in.readStringArray();
        float[] wireScores = in.readFloatArray();
        requireParallel(wireIds, wireIndices, wireScores);
        this.ids = wireIds;
        this.indices = wireIndices;
        this.scores = requireUsableAsBoosts(wireScores);
        this.tailQueries = in.readNamedWriteableList(QueryBuilder.class);
        this.innerHitsQueries = in.readNamedWriteableList(QueryBuilder.class);
        // No wire-version gate: this query is built only for a cluster whose every node supports fused mode (see
        // HybridQueryBuilder#requireClusterSupportsFusedMode), and it has never shipped in a released version.
        this.namedOnlyQueries = in.readNamedWriteableList(QueryBuilder.class);
        // Not on the wire: a wire copy is a shard copy, and the original is only ever read by a response processor on the
        // coordinator that built this query. See originalQuery().
        this.originalQuery = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeStringArray(ids);
        out.writeStringArray(indices);
        out.writeFloatArray(scores);
        out.writeNamedWriteableList(tailQueries);
        out.writeNamedWriteableList(innerHitsQueries);
        out.writeNamedWriteableList(namedOnlyQueries);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        boolean changed = false;
        // Both lists hold the original leg builders, and this query needs them for what they match, never for what they
        // score — the fused scores are already baked into the Top. Rewriting them under a match-set marker is what keeps a
        // leg that is itself a fused hybrid from firing its own legs a second time (see MatchSetRewriteContext).
        QueryRewriteContext matchSetContext = MatchSetRewriteContext.wrap(queryRewriteContext);
        List<QueryBuilder> rewrittenTail = new ArrayList<>(tailQueries.size());
        for (QueryBuilder q : tailQueries) {
            QueryBuilder r = q.rewrite(matchSetContext);
            rewrittenTail.add(r);
            changed |= r != q;
        }
        // The inner_hits sources must be rewritten too — they are not executed, but the fetch phase builds each
        // inner-hit context from the registered builder, so an un-rewritten builder would be a different definition.
        List<QueryBuilder> rewrittenInnerHits = new ArrayList<>(innerHitsQueries.size());
        for (QueryBuilder q : innerHitsQueries) {
            QueryBuilder r = q.rewrite(matchSetContext);
            rewrittenInnerHits.add(r);
            changed |= r != q;
        }
        // The name-only legs must be rewritten too: a name is registered against the query its builder compiles to, and an
        // un-rewritten builder either compiles to something else or refuses to compile at all.
        List<QueryBuilder> rewrittenNamedOnly = new ArrayList<>(namedOnlyQueries.size());
        for (QueryBuilder q : namedOnlyQueries) {
            QueryBuilder r = q.rewrite(matchSetContext);
            rewrittenNamedOnly.add(r);
            changed |= r != q;
        }
        if (changed) {
            // originalQuery is carried over: the coordinator rewrite loop replaces the request's query with whatever the
            // last round returned, so a response processor reads *this* instance — dropping it here would restore the very
            // erasure the field exists to undo, on every request whose legs rewrite at all (a neural leg always does).
            HybridFusionQueryBuilder rewrittenBuilder = new HybridFusionQueryBuilder(
                ids,
                indices,
                scores,
                rewrittenTail,
                rewrittenInnerHits,
                rewrittenNamedOnly,
                originalQuery
            );
            rewrittenBuilder.boost(boost());
            rewrittenBuilder.queryName(queryName());
            return rewrittenBuilder;
        }
        return this;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        registerNamedOnlyQueries(context);
        return buildSelfErasedQuery().toQuery(context);
    }

    /**
     * Convert the carried leg forms purely for the side effect of {@link QueryShardContext#addNamedQuery}, and discard the
     * result — nothing here reaches the executed query, which is {@link #buildSelfErasedQuery()} alone.
     *
     * <p>{@code matched_queries} is reported in the fetch phase from {@code ParsedQuery#namedFilters()}, and
     * {@code MatchedQueriesPhase} builds its own {@link org.apache.lucene.search.Weight} per entry there — so a named leg
     * has to be <i>registered</i>, never <i>executed</i>, exactly as a leg's {@code inner_hits} do (see
     * {@link #extractInnerHitBuilders}). Without this, a Top-only query would convert no leg at all and the field would
     * silently vanish from a response classic hybrid always carries it in.
     *
     * <p>Registering a name twice is a plain map overwrite in the shard context, so this stays correct even if a form here
     * also appears in the Tail. The coordinator does not populate both lists (see {@code HybridFusionOrchestrator}), which
     * keeps the wire payload free of the duplicate rather than relying on that.
     */
    private void registerNamedOnlyQueries(QueryShardContext context) throws IOException {
        for (QueryBuilder namedOnlyQuery : namedOnlyQueries) {
            namedOnlyQuery.toQuery(context);
        }
    }

    /**
     * The legs carried for name registration alone. Exposed for the same reason {@link #buildSelfErasedQuery()} is: what
     * the coordinator decides to carry is a structural contract, and asserting it should not require a shard context.
     */
    List<QueryBuilder> namedOnlyQueries() {
        return namedOnlyQueries;
    }

    /**
     * Build the self-erased {@code bool} query (Top + optional Tail) as a standard {@link BoolQueryBuilder}, before it
     * is compiled to a Lucene query. Kept separate from {@link #doToQuery} so the structural contract — one scoring
     * {@code should} per ranked id, and a single non-scoring {@code filter} Tail iff any Tail query is present — is
     * unit-testable without a shard context.
     */
    BoolQueryBuilder buildSelfErasedQuery() {
        // Top: constant_score(...)^fusedScore per ranked doc — the scoring should-clauses that return the fused window
        // in fused-score order.
        BoolQueryBuilder composite = new BoolQueryBuilder();
        for (int i = 0; i < ids.length; i++) {
            composite.should(new ConstantScoreQueryBuilder(rankedDocQuery(i)).boost(scores[i]));
        }
        // Tail: all leg matches as a non-scoring filter -> total hits and aggregations cover the full match set, and
        // highlighting has the sub-queries' terms available. A doc outside the window matches no Top clause — including
        // a doc in another index that shares a window doc's _id, which is why the Top is _index-qualified — so it scores
        // 0 and sorts below the window, so a request with size <= window_size returns exactly the fused window. A doc
        // matching only the Tail is therefore in the MATCH set but not the RANKED set, which makes fused mode the one
        // query here whose match set is wider than what it ranks — the Tail is load-bearing for total_hits,
        // aggregations and highlighting, so it cannot be narrowed. For a rescore that asymmetry is closed downstream
        // by FusedWindowGuardRescorer, which demotes the Tail-only documents below every ranked one after the request's
        // own rescorers have run, rather than by removing them from the pool.
        if (tailQueries.isEmpty() == false) {
            BoolQueryBuilder tail = new BoolQueryBuilder();
            for (QueryBuilder q : tailQueries) {
                tail.should(q);
            }
            composite.filter(tail);
        }
        return composite;
    }

    /**
     * Address one ranked document by {@code _id} intersected with its {@code _index} — {@code _id} is not unique on its
     * own, and the fused score belongs to exactly one document. There is no unqualified variant: {@code indices} is
     * required, so every position resolves to the index the ranked document was found in.
     */
    private QueryBuilder rankedDocQuery(int position) {
        return addressDocuments(indices[position], ids[position]);
    }

    /**
     * The one definition of document addressing for fused mode: the given {@code _id}s intersected with the one
     * {@code _index} they live in. The Top calls it per ranked document; the Tail calls it per index group of a
     * materialized kNN/neural leg (see {@code HybridFusionOrchestrator#materializedLeg}). Sharing it is the point — the
     * scoring half and the matching half of this query must identify a document the same way, and they previously did not:
     * the Top was {@code _index}-qualified while the Tail addressed a materialized leg by {@code _id} alone, so every
     * same-{@code _id} sibling document in another index passed the Tail {@code filter} and was counted.
     *
     * <p>{@code index} is required. An unqualified form was the previous fallback for hits whose {@code _index} could not
     * be resolved, but no coordinator-side hit is in that state, and the fallback's own semantics were the defect above:
     * it addresses every same-{@code _id} document in the cluster. Both callers get their index from a leg hit, so both
     * always have one.
     */
    static QueryBuilder addressDocuments(String index, String... ids) {
        IdsQueryBuilder idQuery = new IdsQueryBuilder().addIds(ids);
        return new BoolQueryBuilder().filter(idQuery).filter(new TermQueryBuilder(INDEX_FIELD, index));
    }

    /**
     * Address a set of documents spread across indices: one qualified {@link #addressDocuments} clause per index, OR-ed
     * together. The Tail calls it for a materialized leg's hits and {@link #fusedWindowFilter} calls it for the ranked
     * window, so "these documents, wherever they live" has a single definition just as one document does.
     *
     * <p>Grouped by index rather than one clause per document, so a single-index search still presents a single clause —
     * and with one group the {@code bool} wrapper is dropped altogether, since a lone {@code should} is that clause.
     *
     * @param idsByIndex index name to the {@code _id}s to address in it; iteration order decides clause order, so pass an
     *                   order-preserving map for a deterministic query. Must be non-empty: {@code bool{should: []}}
     *                   compiles to {@code MatchAllDocsQuery}, the exact opposite of addressing nothing, so a caller whose
     *                   set may be empty must return {@code match_none} instead of calling this.
     */
    static QueryBuilder addressDocumentGroups(Map<String, List<String>> idsByIndex) {
        assert idsByIndex.isEmpty() == false : "addressing an empty document set compiles to match_all, not match_none";
        List<QueryBuilder> perIndex = new ArrayList<>(idsByIndex.size());
        for (Map.Entry<String, List<String>> group : idsByIndex.entrySet()) {
            perIndex.add(addressDocuments(group.getKey(), group.getValue().toArray(new String[0])));
        }
        if (perIndex.size() == 1) {
            return perIndex.get(0);
        }
        BoolQueryBuilder inAnyIndex = new BoolQueryBuilder();
        perIndex.forEach(inAnyIndex::should);
        return inAnyIndex;
    }

    /**
     * A non-scoring filter matching exactly the fused window — the documents the Top scores, addressed the same way the Top
     * addresses them. Used to confine a request's {@code rescore} to the hybrid's own hits (see {@link FusedRescoreScope}):
     * {@code rescore} is never propagated to a leg, so it runs on the shard against this whole query, where the Tail's
     * non-scoring matches are candidates too. Intersecting the rescore query with this filter is what makes a Tail-only
     * document unliftable.
     *
     * <p>Built from {@code ids} and {@code indices} through {@link #addressDocumentGroups}, deliberately: the filter has to
     * select the same documents the Top clauses do, and the only way to guarantee that is to address them with the same
     * definition rather than a parallel one. In particular it is {@code _index}-qualified, so a sibling index's
     * same-{@code _id} document is outside the window here exactly as it is outside the Top.
     *
     * <p>An empty window yields {@code match_none} rather than an empty {@code bool}, which would compile to
     * {@code MatchAllDocsQuery} and confine the rescore to nothing at all. The coordinator never builds this query with an
     * empty window ({@code HybridFusionOrchestrator#buildFusedQuery} returns {@code match_none} instead), so this is a
     * guard against the failure mode being silent, not a case in the normal flow.
     */
    QueryBuilder fusedWindowFilter() {
        if (ids.length == 0) {
            return new MatchNoneQueryBuilder();
        }
        Map<String, List<String>> idsByIndex = new LinkedHashMap<>();
        for (int i = 0; i < ids.length; i++) {
            idsByIndex.computeIfAbsent(indices[i], index -> new ArrayList<>()).add(ids[i]);
        }
        return addressDocumentGroups(idsByIndex);
    }

    /**
     * Recurse into the registered inner_hits sources so that inner_hits declared on a leg (e.g. a {@code nested} or
     * {@code has_child} sub-query) are registered and fetched per returned parent hit. Mirrors
     * {@link HybridQueryBuilder#extractInnerHitBuilders}. Without this override the self-erased query would silently drop
     * leg-level inner_hits, because {@link AbstractQueryBuilder}'s default implementation is a no-op and the coordinator
     * self-erase replaces the original {@code hybrid} builder with this one before the shard extracts inner_hits.
     *
     * <p>This reads {@code innerHitsQueries}, which is independent of the executed Tail — so inner_hits keep working on a
     * Top-only query. KNN/neural legs materialized to ids are deliberately not part of that list (they cannot carry
     * inner_hits), so the un-materialized leg builders are the ones registered here.
     */
    @Override
    protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
        for (QueryBuilder innerHitsQuery : innerHitsQueries) {
            InnerHitContextBuilder.extractInnerHits(innerHitsQuery, innerHits);
        }
    }

    /**
     * Walk the {@code hybrid} this replaced, not the substitute.
     *
     * <p>A visitor over the coordinator's rewritten query is asking about the user's query — batch semantic highlighting
     * collects its {@code inner_hits} highlight targets exactly that way — and there is nothing in the Top to find: it is
     * an {@code _id} address, and the Tail is a match-set copy of the legs carried only when the request needs it. So the
     * walk is delegated wholesale, which makes what a visitor sees identical to classic hybrid rather than merely closer to
     * it. Without this the inherited default accepts this one builder and stops, so a visitor collecting from the legs
     * finds nothing and whatever it drives is silently skipped.
     *
     * <p>Consequence worth knowing: a walk of this query therefore reports the original's fused legs again, so
     * {@code HybridQueryBuilder#countFusedLegSearches} over the substitute is not zero. No guard reads it that way — the leg
     * search budget counts the request's own {@code source().query()}, which core overwrites only once the whole rewrite has
     * finished, so it never sees a substitute.
     *
     * <p>Falls back to that default when no original was carried, since there is then nothing better to offer.
     */
    @Override
    public void visit(QueryBuilderVisitor visitor) {
        if (Objects.isNull(originalQuery)) {
            visitor.accept(this);
            return;
        }
        originalQuery.visit(visitor);
    }

    @Override
    protected boolean doEquals(HybridFusionQueryBuilder other) {
        return Arrays.equals(ids, other.ids)
            && Arrays.equals(indices, other.indices)
            && Arrays.equals(scores, other.scores)
            && Objects.equals(tailQueries, other.tailQueries)
            && Objects.equals(innerHitsQueries, other.innerHitsQueries)
            && Objects.equals(namedOnlyQueries, other.namedOnlyQueries);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(
            Arrays.hashCode(ids),
            Arrays.hashCode(indices),
            Arrays.hashCode(scores),
            tailQueries,
            innerHitsQueries,
            namedOnlyQueries
        );
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Render the {@code hybrid} this replaced, when it was carried.
     *
     * <p>The rewritten source is rendered as XContent by anything that addresses the request by <i>path</i> —
     * {@code rerank}'s {@code query_context.query_text_path} is resolved against exactly that map — and a path written
     * against the query the user sent stops resolving the moment the query under it is a different one. Emitting the
     * original keeps such a path resolving as it does in classic hybrid; it also means a logged or profiled request shows
     * the query that was asked rather than the window it was answered from.
     *
     * <p>What has to be true for that to be safe is that nothing consumes this representation as a definition of what runs —
     * the rendered form is a fused {@code hybrid}, so re-parsing it would fan out a second time. Nothing does: a shard
     * receives the wire form ({@link #doWriteTo}), not XContent; the one thing in core that re-parses a rendered source in
     * this process is cross-cluster search, which fused mode refuses outright ({@code CandidateScope}); and a non-search
     * rewrite is refused before the self-erase, so {@code _validate/query} and {@code _explain} never produce this query at
     * all. When nothing was carried, the informational form below is unparseable by construction
     * ({@link #fromXContent} throws).
     */
    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        if (Objects.nonNull(originalQuery)) {
            originalQuery.doXContent(builder, params);
            return;
        }
        // Internal query; representation is informational only.
        builder.startObject(NAME);
        builder.field("fused_docs_count", ids.length);
        builder.endObject();
    }

    public static HybridFusionQueryBuilder fromXContent(XContentParser parser) {
        throw new UnsupportedOperationException(
            String.format(
                Locale.ROOT,
                "[%s] is created internally by the hybrid query fused mode and cannot be parsed from a request",
                NAME
            )
        );
    }
}
