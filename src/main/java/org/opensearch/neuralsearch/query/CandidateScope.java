/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.search.slice.SliceBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;

/**
 * The slice of a search request that decides <b>which documents are candidates</b> and <b>how they score</b> — the only
 * part of the request a resolver (fused) mode leg sub-search is allowed to inherit, and the part it must never silently
 * drop.
 *
 * <p>Fused mode answers one user request with two searches: round 1 fans the legs out as a {@code MultiSearch} to pick
 * the candidate window, and round 2 runs the self-erased {@code bool{Top + Tail}} over the user's original request. The
 * two rounds must agree on the candidate set, and when they disagree the failure is silent and always shaped alike:
 * round 1 selects documents round 2 cannot return, round 2 backfills the shortfall from the Tail, and the user gets
 * correctly-ranked hits followed by arbitrary documents at {@code _score: 0.0} instead of an error. Building the leg
 * request field by field from scratch is what allows that: every request field nobody thought about defaults to "not
 * propagated", which is the wrong default for anything selection-relevant.
 *
 * <p>This class inverts the default. Every declared field of {@link SearchRequest} and {@link SearchSourceBuilder}
 * carries an explicit {@link Disposition} in {@link #CLASSIFICATION}, {@code CandidateScopeTests} fails the build when a
 * field of either class is unclassified (or when an entry names a field that no longer exists), and leg requests are
 * built only through {@link #newLegRequest}. A core upgrade that adds a request field therefore cannot reach a leg
 * unexamined.
 *
 * <p><b>Cost.</b> {@link Disposition#REJECTED} fields fail the request at rewrite, before the leg fan-out, so a refusal
 * costs strictly less than the search it replaces. Every {@link Disposition#PROPAGATED} field either narrows a leg's
 * work (routing, preference, pre-filter, shard concurrency, timeout, slice) or is free at the leg (post_filter, PIT), so
 * honoring them does not make a leg more expensive than it is today. There are two exceptions, both of them the user's
 * explicit choice: {@code search_type=dfs_query_then_fetch} adds a term-statistics round trip per leg, and the
 * alternative is picking candidates with statistics they asked us not to use; and {@code profile} runs each leg profiled
 * (see {@link #enableLegProfiling}), which is what the request asked to measure.
 */
final class CandidateScope {

    private static final String FUSION_FIELD_NAME = "fusion";

    /** What fused mode does with a given request field. Exactly one applies to each field of the two request classes. */
    enum Disposition {
        /** Copied onto every leg: it changes which documents are candidates, or it makes the leg cheaper. */
        PROPAGATED,
        /** Fused mode sets its own value on the leg; the user's value is deliberately not honored there. */
        OVERRIDDEN,
        /** Cannot be honored correctly in fused mode — the request fails at rewrite with an explanatory message. */
        REJECTED,
        /** Not copied, but forces the Tail on, so round 2 ranks over the full leg union rather than only the window. */
        FORCES_TAIL,
        /** Deliberately left off the leg: it cannot affect which documents a leg returns, or it belongs to round 2. */
        NOT_PROPAGATED
    }

    /** A disposition plus the reason it is correct, so the table is reviewable without reading the implementation. */
    record Classification(Disposition disposition, String reason) {
    }

    static final String SEARCH_REQUEST = SearchRequest.class.getSimpleName();
    static final String SEARCH_SOURCE = SearchSourceBuilder.class.getSimpleName();

    /**
     * Every declared instance field of {@link SearchRequest} and {@link SearchSourceBuilder}, keyed {@code Class#field}.
     * Kept complete and current by {@code CandidateScopeTests}, which reflects over both classes in both directions.
     */
    static final Map<String, Classification> CLASSIFICATION = classification();

    private static Map<String, Classification> classification() {
        Map<String, Classification> table = new LinkedHashMap<>();

        // ---------------------------------------------- SearchRequest ----------------------------------------------
        put(table, SEARCH_REQUEST, "indices", Disposition.PROPAGATED, "a leg must draw its candidates from the same indices");
        put(table, SEARCH_REQUEST, "indicesOptions", Disposition.PROPAGATED, "same expansion and ignore-unavailable semantics");
        put(
            table,
            SEARCH_REQUEST,
            "routing",
            Disposition.PROPAGATED,
            "round 2 only searches routed shards, so unrouted legs pick candidates round 2 cannot return"
        );
        put(table, SEARCH_REQUEST, "preference", Disposition.PROPAGATED, "pins the legs and round 2 to the same shard copies");
        put(
            table,
            SEARCH_REQUEST,
            "searchType",
            Disposition.PROPAGATED,
            "dfs_query_then_fetch changes term statistics, and therefore leg scores and the window"
        );
        put(
            table,
            SEARCH_REQUEST,
            "allowPartialSearchResults",
            Disposition.PROPAGATED,
            "propagated only when explicitly set, so an unset flag still resolves the cluster default per leg as usual"
        );
        put(
            table,
            SEARCH_REQUEST,
            "maxConcurrentShardRequests",
            Disposition.PROPAGATED,
            "the user's per-node shard fan-out cap must bound the legs too, which multiply that fan-out"
        );
        put(table, SEARCH_REQUEST, "preFilterShardSize", Disposition.PROPAGATED, "can-match pre-filtering makes a leg cheaper");
        put(
            table,
            SEARCH_REQUEST,
            "cancelAfterTimeInterval",
            Disposition.PROPAGATED,
            "a leg must honor the request's cancellation budget or it outlives the request that spawned it"
        );
        put(table, SEARCH_REQUEST, "source", Disposition.OVERRIDDEN, "a leg source is built explicitly, field by field");
        put(
            table,
            SEARCH_REQUEST,
            "pipeline",
            Disposition.OVERRIDDEN,
            "pinned to _none so request/response processors run once for the user request, not once per leg"
        );
        put(
            table,
            SEARCH_REQUEST,
            "ccsMinimizeRoundtrips",
            Disposition.REJECTED,
            "cross-cluster search is refused outright, so its round-trip mode is moot"
        );
        put(
            table,
            SEARCH_REQUEST,
            "scroll",
            Disposition.REJECTED,
            "a scroll pages one reader snapshot to exhaustion, and that snapshot is round 2's alone: the legs that chose "
                + "the window ran as one-shot searches against their own reader instants and are never re-run. Classic "
                + "hybrid rejects scroll too — use point_in_time instead, which every leg and round 2 do share"
        );
        put(
            table,
            SEARCH_REQUEST,
            "requestCache",
            Disposition.NOT_PROPAGATED,
            "caching is selection-neutral, and it still applies to the round-2 query the shards receive, which already "
                + "embeds the window ids"
        );
        put(table, SEARCH_REQUEST, "batchedReduceSize", Disposition.NOT_PROPAGATED, "coordinator reduce batching; selection-neutral");
        put(table, SEARCH_REQUEST, "phaseTook", Disposition.NOT_PROPAGATED, "response timing only");
        put(
            table,
            SEARCH_REQUEST,
            "localClusterAlias",
            Disposition.NOT_PROPAGATED,
            "set only by core's own cross-cluster sub-request constructor; not reachable from a user request"
        );
        put(table, SEARCH_REQUEST, "absoluteStartMillis", Disposition.NOT_PROPAGATED, "as localClusterAlias");
        put(table, SEARCH_REQUEST, "finalReduce", Disposition.NOT_PROPAGATED, "as localClusterAlias");

        // ------------------------------------------- SearchSourceBuilder -------------------------------------------
        put(table, SEARCH_SOURCE, "queryBuilder", Disposition.OVERRIDDEN, "each leg runs exactly one sub-query");
        put(
            table,
            SEARCH_SOURCE,
            "postQueryBuilder",
            Disposition.PROPAGATED,
            "round 2 applies post_filter above the top-docs collector, so its window is post-filtered; an unfiltered leg "
                + "window would be decimated in round 2 and backfilled with score-0 Tail documents"
        );
        put(table, SEARCH_SOURCE, "size", Disposition.OVERRIDDEN, "a leg returns exactly the candidate window");
        put(table, SEARCH_SOURCE, "from", Disposition.OVERRIDDEN, "a leg always starts at 0; paging is a round-2 concern");
        put(table, SEARCH_SOURCE, "fetchSourceContext", Disposition.OVERRIDDEN, "legs are id-only, so _source is switched off");
        put(
            table,
            SEARCH_SOURCE,
            "trackTotalHitsUpTo",
            Disposition.OVERRIDDEN,
            "legs disable totals, unless the rewrite asks a lexical leg to count up to the user's threshold so round 2 can "
                + "skip the Tail when the union is already known to exceed it; the user's value itself decides whether round 2 "
                + "needs the Tail"
        );
        put(
            table,
            SEARCH_SOURCE,
            "aggregations",
            Disposition.OVERRIDDEN,
            "not run per leg; their presence turns the Tail on so round 2 aggregates the full leg union"
        );
        put(table, SEARCH_SOURCE, "highlightBuilder", Disposition.OVERRIDDEN, "as aggregations");
        put(
            table,
            SEARCH_SOURCE,
            "searchPipelineSource",
            Disposition.OVERRIDDEN,
            "an inline pipeline body is not applied per leg; the leg pipeline is pinned to _none"
        );
        put(table, SEARCH_SOURCE, "searchPipeline", Disposition.OVERRIDDEN, "as searchPipelineSource");
        put(table, SEARCH_SOURCE, "timeout", Disposition.PROPAGATED, "bounds a leg's per-shard work as the user intended");
        put(
            table,
            SEARCH_SOURCE,
            "sliceBuilder",
            Disposition.PROPAGATED,
            "round 2 returns only the slice, so unsliced legs would fill the window with documents outside it and leave "
                + "the slice an arbitrary fraction of it. A slice is legal only alongside a point_in_time or a scroll, "
                + "and scroll is rejected, so the reachable shape is a slice over a PIT — inherited together with it"
        );
        put(
            table,
            SEARCH_SOURCE,
            "pointInTimeBuilder",
            Disposition.PROPAGATED,
            "all legs and round 2 then read one immutable view instead of N+1 independent reader instants"
        );
        put(
            table,
            SEARCH_SOURCE,
            "terminateAfter",
            Disposition.REJECTED,
            "round 2's early-termination collector sits below the filter collectors and counts every match in docid "
                + "order, so with the Tail present it spends the whole budget on Tail documents before reaching the window"
        );
        put(
            table,
            SEARCH_SOURCE,
            "indexBoosts",
            Disposition.REJECTED,
            "in round 2 core wraps the whole self-erased query in a BoostQuery, which classic hybrid rejects outright; "
                + "the boost cannot influence which documents enter the window, and propagating it would apply it twice"
        );
        put(
            table,
            SEARCH_SOURCE,
            "derivedFieldsObject",
            Disposition.REJECTED,
            "a leg query over a derived field would silently rewrite to match_none, and core exposes no setter for "
                + "propagating derived-field definitions onto a leg source"
        );
        put(table, SEARCH_SOURCE, "derivedFields", Disposition.REJECTED, "as derivedFieldsObject");
        put(
            table,
            SEARCH_SOURCE,
            "sorts",
            Disposition.FORCES_TAIL,
            "a non-_score sort ranks by the sort key instead of the fused score, so sorting only the window would sort "
                + "an arbitrary subset of the matches; the Tail widens round 2 to the full leg union, over which the "
                + "user's sort is the plain-search answer"
        );
        put(
            table,
            SEARCH_SOURCE,
            "minScore",
            Disposition.NOT_PROPAGATED,
            "a threshold on the normalized fused score is meaningless against a leg's raw scores"
        );
        put(
            table,
            SEARCH_SOURCE,
            "searchAfterBuilder",
            Disposition.NOT_PROPAGATED,
            "round-2 paging over Top then Tail, the same way a size beyond the window already pages"
        );
        put(
            table,
            SEARCH_SOURCE,
            "collapse",
            Disposition.FORCES_TAIL,
            "collapsing applies to the fused result, as it does to classic hybrid's, and collapsing a leg would instead "
                + "change which documents are candidates — but collapse.inner_hits expands each group by re-running the "
                + "round-2 query per group, and a group's members are not confined to the fused window, so Top only would "
                + "silently drop every member that ranked outside it"
        );
        put(
            table,
            SEARCH_SOURCE,
            "rescoreBuilders",
            Disposition.NOT_PROPAGATED,
            "rescoring reorders round 2's top documents after fusion, which is the intended post-fusion rerank; on a leg "
                + "it would change the candidates instead"
        );
        put(
            table,
            SEARCH_SOURCE,
            "explain",
            Disposition.OVERRIDDEN,
            "a leg runs explained only when the fused rewrite has somewhere to keep its explanations, never by "
                + "inheriting the outer flag. That somewhere is FusedDocExplanations, which FusedExplanationMerger "
                + "rebuilds onto the response (see enableLegExplain). It has to be the legs that explain: round 2 "
                + "explains the self-erased query, where a matching Top clause is a childless constant_score, and "
                + "normalization and combination ran on the coordinator"
        );
        put(table, SEARCH_SOURCE, "version", Disposition.NOT_PROPAGATED, "fetch-phase metadata; a leg fetches nothing but ids");
        put(table, SEARCH_SOURCE, "seqNoAndPrimaryTerm", Disposition.NOT_PROPAGATED, "as version");
        put(table, SEARCH_SOURCE, "storedFieldsContext", Disposition.NOT_PROPAGATED, "as version");
        put(table, SEARCH_SOURCE, "docValueFields", Disposition.NOT_PROPAGATED, "as version");
        put(table, SEARCH_SOURCE, "scriptFields", Disposition.NOT_PROPAGATED, "as version");
        put(table, SEARCH_SOURCE, "fetchFields", Disposition.NOT_PROPAGATED, "as version");
        put(table, SEARCH_SOURCE, "trackScores", Disposition.NOT_PROPAGATED, "a leg always scores; this only affects reporting");
        put(
            table,
            SEARCH_SOURCE,
            "includeNamedQueriesScore",
            Disposition.NOT_PROPAGATED,
            "scoring variant of matched_queries reporting: only a leg's hits are read from its response, so a leg's own "
                + "named-query report has nowhere to go. Round 2 answers for the user's named queries itself — the legs "
                + "are converted on the shard for exactly that, whether or not the Tail executes them (see "
                + "HybridFusionQueryBuilder#registerNamedOnlyQueries)"
        );
        put(table, SEARCH_SOURCE, "suggestBuilder", Disposition.NOT_PROPAGATED, "suggestions do not depend on the query");
        put(table, SEARCH_SOURCE, "stats", Disposition.NOT_PROPAGATED, "propagating would count every leg into the user's stats groups");
        put(table, SEARCH_SOURCE, "extBuilders", Disposition.NOT_PROPAGATED, "plugin response extensions, not selection");
        put(
            table,
            SEARCH_SOURCE,
            "profile",
            Disposition.OVERRIDDEN,
            "a leg runs profiled when the fused rewrite has somewhere to publish its tree, and unprofiled otherwise — "
                + "never by inheriting the outer flag. FusedLegProfileMerger is that somewhere: it is attached before the "
                + "rewrite, only for a profiled request, and it merges each leg's tree into the response's profile section "
                + "under the leg's own shard entry (see enableLegProfiling). Profiling a leg costs the leg the usual "
                + "profiling overhead and reports it there, which is the point"
        );
        put(table, SEARCH_SOURCE, "verbosePipeline", Disposition.NOT_PROPAGATED, "pipeline debug output, and a leg runs no pipeline");

        return Map.copyOf(table);
    }

    private static void put(
        final Map<String, Classification> table,
        final String owner,
        final String field,
        final Disposition disposition,
        final String reason
    ) {
        table.put(key(owner, field), new Classification(disposition, reason));
    }

    static String key(final String owner, final String field) {
        return owner + "#" + field;
    }

    // The captured PROPAGATED values. A null means the user left the field unset, and an unset value is never forced
    // onto a leg — the leg then resolves the same default the outer request would.
    private final String[] indices;
    private final IndicesOptions indicesOptions;
    private final String routing;
    private final String preference;
    private final SearchType searchType;
    private final Boolean allowPartialSearchResults;
    private final int maxConcurrentShardRequests;
    private final Integer preFilterShardSize;
    private final TimeValue cancelAfterTimeInterval;
    private final TimeValue timeout;
    private final QueryBuilder postFilter;
    private final SliceBuilder slice;
    private final String pointInTimeId;

    /**
     * When set, every leg sub-search runs with {@code profile: true} so its tree can be merged into the user's response.
     * Not part of the captured scope — it is the fused rewrite's own decision, made from the outer request's
     * {@code profile} flag, not a field inherited from it.
     */
    private boolean legProfiling;

    /**
     * When set, every leg sub-search runs with {@code explain: true} so the raw score each leg contributed can be
     * described in the user's response. As {@link #legProfiling}: not part of the captured scope, and decided by the fused
     * rewrite from the outer request's {@code explain} flag rather than inherited from it.
     */
    private boolean legExplain;

    /**
     * When set, every leg counts its matches up to this {@code track_total_hits} threshold instead of disabling totals, so
     * the rewrite can tell whether the leg union already exceeds the threshold and drop round 2's Tail — see
     * {@code HybridFusionOrchestrator#totalHitsFromLegs}. As {@link #legProfiling}: not inherited from the request but the
     * fused rewrite's own decision, made from the request's {@code track_total_hits} and only when the request's shape
     * would let a derived count replace the Tail and the count has somewhere to go.
     *
     * <p>Every leg counts, ANN legs included. A leg's count is a valid lower bound on the union's whatever form the Tail
     * would have carried the leg in — the leg itself, or an address of the documents it returned, which is used only when
     * it returned all of them — and an ANN leg's count is not as small as it looks: core sums the per-shard counts before
     * capping, so {@code k = 1000} over twelve shards clears the default threshold, and a {@code neural} leg over a
     * {@code rank_features} field is a sparse query whose match set is far larger than {@code k}. Counting costs an ANN leg
     * nothing extra — it collects exactly the documents it counts.
     */
    private Integer legTotalHitsThreshold;

    private CandidateScope(final SearchRequest request) {
        SearchSourceBuilder source = request.source();
        this.indices = request.indices();
        this.indicesOptions = request.indicesOptions();
        this.routing = request.routing();
        this.preference = request.preference();
        this.searchType = request.searchType();
        this.allowPartialSearchResults = request.allowPartialSearchResults();
        this.maxConcurrentShardRequests = request.getMaxConcurrentShardRequestsRaw();
        this.preFilterShardSize = request.getPreFilterShardSize();
        this.cancelAfterTimeInterval = request.getCancelAfterTimeInterval();
        this.timeout = Objects.isNull(source) ? null : source.timeout();
        this.postFilter = Objects.isNull(source) ? null : source.postFilter();
        this.slice = Objects.isNull(source) ? null : source.slice();
        PointInTimeBuilder pointInTime = Objects.isNull(source) ? null : source.pointInTimeBuilder();
        this.pointInTimeId = Objects.isNull(pointInTime) ? null : pointInTime.getId();
    }

    /**
     * Capture the candidate-defining part of {@code request}, refusing the request shapes fused mode cannot answer
     * correctly. Called at rewrite, before the leg fan-out, so a refusal replaces the search instead of following it.
     *
     * @throws IllegalArgumentException for any {@link Disposition#REJECTED} field the request actually sets
     */
    static CandidateScope from(final SearchRequest request) {
        rejectUnsupported(request);
        return new CandidateScope(request);
    }

    /**
     * Fail the {@link Disposition#REJECTED} shapes. Each check tests for a value the user actually supplied, so a plain
     * request never trips one, and the messages name the field the user wrote rather than the field this class keys on.
     */
    private static void rejectUnsupported(final SearchRequest request) {
        // Cross-cluster first, since this runs before index resolution: a literal `cluster:index` would otherwise fail
        // as `no such index`, which says nothing about fused mode, while a wildcard alias (`*:index`) resolves cleanly
        // and would reach round 2 — so neither case can be left to resolution.
        for (String index : request.indices()) {
            if (index.indexOf(':') >= 0) {
                throw unsupported(
                    "cross-cluster search",
                    "the fused window keys and addresses documents by [_index] plus [_id], and neither survives a cluster "
                        + "hop: a remote hit carries the bare index name with its alias held separately, so same-named "
                        + "indices in two clusters collapse into one key, and round 2's [_index] term never matches on a "
                        + "remote shard. Remote documents would reach the user through the Tail alone, at [_score: 0.0]"
                );
            }
        }
        if (Objects.nonNull(request.scroll())) {
            throw unsupported("scroll", reasonFor(SEARCH_REQUEST, "scroll"));
        }
        SearchSourceBuilder source = request.source();
        if (Objects.isNull(source)) {
            return;
        }
        if (source.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER) {
            throw unsupported("terminate_after", reasonFor(SEARCH_SOURCE, "terminateAfter"));
        }
        if (source.indexBoosts().isEmpty() == false) {
            throw unsupported("indices_boost", reasonFor(SEARCH_SOURCE, "indexBoosts"));
        }
        if (Objects.nonNull(source.getDerivedFieldsObject())
            || (Objects.nonNull(source.getDerivedFields()) && source.getDerivedFields().isEmpty() == false)) {
            throw unsupported("derived", reasonFor(SEARCH_SOURCE, "derivedFieldsObject"));
        }
    }

    private static IllegalArgumentException unsupported(final String what, final String why) {
        return new IllegalArgumentException(
            String.format(Locale.ROOT, "[%s] query [%s] does not support [%s]: %s", HybridQueryBuilder.NAME, FUSION_FIELD_NAME, what, why)
        );
    }

    private static String reasonFor(final String owner, final String field) {
        return CLASSIFICATION.get(key(owner, field)).reason();
    }

    /**
     * True when the request ranks by something other than {@code _score}. The fused scores then only pick the candidate
     * set while the user's sort decides the order, so the candidate set has to be the whole leg union rather than the
     * window — see {@link Disposition#FORCES_TAIL}.
     */
    static boolean sortDiscardsFusedRanking(final SearchSourceBuilder source) {
        if (Objects.isNull(source) || Objects.isNull(source.sorts())) {
            return false;
        }
        for (SortBuilder<?> sort : source.sorts()) {
            if ((sort instanceof ScoreSortBuilder) == false) {
                return true;
            }
        }
        return false;
    }

    /**
     * True when the request asks for collapse groups to be expanded, i.e. {@code collapse} carries {@code inner_hits}.
     * Core's {@code ExpandSearchPhase} then issues one extra search per returned group whose query is
     * {@code bool{filter: group key, must: source().query()}} — by then the self-erased fused query. A group's members are
     * whichever documents share the representative's collapse key, which is unrelated to the fused window, so with Top
     * only they match no {@code constant_score} clause and the expansion comes back holding just the members that happened
     * to rank inside the window. The Tail is what makes the expansion cover the group — see {@link Disposition#FORCES_TAIL}.
     *
     * <p>Plain {@code collapse} with no {@code inner_hits} needs nothing: grouping is a query-phase operation over the
     * documents round 2 already returns, and core runs no expansion at all (its own {@code isCollapseRequest} likewise
     * tests for a non-empty inner-hits list).
     */
    static boolean collapseExpandsGroups(final SearchSourceBuilder source) {
        return Objects.nonNull(source) && Objects.nonNull(source.collapse()) && source.collapse().getInnerHits().isEmpty() == false;
    }

    /**
     * Ask every leg built from here on to report its profile tree, because the request asked to be profiled and the
     * rewrite has a {@code FusedLegProfileMerger} to publish the trees to.
     *
     * <p>A profiled leg pays profiling's own overhead, so a profiled fused request spends measurably longer inside the
     * fan-out than the same request unprofiled — the ordinary observer effect of {@code profile}, and the reason the
     * numbers a profiled run reports describe a profiled run. It lands inside the user's {@code timeout} like any other
     * leg work: a leg that exceeds a soft timeout returns the candidates it had, so the window can be narrower than an
     * unprofiled run's. That is not specific to profiling, and it is visible: a fused response reports
     * {@code timed_out} when any leg was truncated, whether or not the request asked to be profiled (see
     * {@code FusedLegTimeoutMerger}).
     */
    void enableLegProfiling() {
        this.legProfiling = true;
    }

    /**
     * Ask every leg built from here on to explain its hits, because the request asked to be explained and the rewrite has a
     * {@code FusedDocExplanations} to keep the explanations in.
     *
     * <p>An explained leg pays explanation's own overhead — a second per-hit pass through the leg's weight — and, unlike
     * profiling, that cost lands on the shards rather than only on the coordinator. It is bounded by the leg's window
     * (explanations are produced during fetch, for the hits the leg returns, not for its whole match set), and it is the
     * same cost classic hybrid pays for {@code explain} on the same sub-queries.
     */
    void enableLegExplain() {
        this.legExplain = true;
    }

    /**
     * Ask every leg built from here on to count its matches up to {@code threshold}, because the request wants a
     * {@code hits.total} beyond the fused window, its shape would let a count derived from the legs replace the Tail, and
     * the rewrite has somewhere to put that count.
     *
     * <p>The cost lands on the leg's query phase: a collector that counts to a threshold cannot skip non-competitive
     * documents until it has reached it, so a lexical leg with a large match set does up to {@code threshold} more document
     * visits per shard than one that only ranks (measured at tens of nanoseconds per visit on warm postings). When a leg
     * then exceeds the threshold, that is the enumeration the Tail would otherwise have done in round 2, moved into round 1
     * where it runs alongside the other legs instead of serially after them; when no leg does, the Tail is kept and the
     * enumeration has been duplicated — small, and bounded by the threshold, but not free, which is why the count is armed
     * only for the request shapes that can use it.
     */
    void enableLegTotalHits(final int threshold) {
        this.legTotalHitsThreshold = threshold;
    }

    /**
     * The single place a leg sub-search is constructed: the captured candidate scope, plus this leg's own query and the
     * candidate window. Every {@link Disposition#OVERRIDDEN} value is set here explicitly rather than inherited, so no
     * field of the outer request can reach a leg by accident. Legs are id-only (no {@code _source}) with totals disabled,
     * since the Tail supplies the full-match-set count when the request needs one.
     *
     * <p>Notes on three of the propagated fields, whose correctness depends on details not obvious from the table:
     * <ul>
     *   <li><b>pipeline</b> is pinned to {@code _none}. Otherwise a leg — a plain {@link SearchRequest} with no explicit
     *       pipeline — would inherit the index's {@code index.search.default_pipeline} and re-run its request/response
     *       processors once per leg: redundant, and incorrect for processors like {@code rerank} that expect request
     *       context an id-only leg does not have. The outer fused request still carries the pipeline, so top-level
     *       processors run exactly once. A nested fused hybrid therefore cannot read its own config from a leg request,
     *       which is why the enclosing rewrite projects the config down instead (see
     *       {@code HybridQueryBuilder#projectResolvedConfigOntoLegs}).</li>
     *   <li><b>allow_partial_search_results</b> is propagated only when the user set it explicitly; left unset, the leg
     *       flag stays unset and resolves the cluster default at execution (default {@code true}) exactly like a normal
     *       search. The distinction matters because the outer flag is a nullable {@code Boolean} that core has not yet
     *       resolved at rewrite time, so an unconditional pass-through would unbox {@code null}. With an effective
     *       {@code true} a shard failure degrades that one leg to partial results; with {@code false} the leg fails and
     *       {@code HybridFusionOrchestrator#groupLegHits} turns that into a whole-request failure. Fused-specific caveat:
     *       normalization is per-leg, so a partially-degraded leg shifts its own min/max and the fused ranking can differ
     *       from a complete run rather than merely returning fewer documents.</li>
     *   <li><b>point_in_time</b> is passed through so every leg and the self-erased round-2 query read one immutable view
     *       instead of N+1 independent reader instants — the consistency window that otherwise exists on a live-ingest
     *       index. Copying the request's indices alongside a PIT is safe: core's REST layer has already resolved a PIT
     *       request's indices to the PIT's own, and the transport layer derives shards from the PIT context regardless.</li>
     * </ul>
     */
    SearchRequest newLegRequest(final QueryBuilder leg, final int windowSize) {
        SearchSourceBuilder legSource = new SearchSourceBuilder().query(leg).size(windowSize).from(0).fetchSource(false);
        // A leg counts to the threshold when the rewrite asked for it (see legTotalHitsThreshold); otherwise totals stay
        // off, as before.
        if (Objects.nonNull(legTotalHitsThreshold)) {
            legSource.trackTotalHitsUpTo(legTotalHitsThreshold);
        } else {
            legSource.trackTotalHits(false);
        }
        if (legProfiling) {
            legSource.profile(true);
        }
        if (legExplain) {
            legSource.explain(true);
        }
        if (Objects.nonNull(timeout)) {
            legSource.timeout(timeout);
        }
        if (Objects.nonNull(postFilter)) {
            legSource.postFilter(postFilter);
        }
        if (Objects.nonNull(slice)) {
            legSource.slice(slice);
        }
        if (Objects.nonNull(pointInTimeId)) {
            // keepAlive is deliberately left unset: the PIT's original keep-alive governs, and a leg never extends it.
            legSource.pointInTimeBuilder(new PointInTimeBuilder(pointInTimeId));
        }

        SearchRequest legRequest = new SearchRequest(indices).indicesOptions(indicesOptions)
            .searchType(searchType)
            .source(legSource)
            .pipeline(SearchPipelineService.NOOP_PIPELINE_ID);
        if (Objects.nonNull(routing)) {
            legRequest.routing(routing);
        }
        if (Objects.nonNull(preference)) {
            legRequest.preference(preference);
        }
        if (Objects.nonNull(allowPartialSearchResults)) {
            legRequest.allowPartialSearchResults(allowPartialSearchResults);
        }
        // The raw getter reports 0 for "unset" and the setters reject anything below 1, so only forward real values.
        if (maxConcurrentShardRequests > 0) {
            legRequest.setMaxConcurrentShardRequests(maxConcurrentShardRequests);
        }
        if (Objects.nonNull(preFilterShardSize)) {
            legRequest.setPreFilterShardSize(preFilterShardSize);
        }
        if (Objects.nonNull(cancelAfterTimeInterval)) {
            legRequest.setCancelAfterTimeInterval(cancelAfterTimeInterval);
        }
        return legRequest;
    }
}
