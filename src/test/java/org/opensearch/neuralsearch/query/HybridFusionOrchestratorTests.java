/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TotalHits;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.OriginalIndices;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.neuralsearch.processor.normalization.RRFScoreNormalizer;
import org.opensearch.neuralsearch.search.explain.FusedDocExplanations;
import org.opensearch.neuralsearch.search.profile.FusedCoordinatorTimings;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.test.OpenSearchTestCase;

public class HybridFusionOrchestratorTests extends OpenSearchTestCase {

    private static final String INDEX = "test-index";

    private FusionSpec minMaxArithmetic() {
        return new FusionSpec(
            FusionSpec.Shape.NORMALIZATION_PROCESSOR,
            FusionSpec.TECHNIQUE_ARITHMETIC_MEAN,
            FusionSpec.NORMALIZATION_MIN_MAX,
            FusionSpec.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
    }

    private FusionSpec rrf(int rankConstant) {
        return new FusionSpec(
            FusionSpec.Shape.SCORE_RANKER_PROCESSOR,
            FusionSpec.TECHNIQUE_RRF,
            FusionSpec.NORMALIZATION_RRF,
            rankConstant,
            new float[0]
        );
    }

    /**
     * One MultiSearch item wrapping a SearchResponse whose hits carry the given (_id -> score) pairs, all from the default
     * index. Every hit carries an {@code _index}, as a real leg response's hits always do — it comes from the shard target
     * the response was read from — and fusion requires it.
     */
    private MultiSearchResponse.Item legItem(Map<String, Float> idToScore) {
        return legItemFromIndex(INDEX, idToScore);
    }

    /** Like {@link #legItem} but from a named index, for asserting cross-index document identity. */
    private MultiSearchResponse.Item legItemFromIndex(String index, Map<String, Float> idToScore) {
        SearchHit[] hits = new SearchHit[idToScore.size()];
        int i = 0;
        for (Map.Entry<String, Float> e : idToScore.entrySet()) {
            hits[i] = hitFrom(i, index, e.getKey(), e.getValue());
            i++;
        }
        return successfulItem(hits);
    }

    /**
     * One leg whose own hits span several indices — {@code "index/_id" -> score} — which is what a leg of a multi-index
     * search actually returns. Insertion-ordered so the clause order under assertion is deterministic.
     */
    private MultiSearchResponse.Item legItemAcrossIndices(LinkedHashMap<String, Float> indexAndIdToScore) {
        SearchHit[] hits = new SearchHit[indexAndIdToScore.size()];
        int i = 0;
        for (Map.Entry<String, Float> e : indexAndIdToScore.entrySet()) {
            String[] indexAndId = e.getKey().split("/", 2);
            hits[i] = hitFrom(i, indexAndId[0], indexAndId[1], e.getValue());
            i++;
        }
        return successfulItem(hits);
    }

    private SearchHit hitFrom(int docId, String index, String id, float score) {
        SearchHit hit = new SearchHit(docId, id, Map.of(), Map.of());
        hit.score(score);
        hit.shard(new SearchShardTarget("node-1", new ShardId(new Index(index, index + "-uuid"), 0), null, OriginalIndices.NONE));
        return hit;
    }

    /**
     * A leg that ran with {@code explain: true}: every hit carries the leg's own explanation of its raw score, which is
     * what the fan-out records and the response side nests under the normalized value. Insertion-ordered so the recorded
     * leg order is deterministic.
     */
    private MultiSearchResponse.Item explainedLegItem(LinkedHashMap<String, Float> idToScore) {
        SearchHit[] hits = new SearchHit[idToScore.size()];
        int i = 0;
        for (Map.Entry<String, Float> e : idToScore.entrySet()) {
            hits[i] = hitFrom(i, INDEX, e.getKey(), e.getValue());
            hits[i].explanation(Explanation.match(e.getValue(), "leg raw score"));
            i++;
        }
        return successfulItem(hits);
    }

    /**
     * The fused score each Top clause carries, keyed by the {@code _id} it addresses — read off the query rather than
     * assumed from the input, so an assertion about "the score round 2 ranks by" is about the query round 2 will run.
     */
    private Map<String, Float> fusedScoresById(QueryBuilder fused) {
        Map<String, Float> byId = new LinkedHashMap<>();
        for (QueryBuilder clause : ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery().should()) {
            ConstantScoreQueryBuilder top = (ConstantScoreQueryBuilder) clause;
            for (QueryBuilder filter : ((BoolQueryBuilder) top.innerQuery()).filter()) {
                if (filter instanceof IdsQueryBuilder ids) {
                    byId.put(ids.ids().iterator().next(), top.boost());
                }
            }
        }
        return byId;
    }

    /**
     * A leg item whose hits carry no {@code _index} — no shard target was ever set on them. Not a shape a real leg response
     * can have (a coordinator-side hit's {@code _index} comes from the shard it was read from), which is exactly why fusion
     * treats it as an invariant violation rather than something to work around.
     */
    private MultiSearchResponse.Item indexlessLegItem(Map<String, Float> idToScore) {
        SearchHit[] hits = new SearchHit[idToScore.size()];
        int i = 0;
        for (Map.Entry<String, Float> e : idToScore.entrySet()) {
            SearchHit hit = new SearchHit(i, e.getKey(), Map.of(), Map.of());
            hit.score(e.getValue());
            hits[i++] = hit;
        }
        return successfulItem(hits);
    }

    private MultiSearchResponse.Item successfulItem(SearchHit[] hits) {
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponseSections sections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        SearchResponse response = new SearchResponse(sections, null, 1, 1, 0, 10, ShardSearchFailure.EMPTY_ARRAY, null);
        return new MultiSearchResponse.Item(response, null);
    }

    private MultiSearchResponse.Item failedItem() {
        return new MultiSearchResponse.Item(null, new RuntimeException("leg boom"));
    }

    /** A wholly-failed leg whose failure carries a specific status, the way a real sub-search failure does. */
    private MultiSearchResponse.Item failedItemWithCause(Exception cause) {
        return new MultiSearchResponse.Item(null, cause);
    }

    /** A SUCCESSFUL MultiSearch item that lost a shard under allow_partial=true: HTTP 200, fewer hits, non-empty
     *  shardFailures (isFailure()==false). Models a partially-degraded leg. */
    private MultiSearchResponse.Item partialLegItem(Map<String, Float> idToScore) {
        SearchHit[] hits = new SearchHit[idToScore.size()];
        int i = 0;
        for (Map.Entry<String, Float> e : idToScore.entrySet()) {
            hits[i] = hitFrom(i, INDEX, e.getKey(), e.getValue());
            i++;
        }
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponseSections sections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        ShardSearchFailure[] failures = new ShardSearchFailure[] { new ShardSearchFailure(new RuntimeException("shard down")) };
        // totalShards=2, successful=1, skipped=0, one shard failure → partial but SUCCESSFUL item.
        SearchResponse response = new SearchResponse(sections, null, 2, 1, 0, 10, failures, null);
        return new MultiSearchResponse.Item(response, null);
    }

    private MultiSearchResponse multiSearch(MultiSearchResponse.Item... items) {
        return new MultiSearchResponse(items, 10L);
    }

    // ---- buildLegMultiSearch ----

    /**
     * The assembly contract only: one leg request per sub-query, each carrying that sub-query and the window. What a leg
     * inherits from the user's request is {@link CandidateScope}'s job and is covered by {@code CandidateScopeTests}.
     */
    public void testBuildLegMultiSearch_oneRequestPerLegBuiltFromTheScope() {
        SearchRequest request = new SearchRequest(INDEX);
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));

        MultiSearchRequest ms = HybridFusionOrchestrator.buildLegMultiSearch(CandidateScope.from(request), legs, 50);

        assertEquals(2, ms.requests().size());
        for (int i = 0; i < legs.size(); i++) {
            SearchRequest leg = ms.requests().get(i);
            assertEquals("leg " + i + " runs its own sub-query", legs.get(i), leg.source().query());
            assertEquals(50, leg.source().size());
            assertEquals(SearchPipelineService.NOOP_PIPELINE_ID, leg.pipeline());
        }
    }

    // ---- buildFusedQuery: Top+Tail / Top-only / match_none ----

    public void testBuildFusedQuery_topLevelWithAggs_keepsTail() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f, "2", 0.5f)), legItem(Map.of("2", 0.8f, "3", 0.4f)));
        SearchSourceBuilder source = new SearchSourceBuilder().aggregation(
            org.opensearch.search.aggregations.AggregationBuilders.terms("t").field("f")
        );

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10);

        assertTrue(fused instanceof HybridFusionQueryBuilder);
        BoolQueryBuilder self = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery();
        assertEquals("union of {1,2,3} scored in Top", 3, self.should().size());
        assertEquals("aggs → Tail retained", 1, self.filter().size());
    }

    public void testBuildFusedQuery_topLevelPlainTopK_topOnly() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f)));
        // track_total_hits:false, no aggs/highlight/explain → plain top-K, Tail dropped.
        SearchSourceBuilder source = new SearchSourceBuilder().trackTotalHits(false);

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10);

        BoolQueryBuilder self = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery();
        assertEquals(2, self.should().size());
        assertEquals("plain top-K → no Tail", 0, self.filter().size());
    }

    public void testBuildFusedQuery_tailDecisionIsDepthIndependent() {
        // The Tail decision comes from the REQUEST alone, never from whether this hybrid is top-level or nested: the
        // fused query self-erases the same way at any depth, and an enclosing clause simply intersects it. So with aggs
        // present the Tail is retained — nesting no longer silently downgrades agg/total_hits accuracy.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f)));
        SearchSourceBuilder source = new SearchSourceBuilder().aggregation(
            org.opensearch.search.aggregations.AggregationBuilders.terms("t").field("f")
        );

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10);

        BoolQueryBuilder self = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery();
        assertEquals("aggs → Tail retained regardless of nesting depth", 1, self.filter().size());
    }

    public void testBuildFusedQuery_whenSortedByField_keepsTail() {
        // A non-_score sort ranks by the sort key, so the fused scores only pick the candidate set. With Top only, the
        // request would sort a window-sized arbitrary subset of its matches; the Tail widens round 2 to the full union.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f)));
        SearchSourceBuilder source = new SearchSourceBuilder().trackTotalHits(false).sort("price");

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10);

        BoolQueryBuilder self = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery();
        assertEquals("field sort → Tail retained so the sort covers the full leg union", 1, self.filter().size());
    }

    public void testBuildFusedQuery_whenSortedByScoreOnly_staysTopOnly() {
        // Sorting by _score is the fused ranking itself, so it must not drag in a Tail the request does not need.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f)));
        SearchSourceBuilder source = new SearchSourceBuilder().trackTotalHits(false).sort("_score");

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10);

        BoolQueryBuilder self = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery();
        assertEquals("_score sort → still plain top-K, no Tail", 0, self.filter().size());
    }

    public void testBuildFusedQuery_whenCollapseInnerHits_keepsTail() {
        // collapse.inner_hits makes core re-run THIS query once per group, filtered to the group key. A group's members
        // are whatever shares that key — unrelated to the fused window — so with Top only every member that ranked outside
        // the window matches nothing and silently vanishes from the expansion, where classic hybrid returns all of them.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f)));
        SearchSourceBuilder source = new SearchSourceBuilder().trackTotalHits(false)
            .collapse(new CollapseBuilder("grp").setInnerHits(new InnerHitBuilder("members")));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10);

        BoolQueryBuilder self = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery();
        assertEquals("collapse.inner_hits → Tail retained so a group expands to all of its members", 1, self.filter().size());
    }

    public void testBuildFusedQuery_whenCollapseWithoutInnerHits_staysTopOnly() {
        // Plain collapse groups the documents round 2 already returns — core runs no expansion search at all — so it must
        // not drag in a Tail the request did not ask for.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f)));
        SearchSourceBuilder source = new SearchSourceBuilder().trackTotalHits(false).collapse(new CollapseBuilder("grp"));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10);

        BoolQueryBuilder self = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery();
        assertEquals("collapse grouping alone → still plain top-K, no Tail", 0, self.filter().size());
    }

    public void testBuildFusedQuery_emptyResult_matchNone() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of()));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(new SearchSourceBuilder(), ms, legs, minMaxArithmetic(), 10);

        assertTrue(fused instanceof MatchNoneQueryBuilder);
    }

    public void testBuildFusedQuery_windowCapsRankedDocs() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f, "2", 0.8f, "3", 0.7f, "4", 0.6f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(
            new SearchSourceBuilder().trackTotalHits(false),
            ms,
            legs,
            minMaxArithmetic(),
            2
        );

        BoolQueryBuilder self = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery();
        assertEquals("window=2 caps the Top to 2 docs", 2, self.should().size());
    }

    // ---- leg failure: a wholly-failed leg fails fast, a partially-degraded leg is fused ----

    public void testBuildFusedQuery_whenAnyLegFailed_thenFailsFast() {
        // A wholly-failed leg (all shards down / non-partial error -> Item.isFailure) fails the whole request — fusing
        // over a missing leg would silently change the ranking function. (A merely partial leg degrades instead — see
        // testBuildFusedQuery_whenLegPartiallyFailed_thenFused.) The failing leg's index is reported, cause chained.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f, "2", 0.5f)), failedItem());

        OpenSearchStatusException e = expectThrows(
            OpenSearchStatusException.class,
            () -> HybridFusionOrchestrator.buildFusedQuery(
                new SearchSourceBuilder().trackTotalHits(false),
                ms,
                legs,
                minMaxArithmetic(),
                10
            )
        );
        assertTrue("reports the failing leg index", e.getMessage().contains("fused-mode sub-query 1 failed"));
        assertNotNull("chains the leg failure as cause", e.getCause());
        assertTrue(e.getCause().getMessage().contains("leg boom"));
        // A bare RuntimeException really is a server error, so 500 here is the derived status, not a default.
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, e.status());
    }

    public void testBuildFusedQuery_whenAllLegsFailed_thenFailsFast() {
        // All legs failing also fails fast — on the first failed leg (index 0).
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(failedItem(), failedItem());

        OpenSearchStatusException e = expectThrows(
            OpenSearchStatusException.class,
            () -> HybridFusionOrchestrator.buildFusedQuery(new SearchSourceBuilder(), ms, legs, minMaxArithmetic(), 10)
        );
        assertTrue(e.getMessage().contains("fused-mode sub-query 0 failed"));
        assertNotNull(e.getCause());
    }

    /**
     * A leg failure must keep the status the leg itself reported. The user's own mistake — say a malformed range bound,
     * which classic hybrid answers with 400 {@code query_shard_exception} — was arriving as a 500 because the wrapper was
     * an {@code IllegalStateException} and {@code ExceptionsHelper#status} has no case for it, leaving the real status
     * reachable only under {@code caused_by}.
     */
    public void testBuildFusedQuery_whenLegFailedWithClientError_thenStatusStaysBadRequest() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(
            legItem(Map.of("1", 0.9f)),
            failedItemWithCause(
                new SearchPhaseExecutionException(
                    "query",
                    "all shards failed",
                    new ShardSearchFailure[] { new ShardSearchFailure(new IllegalArgumentException("bad range bound")) }
                )
            )
        );

        OpenSearchStatusException e = expectThrows(
            OpenSearchStatusException.class,
            () -> HybridFusionOrchestrator.buildFusedQuery(new SearchSourceBuilder(), ms, legs, minMaxArithmetic(), 10)
        );
        assertEquals("the leg's own 400 must survive the wrapper", RestStatus.BAD_REQUEST, e.status());
        assertEquals("and the status a REST layer derives must agree", RestStatus.BAD_REQUEST, ExceptionsHelper.status(e));
    }

    /** Same for a queue rejection: masking 429 as 500 means a client's retry-on-429 never fires. */
    public void testBuildFusedQuery_whenLegRejected_thenStatusStaysTooManyRequests() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(
            legItem(Map.of("1", 0.9f)),
            failedItemWithCause(new OpenSearchRejectedExecutionException("search queue full"))
        );

        OpenSearchStatusException e = expectThrows(
            OpenSearchStatusException.class,
            () -> HybridFusionOrchestrator.buildFusedQuery(new SearchSourceBuilder(), ms, legs, minMaxArithmetic(), 10)
        );
        assertEquals(RestStatus.TOO_MANY_REQUESTS, e.status());
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ExceptionsHelper.status(e));
    }

    public void testBuildFusedQuery_whenLegPartiallyFailed_thenFusedWithWarning() {
        // A leg that lost some shards under allow_partial_search_results=true is a SUCCESSFUL item with fewer hits — it
        // is fused (degrade, matching OpenSearch's default), not rejected. groupLegHits only hard-fails a wholly-failed
        // item, so a partial-but-successful leg flows through — but emits a Warning header naming it, because per-leg
        // normalization means the degraded leg can shift the fused ranking, not just drop docs.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(partialLegItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(
            new SearchSourceBuilder().trackTotalHits(false),
            ms,
            legs,
            minMaxArithmetic(),
            10
        );

        assertTrue(fused instanceof HybridFusionQueryBuilder);
        assertEquals(
            "both legs' docs fused despite one leg's partial shard failure",
            2,
            ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery().should().size()
        );
        assertWarnings(
            "[hybrid] fused-mode sub-query [0] returned partial results (shard failures); fused scores were computed "
                + "over an incomplete result set, so ranking may differ from a complete run"
        );
    }

    public void testBuildFusedQuery_whenNoLegDegraded_thenNoWarning() {
        // Clean legs must not emit a warning (OpenSearchTestCase fails the test on any unasserted warning).
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f)));

        HybridFusionOrchestrator.buildFusedQuery(new SearchSourceBuilder().trackTotalHits(false), ms, legs, minMaxArithmetic(), 10);
    }

    // ---- document identity: _index + _id ----

    public void testBuildFusedQuery_whenSameIdInDifferentIndices_thenNotConflated() {
        // The bug this guards: keying on _id alone made a doc in idx-a and a DIFFERENT doc in idx-b with the same _id
        // fuse as one entity, and the self-erased _id Top then boosted both to that one score. Keyed by _index + _id they
        // stay two documents, and each Top clause is index-qualified so a score lands on exactly one of them.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItemFromIndex("idx-a", Map.of("1", 0.9f)), legItemFromIndex("idx-b", Map.of("1", 0.8f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(
            new SearchSourceBuilder().trackTotalHits(false),
            ms,
            legs,
            minMaxArithmetic(),
            10
        );

        BoolQueryBuilder self = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery();
        assertEquals("same _id in two indices stays two distinct fused docs", 2, self.should().size());
        for (QueryBuilder clause : self.should()) {
            QueryBuilder inner = ((ConstantScoreQueryBuilder) clause).innerQuery();
            assertTrue("multi-index Top clause must be index-qualified", inner instanceof BoolQueryBuilder);
            assertEquals("qualified by _id AND _index", 2, ((BoolQueryBuilder) inner).filter().size());
        }
    }

    /**
     * The blocker this guards. Qualification used to be dropped when the window spanned a single index, but the window is
     * not evidence about the request — one index outranking its siblings, or a window_size below the fused set size, both
     * yield a single-index window for a search that round 2 still executes against every requested index, where a sibling
     * index's same-_id doc matches the bare ids clause and inherits the fused score. So qualify unconditionally.
     */
    public void testBuildFusedQuery_whenWindowSpansOneIndex_thenTopIsStillQualified() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItemFromIndex("idx-a", Map.of("1", 0.9f)), legItemFromIndex("idx-a", Map.of("2", 0.8f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(
            new SearchSourceBuilder().trackTotalHits(false),
            ms,
            legs,
            minMaxArithmetic(),
            10
        );

        BoolQueryBuilder self = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery();
        assertEquals(2, self.should().size());
        for (QueryBuilder clause : self.should()) {
            QueryBuilder inner = ((ConstantScoreQueryBuilder) clause).innerQuery();
            assertTrue("a single-index window must not drop the _index qualification", inner instanceof BoolQueryBuilder);
            assertEquals("qualified by _id AND _index", 2, ((BoolQueryBuilder) inner).filter().size());
        }
    }

    /**
     * A hit with no {@code _index} cannot be fused, and the request fails rather than degrading. The previous behaviour was
     * to drop the whole {@code indices} array and address the window by {@code _id} alone — which is the same-{@code _id}
     * conflation the two tests above exist to prevent, reintroduced for every clause because one hit lacked an index. Since
     * a coordinator-side hit always carries its {@code _index}, this shape is a broken invariant, not user input.
     */
    public void testBuildFusedQuery_whenAnyHitCarriesNoIndex_thenFailsRatherThanDegrading() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItemFromIndex("idx-a", Map.of("2", 0.8f)), indexlessLegItem(Map.of("1", 0.9f)));

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> HybridFusionOrchestrator.buildFusedQuery(
                new SearchSourceBuilder().trackTotalHits(false),
                ms,
                legs,
                minMaxArithmetic(),
                10
            )
        );
        assertTrue("names the offending leg and hit: " + e.getMessage(), e.getMessage().contains("sub-query 1 returned a hit [_id: 1]"));
        assertTrue(e.getMessage().contains("no [_index]"));
    }

    // ---- inner_hits are registered without executing the legs ----

    public void testBuildFusedQuery_whenLegHasInnerHits_thenRegisteredWithoutTail() {
        // inner_hits are built in the fetch phase from the registered contexts, so a leg only needs to be REGISTERED,
        // never executed. With track_total_hits:false and no aggs there is nothing else needing the Tail, so the query
        // stays Top-only (no redundant leg re-execution) while inner_hits are still extractable.
        QueryBuilder nestedLeg = new org.opensearch.index.query.NestedQueryBuilder(
            "user",
            new MatchQueryBuilder("user.name", "alice"),
            org.apache.lucene.search.join.ScoreMode.None
        ).innerHit(new org.opensearch.index.query.InnerHitBuilder());
        List<QueryBuilder> legs = List.of(nestedLeg, new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(
            new SearchSourceBuilder().trackTotalHits(false),
            ms,
            legs,
            minMaxArithmetic(),
            10
        );

        HybridFusionQueryBuilder fusedBuilder = (HybridFusionQueryBuilder) fused;
        assertEquals("leg inner_hits no longer force the Tail", 0, fusedBuilder.buildSelfErasedQuery().filter().size());
        Map<String, org.opensearch.index.query.InnerHitContextBuilder> innerHits = new java.util.HashMap<>();
        fusedBuilder.extractInnerHitBuilders(innerHits);
        assertFalse("inner_hits must still be registered for the fetch phase", innerHits.isEmpty());
    }

    // ---- knn/neural leg materialized in the Tail (no second ANN walk), addressed by _index + _id ----

    /** A leg reporting a materializable writeable name, without touching KNN-internal construction/validation. */
    private QueryBuilder legNamed(String writeableName) {
        return new MatchQueryBuilder("vec", "q") {
            @Override
            public String getWriteableName() {
                return writeableName;
            }
        };
    }

    /** An aggregation is the cheapest Tail trigger, so the materialized leg is there to inspect. */
    private SearchSourceBuilder sourceWithAggregation() {
        return new SearchSourceBuilder().aggregation(org.opensearch.search.aggregations.AggregationBuilders.terms("t").field("f"));
    }

    private BoolQueryBuilder tailOf(QueryBuilder fused) {
        return (BoolQueryBuilder) ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery().filter().get(0);
    }

    /** Asserts a clause addresses exactly these ids inside exactly this index. */
    private void assertAddressedTo(QueryBuilder clause, String index, String... ids) {
        assertTrue("expected an _index-qualified bool, got " + clause, clause instanceof BoolQueryBuilder);
        BoolQueryBuilder qualified = (BoolQueryBuilder) clause;
        assertEquals("qualified by _id AND _index", 2, qualified.filter().size());
        assertEquals(Set.of(ids), ((IdsQueryBuilder) qualified.filter().get(0)).ids());
        TermQueryBuilder indexTerm = (TermQueryBuilder) qualified.filter().get(1);
        assertEquals("_index", indexTerm.fieldName());
        assertEquals(index, indexTerm.value());
    }

    public void testBuildFusedQuery_knnLeg_materializedAsQualifiedDocsInTail() {
        // A materializable leg's Lucene match set IS its returned top-k, so legQueriesForTail replaces it with a direct
        // address of those hits rather than re-walking the ANN graph — and addresses them by _index + _id, because the
        // Tail is a filter and therefore decides the match set.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), legNamed("knn"));
        MultiSearchResponse ms = multiSearch(
            legItemFromIndex(INDEX, Map.of("1", 0.9f)),
            legItemFromIndex(INDEX, Map.of("2", 0.8f, "3", 0.7f))
        );

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(sourceWithAggregation(), ms, legs, minMaxArithmetic(), 10);

        BoolQueryBuilder tail = tailOf(fused);
        assertEquals(2, tail.should().size());
        assertEquals("the lexical leg stays a real query", legs.get(0), tail.should().get(0));
        assertAddressedTo(tail.should().get(1), INDEX, "2", "3");
    }

    /**
     * The blocker this guards. A leg of a multi-index search returns hits from several indices, and each id must be
     * addressed inside the index it came from. Addressed by {@code _id} alone, every same-{@code _id} sibling document in
     * the other index passed the Tail filter — counted into {@code total_hits}, into every aggregation bucket, and
     * returned as a score-0 hit — which inflates exactly the numbers the Tail exists to make correct. Qualifying the Top
     * did not help: the Tail was built straight from the leg hits, never from the ranked window's resolved indices.
     */
    public void testBuildFusedQuery_whenKnnLegSpansTwoIndices_thenTailAddressesEachIndexSeparately() {
        LinkedHashMap<String, Float> knnHits = new LinkedHashMap<>();
        knnHits.put("idx-a/1", 0.9f);
        knnHits.put("idx-a/2", 0.8f);
        knnHits.put("idx-b/1", 0.7f);
        List<QueryBuilder> legs = List.of(legNamed("knn"));
        MultiSearchResponse ms = multiSearch(legItemAcrossIndices(knnHits));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(sourceWithAggregation(), ms, legs, minMaxArithmetic(), 10);

        BoolQueryBuilder tail = tailOf(fused);
        assertEquals(1, tail.should().size());
        BoolQueryBuilder perIndex = (BoolQueryBuilder) tail.should().get(0);
        assertEquals("one qualified clause per index the leg returned hits from", 2, perIndex.should().size());
        assertAddressedTo(perIndex.should().get(0), "idx-a", "1", "2");
        assertAddressedTo(perIndex.should().get(1), "idx-b", "1");
        assertTrue(
            "no clause may address an _id without its _index",
            perIndex.should().stream().noneMatch(clause -> clause instanceof IdsQueryBuilder)
        );
    }

    /**
     * The Top and the Tail must identify a document the same way — they are the scoring half and the matching half of one
     * query, and a Tail narrower than the Top would filter away the very documents the Top scored. Both go through
     * {@link HybridFusionQueryBuilder#addressDocuments}, so this compares the two builders directly rather than
     * re-describing the shape.
     */
    public void testBuildFusedQuery_whenLegIsMaterialized_thenTopAndTailAddressTheDocumentIdentically() {
        List<QueryBuilder> legs = List.of(legNamed("knn"));
        MultiSearchResponse ms = multiSearch(legItemFromIndex(INDEX, Map.of("1", 0.9f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(sourceWithAggregation(), ms, legs, minMaxArithmetic(), 10);

        BoolQueryBuilder self = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery();
        QueryBuilder topAddress = ((ConstantScoreQueryBuilder) self.should().get(0)).innerQuery();
        QueryBuilder tailAddress = ((BoolQueryBuilder) self.filter().get(0)).should().get(0);
        assertEquals("Top and Tail must address the same document identically", topAddress, tailAddress);
    }

    public void testBuildFusedQuery_whenMaterializedLegHitsCarryNoIndex_thenFailsRatherThanDegrading() {
        // The Tail is a filter, so an _id-only clause here widens the match set to every same-_id document in the cluster —
        // inflating total_hits and every aggregation bucket. The invariant is asserted once, where every leg's hits enter
        // fusion, so the Tail path refuses the same shape the Top path does.
        List<QueryBuilder> legs = List.of(legNamed("knn"));
        MultiSearchResponse ms = multiSearch(indexlessLegItem(Map.of("2", 0.8f, "3", 0.7f)));

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> HybridFusionOrchestrator.buildFusedQuery(sourceWithAggregation(), ms, legs, minMaxArithmetic(), 10)
        );
        assertTrue(e.getMessage().contains("cannot be fused"));
    }

    /**
     * An ANN leg that matched nothing must keep matching nothing. {@code bool{should: []}} compiles to
     * {@code MatchAllDocsQuery}, so an empty leg rendered as an empty bool would make the Tail match every document in the
     * index — total_hits and every aggregation would report the whole corpus. The bare ids query this replaced was only
     * accidentally safe (core rewrites an empty ids query to {@code match_none}), so the guard is explicit here.
     */
    public void testBuildFusedQuery_whenKnnLegReturnedNothing_thenTailClauseIsMatchNoneNotMatchAll() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), legNamed("knn"));
        MultiSearchResponse ms = multiSearch(legItemFromIndex(INDEX, Map.of("1", 0.9f)), legItemFromIndex(INDEX, Map.of()));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(sourceWithAggregation(), ms, legs, minMaxArithmetic(), 10);

        BoolQueryBuilder tail = tailOf(fused);
        assertEquals(2, tail.should().size());
        assertTrue("an empty ANN leg must be match_none, never an empty bool", tail.should().get(1) instanceof MatchNoneQueryBuilder);
    }

    // ---- the truncation bound: an address of the returned hits stands for the match set only if nothing was truncated ----

    /**
     * The bound that makes materialization sound, and the under-count it closes. A materialized leg stands for the documents
     * it <i>returned</i>, which is {@code min(matches, window_size)} because {@code newLegRequest} caps every leg at
     * {@code size = window_size}. So a leg that filled the window may have matched documents it never returned — and the
     * Tail, being a {@code filter}, would leave those out of {@code total_hits} and out of every aggregation bucket, at
     * HTTP 200, where classic hybrid counts them. Such a leg is therefore kept as the real query and counted properly.
     *
     * <p>The same leg one window wider is the control: it came back short, so it was not truncated and the address is exact.
     * That is what makes the first half a truncation test rather than materialization being switched off.
     */
    public void testBuildFusedQuery_whenMaterializableLegFilledTheWindow_thenKeptAsTheRealQueryInTheTail() {
        List<QueryBuilder> legs = List.of(legNamed("knn"));
        Map<String, Float> twoHits = Map.of("2", 0.8f, "3", 0.7f);

        QueryBuilder truncated = HybridFusionOrchestrator.buildFusedQuery(
            sourceWithAggregation(),
            multiSearch(legItemFromIndex(INDEX, twoHits)),
            legs,
            minMaxArithmetic(),
            twoHits.size()
        );
        assertSame(
            "a leg that returned as many hits as the window let it may have matched more, so it must be counted for real",
            legs.get(0),
            tailOf(truncated).should().get(0)
        );

        QueryBuilder exact = HybridFusionOrchestrator.buildFusedQuery(
            sourceWithAggregation(),
            multiSearch(legItemFromIndex(INDEX, twoHits)),
            legs,
            minMaxArithmetic(),
            twoHits.size() + 1
        );
        assertAddressedTo(tailOf(exact).should().get(0), INDEX, "2", "3");
    }

    /**
     * The defect the bound was added for. {@code neural} is a materializable <i>name</i>, but against a
     * {@code rank_features} semantic embedding field a {@code neural} query rewrites into {@code neural_sparse}, whose match
     * set is every document holding a query token — far larger than the window. Fused mode substitutes the Tail before the
     * legs are rewritten, so the coordinator sees the same {@code neural} name for the sparse leg as for a dense one and
     * cannot tell them apart. The truncation test is what stops the sparse leg from being materialized, because over a real
     * corpus it fills the window.
     */
    public void testBuildFusedQuery_whenNeuralLegFilledTheWindow_thenKeptAsTheRealQueryInTheTail() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), legNamed("neural"));
        MultiSearchResponse ms = multiSearch(
            legItemFromIndex(INDEX, Map.of("1", 0.9f)),
            legItemFromIndex(INDEX, Map.of("2", 0.8f, "3", 0.7f))
        );

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(sourceWithAggregation(), ms, legs, minMaxArithmetic(), 2);

        assertSame(
            "a neural leg that filled the window may be a sparse one, whose match set the window truncated",
            legs.get(1),
            tailOf(fused).should().get(1)
        );
    }

    /**
     * The test is a necessary condition, not a sufficient one — the type check stays. A term-defined leg that came back
     * short of the window is still kept as the real query: materializing it would be exact but pointless, since re-running
     * it walks no graph, and it would replace whatever inner structure the leg compiles to on the shard.
     */
    public void testBuildFusedQuery_whenNonAnnLegCameBackShort_thenStillNotMaterialized() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"));
        MultiSearchResponse ms = multiSearch(legItemFromIndex(INDEX, Map.of("1", 0.9f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(sourceWithAggregation(), ms, legs, minMaxArithmetic(), 10);

        assertSame("only an ANN leg is worth materializing", legs.get(0), tailOf(fused).should().get(0));
    }

    /**
     * The bound belongs to the Tail, not to the carried form. On the Top-only path there is no Tail by construction, so what
     * a substitute addresses cannot reach {@code total_hits} or an aggregation — applying the bound there would buy a
     * shard-side ANN compile (and, for {@code neural}, a second inference) for a reporting field alone. Both paths share one
     * materializer, so this pins that the truncation test did not leak into the one where nothing counts.
     */
    public void testBuildFusedQuery_whenTopOnlyAndNamedLegFilledTheWindow_thenStillMaterializedForRegistration() {
        List<QueryBuilder> legs = List.of(legNamed("knn").queryName("vector"));
        MultiSearchResponse ms = multiSearch(legItemFromIndex(INDEX, Map.of("2", 0.8f, "3", 0.7f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(topOnlySource(), ms, legs, minMaxArithmetic(), 2);

        assertEquals("registration must not turn a Top-only query into Top+Tail", 0, tailFilterCount(fused));
        QueryBuilder carried = namedOnlyLegsOf(fused).get(0);
        assertEquals("vector", carried.queryName());
        assertAddressedTo(carried, INDEX, "2", "3");
    }

    // ---- weighted combination + highlight/totals tail triggers (explain/profile do NOT trigger the Tail) ----

    public void testBuildFusedQuery_withPerLegWeights_fusesWithoutError() {
        // Weighted arithmetic mean: exercises weightsParams() building the combination technique from FusionSpec weights.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f, "2", 0.5f)), legItem(Map.of("2", 0.8f, "3", 0.4f)));
        FusionSpec weighted = new FusionSpec(
            FusionSpec.Shape.NORMALIZATION_PROCESSOR,
            FusionSpec.TECHNIQUE_ARITHMETIC_MEAN,
            FusionSpec.NORMALIZATION_MIN_MAX,
            FusionSpec.DEFAULT_RANK_CONSTANT,
            new float[] { 0.7f, 0.3f }
        );

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(
            new SearchSourceBuilder().trackTotalHits(false),
            ms,
            legs,
            weighted,
            10
        );

        assertTrue(fused instanceof HybridFusionQueryBuilder);
        assertEquals(3, ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery().should().size());
    }

    public void testBuildFusedQuery_explainDoesNotTriggerTail() {
        // explain/profile no longer force the Tail: fusion is computed on the coordinator (Top is constant_score(ids)),
        // so the Lucene tree has no fusion breakdown to explain and the Tail would only re-execute legs. With
        // track_total_hits:false there is nothing else needing the Tail, so the query is Top-only.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)));
        SearchSourceBuilder source = new SearchSourceBuilder().trackTotalHits(false).explain(true);

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10);

        assertEquals("explain alone → no Tail", 0, ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery().filter().size());
    }

    // ---- buildFusedQuery: the per-leg breakdown recorded for the request's `explain` ----

    /**
     * The contract of the recorded breakdown: one entry per document fusion ranked, one node per leg that matched it,
     * that leg's own round-1 explanation kept under its normalized value, and a top value equal to the score round 2 will
     * actually rank the document by. The last part is what makes the tree an account of the ranking rather than a
     * plausible-looking set of numbers beside it.
     */
    public void testBuildFusedQuery_whenLegsRanExplained_thenEveryRankedDocumentIsRecorded() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(
            explainedLegItem(new LinkedHashMap<>(Map.of("1", 0.9f))),
            explainedLegItem(new LinkedHashMap<>(Map.of("1", 0.6f)))
        );
        FusedDocExplanations explanations = new FusedDocExplanations();

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(
            new SearchSourceBuilder().trackTotalHits(false).explain(true),
            ms,
            legs,
            minMaxArithmetic(),
            10,
            new FusedCoordinatorTimings(),
            explanations
        );

        assertFalse("an explained fan-out records what it fused", explanations.isEmpty());
        assertEquals(
            "the wording is the combination technique's own",
            "arithmetic_mean combination of:",
            explanations.combinationDescription()
        );
        assertEquals("and the normalizer's own", "min_max normalization of:", explanations.normalizationDescription());

        Explanation tree = explanations.explain(FusedDocExplanations.documentKey(INDEX, "1"), Float.NaN);
        assertNotNull("the one ranked document is described", tree);
        assertEquals("one node per leg that matched", 2, tree.getDetails().length);
        for (int leg = 0; leg < 2; leg++) {
            Explanation legNode = tree.getDetails()[leg];
            assertEquals("min_max normalization of:", legNode.getDescription());
            assertEquals("the leg's own explanation is kept under its normalized value", 1, legNode.getDetails().length);
            assertEquals("leg raw score", legNode.getDetails()[0].getDescription());
        }
        assertEquals(
            "the described score is the one fusion computed, which for an undegenerate document is also the one round 2 "
                + "ranks by (see the floored case below, where the two differ)",
            fusedScoresById(fused).get("1"),
            tree.getValue().floatValue(),
            0.0f
        );
    }

    /**
     * The normalization node's wording comes from the normalizer's {@code describe()}, not its {@code techniqueName()},
     * which for rrf are different strings: the name alone would describe a normalization the request did not ask for,
     * since two queries differing only in {@code rank_constant} score differently. It read the name at first, so this is
     * the assertion that would have caught it — and the one the min_max case above structurally cannot make, min_max
     * being the technique where the two strings coincide.
     */
    public void testBuildFusedQuery_whenNormalizationIsRrf_thenTheDescriptionNamesTheRankConstant() {
        int rankConstant = 25;
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(
            explainedLegItem(new LinkedHashMap<>(Map.of("1", 0.9f))),
            explainedLegItem(new LinkedHashMap<>(Map.of("1", 0.6f)))
        );
        FusedDocExplanations explanations = new FusedDocExplanations();

        HybridFusionOrchestrator.buildFusedQuery(
            new SearchSourceBuilder().trackTotalHits(false).explain(true),
            ms,
            legs,
            rrf(rankConstant),
            10,
            new FusedCoordinatorTimings(),
            explanations
        );

        assertEquals(
            "the configured rank constant, in classic hybrid's wording",
            "rrf, rank_constant [" + rankConstant + "] normalization of:",
            explanations.normalizationDescription()
        );
        // Pinned negatively as well, because the pre-fix string is a prefix of the correct one: a contains() assertion
        // would have passed against it.
        assertNotEquals("rrf normalization of:", explanations.normalizationDescription());

        Explanation tree = explanations.explain(FusedDocExplanations.documentKey(INDEX, "1"), Float.NaN);
        for (Explanation legNode : tree.getDetails()) {
            assertEquals("rrf, rank_constant [" + rankConstant + "] normalization of:", legNode.getDescription());
        }
    }

    /**
     * A leg that did not match a document contributes no node rather than a zero one — the same choice classic hybrid
     * makes. A zero node would read as "this leg scored it at zero" when the leg never saw it.
     */
    public void testBuildFusedQuery_whenALegDidNotMatchADocument_thenOnlyTheMatchingLegsAreRecorded() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        // Only document 2 is in both legs; 1 is leg 0's alone and 3 is leg 1's alone.
        MultiSearchResponse ms = multiSearch(
            explainedLegItem(new LinkedHashMap<>(Map.of("1", 0.9f, "2", 0.5f))),
            explainedLegItem(new LinkedHashMap<>(Map.of("2", 0.8f, "3", 0.4f)))
        );
        FusedDocExplanations explanations = new FusedDocExplanations();

        HybridFusionOrchestrator.buildFusedQuery(
            new SearchSourceBuilder().trackTotalHits(false).explain(true),
            ms,
            legs,
            minMaxArithmetic(),
            10,
            new FusedCoordinatorTimings(),
            explanations
        );

        assertEquals(
            "the document both legs matched keeps both nodes",
            2,
            explanations.explain(FusedDocExplanations.documentKey(INDEX, "2"), Float.NaN).getDetails().length
        );
        assertEquals(
            "a document only leg 0 matched gets one node, not two with a zero",
            1,
            explanations.explain(FusedDocExplanations.documentKey(INDEX, "1"), Float.NaN).getDetails().length
        );
        assertEquals(
            "and likewise for one only the last leg matched",
            1,
            explanations.explain(FusedDocExplanations.documentKey(INDEX, "3"), Float.NaN).getDetails().length
        );
    }

    /**
     * Recording happens after the window cut, so what is described is exactly what round 2 will rank. A document the
     * window dropped has no Top clause to carry a fused score, so describing it would name a ranking that never happened.
     */
    public void testBuildFusedQuery_whenTheWindowDroppedADocument_thenOnlyTheWindowIsRecorded() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"));
        MultiSearchResponse ms = multiSearch(explainedLegItem(new LinkedHashMap<>(Map.of("1", 0.9f, "2", 0.5f, "3", 0.1f))));
        FusedDocExplanations explanations = new FusedDocExplanations();

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(
            new SearchSourceBuilder().trackTotalHits(false).explain(true),
            ms,
            legs,
            minMaxArithmetic(),
            1,
            new FusedCoordinatorTimings(),
            explanations
        );

        Map<String, Float> ranked = fusedScoresById(fused);
        assertEquals("window=1 ranks one document", 1, ranked.size());
        String rankedId = ranked.keySet().iterator().next();
        assertNotNull(
            "the ranked document is described",
            explanations.explain(FusedDocExplanations.documentKey(INDEX, rankedId), Float.NaN)
        );
        for (String dropped : List.of("1", "2", "3")) {
            if (dropped.equals(rankedId)) {
                continue;
            }
            assertNull(
                "a document the window dropped has no fused score to describe",
                explanations.explain(FusedDocExplanations.documentKey(INDEX, dropped), Float.NaN)
            );
        }
    }

    /**
     * The cost of the always-constructed collector on the path that never asks for it. An unexplained request's legs return
     * hits with no explanation, so nothing is recorded and nothing about the fused query changes — which is what makes the
     * explained and unexplained runs compute the same ranking rather than two code paths that agree by inspection.
     */
    public void testBuildFusedQuery_whenLegsRanUnexplained_thenNothingIsRecorded() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        SearchSourceBuilder source = new SearchSourceBuilder().trackTotalHits(false);
        FusedDocExplanations explanations = new FusedDocExplanations();

        QueryBuilder recorded = HybridFusionOrchestrator.buildFusedQuery(
            source,
            multiSearch(legItem(Map.of("1", 0.9f, "2", 0.5f)), legItem(Map.of("2", 0.8f))),
            legs,
            minMaxArithmetic(),
            10,
            new FusedCoordinatorTimings(),
            explanations
        );
        QueryBuilder plain = HybridFusionOrchestrator.buildFusedQuery(
            source,
            multiSearch(legItem(Map.of("1", 0.9f, "2", 0.5f)), legItem(Map.of("2", 0.8f))),
            legs,
            minMaxArithmetic(),
            10
        );

        assertTrue("legs that ran unexplained have nothing to record", explanations.isEmpty());
        assertNull("so no document gets a tree", explanations.combinationDescription());
        assertEquals("and the fused query is the one the overload without a collector builds", plain, recorded);
    }

    public void testBuildFusedQuery_profileDoesNotTriggerTail() {
        // Same as explain: profiling the self-erased query would only time a redundant re-execution of legs that already
        // ran in the fan-out, so profile is not a Tail trigger. With track_total_hits:false the query is Top-only.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)));
        SearchSourceBuilder source = new SearchSourceBuilder().trackTotalHits(false).profile(true);

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10);

        assertEquals("profile alone → no Tail", 0, ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery().filter().size());
    }

    public void testBuildFusedQuery_defaultTrackTotalHits_keepsTailForAccurateCount() {
        // No aggs/explain and track_total_hits left at default → wantsTotalsBeyondWindow keeps the Tail for the count.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f, "2", 0.5f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(new SearchSourceBuilder(), ms, legs, minMaxArithmetic(), 10);

        assertEquals("default totals → Tail retained", 1, ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery().filter().size());
    }

    public void testBuildFusedQuery_nullSource_keepsTail() {
        // A null source (defensive) is treated as "wants totals" → Tail retained.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(null, ms, legs, minMaxArithmetic(), 10);

        assertEquals(1, ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery().filter().size());
    }

    public void testBuildFusedQuery_neuralNamedLeg_materializedAsQualifiedDocs() {
        // "neural" is also a materializable name → its leg is addressed by its returned hits in the Tail, not re-walked.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), legNamed("neural"));
        MultiSearchResponse ms = multiSearch(legItemFromIndex(INDEX, Map.of("1", 0.9f)), legItemFromIndex(INDEX, Map.of("2", 0.8f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(sourceWithAggregation(), ms, legs, minMaxArithmetic(), 10);

        assertAddressedTo(tailOf(fused).should().get(1), INDEX, "2");
    }

    // ---- a leg's _name reaches matched_queries without the Tail: carried for registration, not for execution ----

    private List<QueryBuilder> namedOnlyLegsOf(QueryBuilder fused) {
        return ((HybridFusionQueryBuilder) fused).namedOnlyQueries();
    }

    private SearchSourceBuilder topOnlySource() {
        return new SearchSourceBuilder().trackTotalHits(false);
    }

    /**
     * The measured defect. {@code matched_queries} is reported from the names registered while a query is converted, and the
     * Tail was the only thing that converted legs — so a Top-only request silently dropped a field classic hybrid always
     * returns. The leg forms are now carried for registration alone, which leaves the executed query Top-only.
     */
    public void testBuildFusedQuery_whenTopOnlyAndLegIsNamed_thenOnlyTheNamedLegIsCarriedWithoutTail() {
        List<QueryBuilder> legs = List.of(
            new MatchQueryBuilder("text", "hello").queryName("lexical"),
            new TermQueryBuilder("text", "place")
        );
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(topOnlySource(), ms, legs, minMaxArithmetic(), 10);

        assertEquals("registration must not turn a Top-only query into Top+Tail", 0, tailFilterCount(fused));
        // Only the named leg is carried. Carrying the unnamed one registers nothing, and it is not free: every carried leg
        // is converted on the shard, so an unnamed leg costs a toQuery a Top-only request would otherwise never pay.
        List<QueryBuilder> carried = namedOnlyLegsOf(fused);
        assertEquals("the unnamed leg has nothing to register", 1, carried.size());
        assertEquals("lexical", carried.get(0).queryName());
    }

    public void testBuildFusedQuery_whenTopOnlyAndNoLegIsNamed_thenNothingIsCarried() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(topOnlySource(), ms, legs, minMaxArithmetic(), 10);

        assertEquals("the common case pays nothing", 0, namedOnlyLegsOf(fused).size());
        assertEquals(0, tailFilterCount(fused));
    }

    /**
     * With the Tail present the legs are converted as a side effect of being executed, so a second copy on the wire would
     * register names the shard already has. The two lists are never both populated.
     */
    public void testBuildFusedQuery_whenTailIsBuiltAndLegIsNamed_thenTheTailCarriesTheName() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello").queryName("lexical"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(sourceWithAggregation(), ms, legs, minMaxArithmetic(), 10);

        assertEquals("the executed Tail registers its own names", 0, namedOnlyLegsOf(fused).size());
        assertEquals("lexical", tailOf(fused).should().get(0).queryName());
    }

    /**
     * A materialized leg answers to its own {@code _name}. The substitute is a fresh builder, so before this a named
     * kNN/neural leg lost {@code matched_queries} in <i>every</i> configuration — Tail or not. What it reports is the
     * documents the leg returned, the same bound materialization the match set already accepts.
     */
    public void testBuildFusedQuery_whenMaterializedLegIsNamed_thenTheSubstituteKeepsTheName() {
        List<QueryBuilder> legs = List.of(legNamed("knn").queryName("vector"));
        MultiSearchResponse ms = multiSearch(legItemFromIndex(INDEX, Map.of("2", 0.8f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(sourceWithAggregation(), ms, legs, minMaxArithmetic(), 10);

        QueryBuilder materialized = tailOf(fused).should().get(0);
        assertEquals("vector", materialized.queryName());
        assertAddressedTo(materialized, INDEX, "2");
    }

    /** Both halves of the fix at once: a Top-only request whose only leg is a named ANN leg. */
    public void testBuildFusedQuery_whenTopOnlyAndMaterializedLegIsNamed_thenTheCarriedFormKeepsTheName() {
        List<QueryBuilder> legs = List.of(legNamed("knn").queryName("vector"));
        MultiSearchResponse ms = multiSearch(legItemFromIndex(INDEX, Map.of("2", 0.8f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(topOnlySource(), ms, legs, minMaxArithmetic(), 10);

        assertEquals(0, tailFilterCount(fused));
        QueryBuilder carried = namedOnlyLegsOf(fused).get(0);
        assertEquals("vector", carried.queryName());
        assertAddressedTo(carried, INDEX, "2");
    }

    /**
     * Skipping the unnamed legs makes a carried leg's position in the list stop matching its position among the legs, so a
     * materialized substitute has to be built from <i>its own</i> leg's hits and not from the hits of whatever landed at the
     * same output index. Only a named leg behind an unnamed one can catch that.
     */
    public void testBuildFusedQuery_whenOnlyALaterLegIsNamed_thenTheSubstituteAddressesThatLegsOwnHits() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), legNamed("knn").queryName("vector"));
        MultiSearchResponse ms = multiSearch(
            legItemFromIndex(INDEX, Map.of("1", 0.9f)),
            legItemFromIndex(INDEX, Map.of("2", 0.8f, "3", 0.7f))
        );

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(topOnlySource(), ms, legs, minMaxArithmetic(), 10);

        List<QueryBuilder> carried = namedOnlyLegsOf(fused);
        assertEquals("only the second leg is named", 1, carried.size());
        assertEquals("vector", carried.get(0).queryName());
        assertAddressedTo(carried.get(0), INDEX, "2", "3");
    }

    /**
     * A {@code _name} nested inside a leg counts. Only {@code bool} overrides {@code visit(QueryBuilderVisitor)} in core, so
     * a visitor walk — like a shallow {@code queryName()} check — is blind to these shapes; the rendered form is what is
     * inspected instead.
     */
    public void testBuildFusedQuery_whenNameIsNestedInsideALeg_thenStillCarried() {
        QueryBuilder underConstantScore = new ConstantScoreQueryBuilder(new MatchQueryBuilder("text", "hello").queryName("inner"));
        QueryBuilder underNested = new org.opensearch.index.query.NestedQueryBuilder(
            "user",
            new MatchQueryBuilder("user.name", "alice").queryName("deep"),
            org.apache.lucene.search.join.ScoreMode.None
        );
        // bool is the one container core teaches to visit(), so it is the only shape a visitor walk would have found.
        QueryBuilder underBool = new BoolQueryBuilder().must(new MatchQueryBuilder("text", "hello").queryName("in_bool"));
        QueryBuilder underFunctionScore = new org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder(
            new MatchQueryBuilder("text", "hello").queryName("scored")
        );
        for (QueryBuilder leg : List.of(underConstantScore, underNested, underBool, underFunctionScore)) {
            List<QueryBuilder> legs = List.of(leg);
            MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)));

            QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(topOnlySource(), ms, legs, minMaxArithmetic(), 10);

            assertEquals("a name below the leg's own level must still be registered: " + leg, 1, namedOnlyLegsOf(fused).size());
            assertEquals("and the query stays Top-only", 0, tailFilterCount(fused));
        }
    }

    public void testBuildFusedQuery_whenALegIsWrappedButUnnamed_thenNotCarried() {
        // The counterpart of the test above: a wrapped leg carrying no name anywhere is not carried, so the rendered check
        // is not simply always-true.
        List<QueryBuilder> legs = List.of(new ConstantScoreQueryBuilder(new MatchQueryBuilder("text", "hello")));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(topOnlySource(), ms, legs, minMaxArithmetic(), 10);

        assertEquals(0, namedOnlyLegsOf(fused).size());
    }

    /**
     * The documented gap. A {@code _name} nested inside a <i>materializable</i> leg — on a {@code knn} filter, say — is
     * detected, so the leg is carried, but the substitute is an address of the returned hits and inherits only the leg's
     * own name: the shard never sees the leg's structure, so an inner name has nothing to be registered against. Registering
     * the original leg instead is what materialization exists to avoid (a second graph walk and, for {@code neural}, a second
     * inference call) for a reporting field. Pinned so the asymmetry is a decision on record rather than a surprise.
     */
    public void testBuildFusedQuery_whenNameIsNestedInsideAMaterializableLeg_thenTheSubstituteCarriesNoName() {
        QueryBuilder annLegWithNamedFilter = new ConstantScoreQueryBuilder(new MatchQueryBuilder("f", "v").queryName("filter_name")) {
            @Override
            public String getWriteableName() {
                return "knn";
            }
        };
        MultiSearchResponse ms = multiSearch(legItemFromIndex(INDEX, Map.of("2", 0.8f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(
            topOnlySource(),
            ms,
            List.of(annLegWithNamedFilter),
            minMaxArithmetic(),
            10
        );

        QueryBuilder carried = namedOnlyLegsOf(fused).get(0);
        assertAddressedTo(carried, INDEX, "2");
        assertNull("materialization cannot carry a name from inside the leg it replaced", carried.queryName());
    }

    private int tailFilterCount(QueryBuilder fused) {
        return ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery().filter().size();
    }

    // ---- fused scores are floored above the non-scoring Tail ----

    private FusionSpec l2Arithmetic() {
        return new FusionSpec(
            FusionSpec.Shape.NORMALIZATION_PROCESSOR,
            FusionSpec.TECHNIQUE_ARITHMETIC_MEAN,
            "l2",
            FusionSpec.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
    }

    private FusionSpec zScoreArithmetic() {
        return new FusionSpec(
            FusionSpec.Shape.NORMALIZATION_PROCESSOR,
            FusionSpec.TECHNIQUE_ARITHMETIC_MEAN,
            "z_score",
            FusionSpec.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
    }

    private FusionSpec minMaxWeighted(float... weights) {
        return new FusionSpec(
            FusionSpec.Shape.NORMALIZATION_PROCESSOR,
            FusionSpec.TECHNIQUE_ARITHMETIC_MEAN,
            FusionSpec.NORMALIZATION_MIN_MAX,
            FusionSpec.DEFAULT_RANK_CONSTANT,
            weights
        );
    }

    private ConstantScoreQueryBuilder topClause(QueryBuilder fused, int position) {
        return (ConstantScoreQueryBuilder) ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery().should().get(position);
    }

    /**
     * The tie this closes. The Top scores and the Tail does not, which is the whole mechanism separating the fused window
     * from everything else; a ranked document at exactly {@code 0.0} ties with the Tail-only documents it is meant to
     * outrank, and Lucene then breaks that tie by ascending doc id — so a document fusion did not rank can be returned
     * ahead of one it did, and with {@code size == window_size} the ranked one is dropped outright.
     *
     * <p>{@code l2} is one of the two ways to reach exactly {@code 0.0}: a leg whose raw scores are all {@code 0.0} has a
     * zero norm, and {@code L2ScoreNormalizer.MIN_SCORE} is {@code 0.0f} — unlike min_max's and z_score's {@code 0.001f}.
     * A document appearing only in such a leg therefore fuses to {@code 0.0} under default weights.
     */
    public void testBuildFusedQuery_whenL2LegHasZeroNorm_thenRankedScoreIsFlooredAboveTheTail() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        // Leg 0's only hit scores 0.0 → zero norm → normalizes to L2's MIN_SCORE of 0.0f. Doc 3 is in no other leg.
        MultiSearchResponse ms = multiSearch(legItem(Map.of("3", 0.0f)), legItem(Map.of("1", 5.0f, "2", 3.0f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(new SearchSourceBuilder(), ms, legs, l2Arithmetic(), 10);

        BoolQueryBuilder self = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery();
        assertEquals("union of {1,2,3} ranked", 3, self.should().size());
        assertAddressedTo(((ConstantScoreQueryBuilder) self.should().get(2)).innerQuery(), INDEX, "3");
        assertEquals(
            "a fused score of exactly 0.0 is floored, so it still outranks the Tail",
            HybridFusionOrchestrator.MIN_RANKED_SCORE,
            topClause(fused, 2).boost(),
            0.0f
        );
        assertTrue("every ranked document outscores a Tail-only document", topClause(fused, 0).boost() > 0.0f);
    }

    /**
     * What the floor's <i>value</i> has to satisfy, and the reason it is not {@link Float#MIN_VALUE}: the score does not
     * reach Lucene's comparison untouched. An enclosing clause's {@code boost}, a rescore's {@code query_weight} (core's
     * {@code QueryRescorer} multiplies every window document's first-pass score by it, matched or not) and a
     * {@code score_mode: multiply} rescore all attenuate it first, and {@code Float.MIN_VALUE} is subnormal — any factor at
     * or below {@code 0.5} rounds it back to exactly {@code 0.0} and restores the tie the floor exists to break.
     *
     * <p>Asserted on the constant rather than through a query because that is where the requirement lives: the arithmetic
     * below is core's and Lucene's, and this is the only place the plugin gets to choose a value that survives it.
     */
    public void testMinRankedScore_survivesTheAttenuationAFusedScoreMeetsDownstream() {
        assertEquals(
            "subnormal, so a factor of 0.5 annihilates it — the trap this constant exists to avoid",
            0.0f,
            Float.MIN_VALUE * 0.5f,
            0.0f
        );

        for (float factor : new float[] { 0.5f, 0.1f, 0.001f, 1e-6f, 1e-12f }) {
            assertTrue(
                "the floor must stay above the Tail's 0.0 after being multiplied by " + factor,
                HybridFusionOrchestrator.MIN_RANKED_SCORE * factor > 0.0f
            );
        }
        assertTrue(
            "and it must stay far below the smallest score a real config produces — min_max floors a normalized score at "
                + "0.001 and arithmetic_mean divides by a weight sum of 1.0",
            HybridFusionOrchestrator.MIN_RANKED_SCORE < 0.001f * 1e-9f
        );
    }

    /**
     * The second route to exactly {@code 0.0}, and the one that works with any normalization technique: a {@code weights}
     * entry of {@code 0.0} zeroes its leg's contribution, so a document that matched only that leg fuses to {@code 0.0}.
     * Two such documents also pin down what the floor must NOT do: they were tied at {@code 0.0} before it and stay tied
     * after, in the same key order — flooring is not allowed to invent an order fusion did not produce, only to lift the
     * whole tie above the Tail.
     */
    public void testBuildFusedQuery_whenLegWeightIsZero_thenAllZeroScoresAreFlooredAndKeepTheirOrder() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 5.0f, "2", 3.0f)), legItem(Map.of("3", 7.0f, "4", 2.0f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(new SearchSourceBuilder(), ms, legs, minMaxWeighted(1.0f, 0.0f), 10);

        BoolQueryBuilder self = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery();
        assertEquals(4, self.should().size());
        assertEquals("leg 0 at weight 1.0 still ranks normally", 1.0f, topClause(fused, 0).boost(), 0.001f);
        // Docs 3 and 4 matched only the zero-weighted leg, so both fused to exactly 0.0 and were tied on the composite key.
        assertAddressedTo(((ConstantScoreQueryBuilder) self.should().get(2)).innerQuery(), INDEX, "3");
        assertAddressedTo(((ConstantScoreQueryBuilder) self.should().get(3)).innerQuery(), INDEX, "4");
        assertEquals(HybridFusionOrchestrator.MIN_RANKED_SCORE, topClause(fused, 2).boost(), 0.0f);
        assertEquals(HybridFusionOrchestrator.MIN_RANKED_SCORE, topClause(fused, 3).boost(), 0.0f);
    }

    /**
     * The combination node has to report the score fusion computed, not the floored score round 2 ranks by. Same input as
     * the test above, run explained: documents 3 and 4 matched only the zero-weighted leg, so fusion produced exactly
     * {@code 0.0} for them while their Top clause carries {@link HybridFusionOrchestrator#MIN_RANKED_SCORE}. Recording the
     * floored value instead would label the combination node {@code 1e-30} over children that combine to {@code 0.0}, and
     * because the hit's score is that same {@code 1e-30} the wrapper naming the floor would be suppressed
     * ({@code FusedDocExplanations#explain} returns the combination node bare when the two agree) — so the tree would
     * claim a number its own children do not produce, with nothing pointing at the floor.
     *
     * <p>The single rendered child is the caveat the fix does not remove: the zero-weighted leg's slot still counted its
     * weight into the combiner's divisor while rendering no node, so the parent is deliberately not the mean of what is
     * shown. Classic renders a partially-matched document the same way.
     */
    public void testBuildFusedQuery_whenAZeroFusedScoreIsFloored_thenTheCombinationNodeKeepsTheComputedScore() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(
            explainedLegItem(new LinkedHashMap<>(Map.of("1", 5.0f, "2", 3.0f))),
            explainedLegItem(new LinkedHashMap<>(Map.of("3", 7.0f, "4", 2.0f)))
        );
        FusedDocExplanations explanations = new FusedDocExplanations();

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(
            new SearchSourceBuilder().explain(true),
            ms,
            legs,
            minMaxWeighted(1.0f, 0.0f),
            10,
            new FusedCoordinatorTimings(),
            explanations
        );

        // Round 2 ranks document 3 by the floored score: that is what its Top clause boost carries.
        assertEquals(HybridFusionOrchestrator.MIN_RANKED_SCORE, fusedScoresById(fused).get("3"), 0.0f);

        Explanation tree = explanations.explain(FusedDocExplanations.documentKey(INDEX, "3"), HybridFusionOrchestrator.MIN_RANKED_SCORE);
        assertNotNull("the document is in the window, so it is described", tree);
        assertEquals(
            "the floor moved the score, so the top node names what round 2 returned rather than relabelling the combination",
            "score of the fused hybrid query as round 2 returned it, computed from:",
            tree.getDescription()
        );
        assertEquals(HybridFusionOrchestrator.MIN_RANKED_SCORE, tree.getValue().floatValue(), 0.0f);

        assertEquals("the combination is its only child", 1, tree.getDetails().length);
        Explanation combination = tree.getDetails()[0];
        assertEquals(explanations.combinationDescription(), combination.getDescription());
        assertEquals("the combination node reports what fusion computed, not the floor", 0.0f, combination.getValue().floatValue(), 0.0f);
        assertEquals("only the leg that matched is rendered", 1, combination.getDetails().length);
    }

    /**
     * Both branches of {@code scoreAboveTail}, called directly, because fused mode reaches only one of them through
     * {@link HybridFusionOrchestrator#buildFusedQuery}. A <i>non-finite</i> score is floored, not refused: z_score returns
     * a raw {@code +Infinity} leg score unchanged through its equal-to-mean edge case (the integration control below
     * measures it end to end), so a fused {@code +Infinity} is reachable and is floored to {@code MIN_RANKED_SCORE} exactly
     * as {@code 0.0} is — a degenerate score ranks a document last rather than failing an otherwise legal request. A
     * {@code NaN} is floored the same way, defensively; nothing in scope produces a fused {@code NaN}, since min_max's and
     * l2's {@code Inf/Inf} is dropped by arithmetic_mean before it can combine.
     *
     * <p>Only a <i>negative</i> score is refused, and with an {@code IllegalStateException} rather than an
     * {@code IllegalArgumentException} because no change to the request fixes it: a negative fused score would mean the
     * non-negativity invariant broke, and answering with a coordinator-invented order is worse than failing. {@code -0.0f}
     * sits on the flooring side, not the refusing side — {@code [-0.0f < 0.0f]} is {@code false} — which is the one place
     * this deliberately disagrees with {@code HybridFusionQueryBuilder#requireUsableAsBoosts}, which rejects it. Asserted so
     * the two stay differently by intention rather than by accident.
     */
    public void testScoreAboveTail_floorsNonFiniteAndRefusesOnlyNegative() {
        for (float floored : new float[] { Float.POSITIVE_INFINITY, Float.NaN, 0.0f, -0.0f }) {
            assertEquals(
                "a non-finite or zero fused score of [" + floored + "] is floored to the window bottom, not refused",
                HybridFusionOrchestrator.MIN_RANKED_SCORE,
                HybridFusionOrchestrator.scoreAboveTail(floored),
                0.0f
            );
        }

        for (float refused : new float[] { Float.NEGATIVE_INFINITY, -1.0f, -Float.MIN_VALUE }) {
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                "expected a negative fused score of [" + refused + "] to be refused",
                () -> HybridFusionOrchestrator.scoreAboveTail(refused)
            );
            assertTrue(e.getMessage(), e.getMessage().contains("a fused score must be non-negative"));
        }

        assertEquals(
            "anything already above the floor passes through untouched",
            0.25f,
            HybridFusionOrchestrator.scoreAboveTail(0.25f),
            0.0f
        );
    }

    /**
     * min_max's leg of the non-finite story, and the reason the floor no longer refuses. The same raw {@code +Infinity}
     * that z_score carries through to a fused {@code +Infinity} (the test above), min_max launders to {@code 0.0}. So the
     * three in-scope normalizers do not agree on the intermediate value — they agree only on the floored result, which is
     * what the fix restored. Measured rather than assumed, because the laundering is a claim about shared scalar arithmetic
     * and would rot silently.
     *
     * <p>The two non-finite inputs are laundered differently under min_max, which is why each is asserted rather than looped
     * over. {@code NaN} never reaches the combiner: {@code Floats.compare(NaN, NaN) == 0}, so a single-hit leg whose min,
     * max and score are all {@code NaN} matches min_max's single-score edge case and normalizes to {@code 1.0} — the top of
     * its own leg. {@code +Infinity} does make min_max emit {@code NaN}, since {@code (Inf - min) / (Inf - min)} is
     * {@code Inf/Inf}, but arithmetic_mean's {@code score >= 0.0} participation rule is false for {@code NaN}, so that leg's
     * slot leaves both numerator and denominator and the document fuses to exactly {@code 0.0} — floored, not refused.
     * z_score is the one normalizer that does not launder: its equal-to-mean edge case returns the leg {@code maxScore}, so
     * a {@code +Infinity} hit stays {@code +Infinity} and reaches the floor as such.
     *
     * <p>That laundering is not something this path could fix — it is in the scalar math classic hybrid shares, and
     * changing it would change classic's scores too. It is recorded here because it is what made the refusal look
     * unreachable when only min_max was measured.
     */
    public void testBuildFusedQuery_whenALegHitScoreIsNonFinite_thenLaunderedByFusionRatherThanRefused() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));

        MultiSearchResponse withNaN = multiSearch(legItem(Map.of("1", Float.NaN)), legItem(Map.of("2", 3.0f)));
        QueryBuilder fusedNaN = HybridFusionOrchestrator.buildFusedQuery(new SearchSourceBuilder(), withNaN, legs, minMaxArithmetic(), 10);
        assertEquals("both documents are ranked", 2, ((HybridFusionQueryBuilder) fusedNaN).buildSelfErasedQuery().should().size());
        // Each doc matched one leg at a normalized 1.0 and contributed 0.0 to the other, so both fuse to 0.5 and tie —
        // the NaN hit is ranked exactly as a legitimate best hit of its leg would be.
        assertEquals("the NaN hit ranks as its leg's best", 0.5f, topClause(fusedNaN, 0).boost(), 0.0f);
        assertEquals(0.5f, topClause(fusedNaN, 1).boost(), 0.0f);

        MultiSearchResponse withInfinity = multiSearch(legItem(Map.of("1", Float.POSITIVE_INFINITY)), legItem(Map.of("2", 3.0f)));
        QueryBuilder fusedInfinity = HybridFusionOrchestrator.buildFusedQuery(
            new SearchSourceBuilder(),
            withInfinity,
            legs,
            minMaxArithmetic(),
            10
        );
        assertEquals(2, ((HybridFusionQueryBuilder) fusedInfinity).buildSelfErasedQuery().should().size());
        assertAddressedTo(topClause(fusedInfinity, 0).innerQuery(), INDEX, "2");
        assertEquals("the leg that scored finitely is unaffected", 0.5f, topClause(fusedInfinity, 0).boost(), 0.0f);
        assertAddressedTo(topClause(fusedInfinity, 1).innerQuery(), INDEX, "1");
        assertEquals(
            "and the +Infinity hit fused to exactly 0.0, so it was floored above the Tail rather than refused",
            HybridFusionOrchestrator.MIN_RANKED_SCORE,
            topClause(fusedInfinity, 1).boost(),
            0.0f
        );
    }

    /**
     * The z_score twin of the min_max non-finite case above, and the reason the floor no longer refuses a non-finite
     * score. min_max launders a raw {@code +Infinity} leg hit to {@code 0.0} (Inf/Inf → NaN, dropped by arithmetic_mean),
     * but z_score's equal-to-mean edge case returns the leg {@code maxScore} unchanged, so the same hit normalizes to
     * {@code +Infinity} and arithmetic_mean keeps it — a fused {@code +Infinity}. Before the floor was widened this reached
     * {@link HybridFusionOrchestrator#scoreAboveTail} as {@code +Infinity} and failed the request with an
     * {@link IllegalStateException} (a server error) for an in-scope config; now the document is floored above the Tail and
     * the request succeeds, matching min_max for the identical input.
     */
    public void testBuildFusedQuery_whenALegHitScoreIsNonFiniteUnderZScore_thenFlooredRatherThanRefused() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", Float.POSITIVE_INFINITY)), legItem(Map.of("2", 3.0f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(new SearchSourceBuilder(), ms, legs, zScoreArithmetic(), 10);

        BoolQueryBuilder self = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery();
        assertEquals("both documents are ranked, neither refused", 2, self.should().size());
        // Doc 1's +Infinity sorted it to the top of the window before the floor ran, so it is should()-clause 0 — but its
        // score is floored to the window bottom, so at query time doc 2's finite 1.5 outscores it. The floor lifts the
        // score above the Tail's 0.0 without inventing an order: the effective ranking (doc 2 over doc 1) is what min_max
        // produces for the same input, only reached from a +Infinity fused score rather than a laundered 0.0.
        assertAddressedTo(topClause(fused, 0).innerQuery(), INDEX, "1");
        assertAddressedTo(topClause(fused, 1).innerQuery(), INDEX, "2");
        assertEquals(
            "the +Infinity hit fused to +Infinity under z_score and was floored above the Tail rather than refused",
            HybridFusionOrchestrator.MIN_RANKED_SCORE,
            topClause(fused, 0).boost(),
            0.0f
        );
        assertEquals("the finitely-scored document keeps its real fused score", 1.5f, topClause(fused, 1).boost(), 0.001f);
        assertTrue(
            "so at query time the finite document outscores the floored +Infinity one",
            topClause(fused, 1).boost() > topClause(fused, 0).boost()
        );
    }

    /**
     * The l2 control for the same input, and the correction to a natural but wrong assumption: l2 does <i>not</i> reach the
     * non-finite floor. A leg holding a {@code +Infinity} hit has an {@code +Infinity} L2 norm (the sum of squares
     * overflows), so {@code +Infinity / +Infinity} is {@code NaN} — which arithmetic_mean drops, exactly as it drops
     * min_max's {@code NaN}. So of the three in-scope normalizers only z_score propagates a non-finite score; l2 and
     * min_max both launder it to {@code 0.0}. Pinned so a future change to l2's norm handling that let {@code +Infinity}
     * through would surface here rather than as a server error in production.
     */
    public void testBuildFusedQuery_whenALegHitScoreIsNonFiniteUnderL2_thenLaunderedLikeMinMax() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", Float.POSITIVE_INFINITY)), legItem(Map.of("2", 3.0f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(new SearchSourceBuilder(), ms, legs, l2Arithmetic(), 10);

        BoolQueryBuilder self = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery();
        assertEquals("both documents are ranked, neither refused", 2, self.should().size());
        assertAddressedTo(topClause(fused, 1).innerQuery(), INDEX, "1");
        assertEquals(
            "the +Infinity hit was laundered to 0.0 by l2 and floored above the Tail, exactly as under min_max",
            HybridFusionOrchestrator.MIN_RANKED_SCORE,
            topClause(fused, 1).boost(),
            0.0f
        );
    }

    /**
     * The one property of {@code MIN_RANKED_SCORE} that reads like a guarantee and is not: its lower bound is per
     * multiplication and does not compose. Attenuation is applied a factor at a time — Lucene for an enclosing clause's
     * {@code boost}, core's {@code QueryRescorer} once per rescorer in the chain — and each step rounds to float32, so the
     * factors multiply. Every factor used below is individually far inside the bound that
     * {@link #testMinRankedScore_survivesTheAttenuationAFusedScoreMeetsDownstream} asserts; chained, they annihilate the
     * floor and restore the Tail tie it exists to break.
     *
     * <p>Pinned rather than fixed, and the constant's javadoc carries the reasoning: no float32 value survives arbitrary
     * multiplication, raising this one only trades tolerance below for headroom above, and the attenuating values are legal
     * core parameters. What this test protects is the honesty of the bound — change the constant and it reports the new one.
     */
    public void testMinRankedScore_attenuationBoundIsPerFactorAndDoesNotCompose() {
        assertTrue("a single factor at the documented bound survives", HybridFusionOrchestrator.MIN_RANKED_SCORE * 7.0065e-16f > 0.0f);
        assertEquals(
            "and just below it the product rounds to zero rather than to the smallest subnormal",
            0.0f,
            HybridFusionOrchestrator.MIN_RANKED_SCORE * 7.006e-16f,
            0.0f
        );

        // Three rescorers at query_weight 1e-6 — a factor twelve orders of magnitude inside the per-factor bound.
        float chained = HybridFusionOrchestrator.MIN_RANKED_SCORE;
        for (int rescorer = 0; rescorer < 3; rescorer++) {
            assertTrue("each factor on its own leaves the floor positive", HybridFusionOrchestrator.MIN_RANKED_SCORE * 1e-6f > 0.0f);
            chained = chained * 1e-6f;
        }
        assertEquals("but three of them in sequence annihilate it", 0.0f, chained, 0.0f);

        // And the boundary for the mildest factor that still composes to zero, which is where the count matters.
        float compounded = HybridFusionOrchestrator.MIN_RANKED_SCORE;
        for (int rescorer = 0; rescorer < 5; rescorer++) {
            compounded = compounded * 0.001f;
        }
        assertTrue("five rescorers at query_weight 0.001 still hold", compounded > 0.0f);
        assertEquals("six do not", 0.0f, compounded * 0.001f, 0.0f);
    }

    // ---- rrf dispatch: rank scores, not min_max normalization, and the resolved rank_constant is honored ----

    /** The scoring should-clause boosts in Top order — i.e. the fused scores, highest first. */
    private float[] topScores(QueryBuilder fused) {
        List<QueryBuilder> should = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery().should();
        float[] scores = new float[should.size()];
        for (int i = 0; i < should.size(); i++) {
            scores[i] = should.get(i).boost();
        }
        return scores;
    }

    public void testBuildFusedQuery_rrf_fusesRankScores() {
        // leg0 ranks 1 > 2; leg1 ranks 2 > 3. RRF sums rank scores, so doc 2 (rank 1 in leg0 + rank 0 in leg1) tops the
        // window, then doc 1 (rank 0, one leg) then doc 3 (rank 1, one leg). Scores must be the rank arithmetic, NOT the
        // min_max normalization of the raw scores — this is what proves the lookup resolved RrfScalarNormalizer.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f, "2", 0.5f)), legItem(Map.of("2", 0.8f, "3", 0.4f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(
            new SearchSourceBuilder().trackTotalHits(false),
            ms,
            legs,
            rrf(FusionSpec.DEFAULT_RANK_CONSTANT),
            10
        );

        float rank0 = RRFScoreNormalizer.scoreForRank(0, FusionSpec.DEFAULT_RANK_CONSTANT);
        float rank1 = RRFScoreNormalizer.scoreForRank(1, FusionSpec.DEFAULT_RANK_CONSTANT);
        assertArrayEquals("doc2 (both legs), then doc1, then doc3", new float[] { rank1 + rank0, rank0, rank1 }, topScores(fused), 0.0f);
    }

    public void testBuildFusedQuery_rrf_honorsRankConstant() {
        // The rank constant is read from the FusionSpec rather than defaulted: the same single-leg hit set fuses to
        // 1/(k+1) and 1/(k+2) for whichever k was configured.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"));
        for (int rankConstant : new int[] { 1, 10_000 }) {
            MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f, "2", 0.5f)));
            QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(
                new SearchSourceBuilder().trackTotalHits(false),
                ms,
                legs,
                rrf(rankConstant),
                10
            );
            assertArrayEquals(
                "rank_constant " + rankConstant + " drives the rank scores",
                new float[] { RRFScoreNormalizer.scoreForRank(0, rankConstant), RRFScoreNormalizer.scoreForRank(1, rankConstant) },
                topScores(fused),
                0.0f
            );
        }
    }

    public void testBuildFusedQuery_rrf_ignoresRawScoreMagnitude() {
        // Two legs whose raw scores are orders of magnitude apart fuse identically to two legs with comparable scores,
        // because RRF reads rank only. Under min_max the per-leg normalization would differ.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        float[] comparable = topScores(
            HybridFusionOrchestrator.buildFusedQuery(
                new SearchSourceBuilder().trackTotalHits(false),
                multiSearch(legItem(Map.of("1", 0.9f, "2", 0.5f)), legItem(Map.of("1", 0.8f, "2", 0.4f))),
                legs,
                rrf(FusionSpec.DEFAULT_RANK_CONSTANT),
                10
            )
        );
        float[] skewed = topScores(
            HybridFusionOrchestrator.buildFusedQuery(
                new SearchSourceBuilder().trackTotalHits(false),
                multiSearch(legItem(Map.of("1", 900.0f, "2", 0.5f)), legItem(Map.of("1", 0.008f, "2", 0.004f))),
                legs,
                rrf(FusionSpec.DEFAULT_RANK_CONSTANT),
                10
            )
        );
        assertArrayEquals("rank-only fusion is invariant to raw score magnitude", comparable, skewed, 0.0f);
    }
}
