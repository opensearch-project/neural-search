/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.List;
import java.util.Map;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.test.OpenSearchTestCase;

public class HybridFusionOrchestratorTests extends OpenSearchTestCase {

    private static final String INDEX = "test-index";

    private FusionSpec minMaxArithmetic() {
        return new FusionSpec(
            FusionSpec.TECHNIQUE_ARITHMETIC_MEAN,
            FusionSpec.NORMALIZATION_MIN_MAX,
            FusionSpec.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
    }

    /** One MultiSearch item wrapping a SearchResponse whose hits carry the given (_id -> score) pairs. */
    private MultiSearchResponse.Item legItem(Map<String, Float> idToScore) {
        SearchHit[] hits = new SearchHit[idToScore.size()];
        int i = 0;
        for (Map.Entry<String, Float> e : idToScore.entrySet()) {
            SearchHit hit = new SearchHit(i, e.getKey(), Map.of(), Map.of());
            hit.score(e.getValue());
            hits[i++] = hit;
        }
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponseSections sections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        SearchResponse response = new SearchResponse(sections, null, 1, 1, 0, 10, null, null);
        return new MultiSearchResponse.Item(response, null);
    }

    private MultiSearchResponse.Item failedItem() {
        return new MultiSearchResponse.Item(null, new RuntimeException("leg boom"));
    }

    private MultiSearchResponse multiSearch(MultiSearchResponse.Item... items) {
        return new MultiSearchResponse(items, 10L);
    }

    // ---- buildLegMultiSearch ----

    public void testBuildLegMultiSearch_perLegSourceShape() {
        SearchRequest request = new SearchRequest(INDEX);
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));

        MultiSearchRequest ms = HybridFusionOrchestrator.buildLegMultiSearch(request, legs, 50);

        assertEquals(2, ms.requests().size());
        for (SearchRequest leg : ms.requests()) {
            SearchSourceBuilder source = leg.source();
            assertEquals(50, source.size());
            assertEquals(0, source.from());
            assertFalse(source.fetchSource().fetchSource());
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

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10, true);

        assertTrue(fused instanceof HybridFusionQuery);
        BoolQueryBuilder self = ((HybridFusionQuery) fused).buildSelfErasedQuery();
        assertEquals("union of {1,2,3} scored in Top", 3, self.should().size());
        assertEquals("aggs → Tail retained", 1, self.filter().size());
    }

    public void testBuildFusedQuery_topLevelPlainTopK_topOnly() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f)));
        // track_total_hits:false, no aggs/highlight/explain → plain top-K, Tail dropped.
        SearchSourceBuilder source = new SearchSourceBuilder().trackTotalHits(false);

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10, true);

        BoolQueryBuilder self = ((HybridFusionQuery) fused).buildSelfErasedQuery();
        assertEquals(2, self.should().size());
        assertEquals("plain top-K → no Tail", 0, self.filter().size());
    }

    public void testBuildFusedQuery_nested_alwaysTopOnly() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f)));
        // Even with aggs present, a nested (topLevel=false) fused query is Top-only.
        SearchSourceBuilder source = new SearchSourceBuilder().aggregation(
            org.opensearch.search.aggregations.AggregationBuilders.terms("t").field("f")
        );

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10, false);

        BoolQueryBuilder self = ((HybridFusionQuery) fused).buildSelfErasedQuery();
        assertEquals("nested → no Tail regardless of aggs", 0, self.filter().size());
    }

    public void testBuildFusedQuery_emptyResult_matchNone() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of()));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(new SearchSourceBuilder(), ms, legs, minMaxArithmetic(), 10, true);

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
            2,
            true
        );

        BoolQueryBuilder self = ((HybridFusionQuery) fused).buildSelfErasedQuery();
        assertEquals("window=2 caps the Top to 2 docs", 2, self.should().size());
    }

    // ---- graceful leg failure ----

    public void testBuildFusedQuery_oneLegFailed_survivesOnRemaining() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f, "2", 0.5f)), failedItem());

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(
            new SearchSourceBuilder().trackTotalHits(false),
            ms,
            legs,
            minMaxArithmetic(),
            10,
            true
        );

        BoolQueryBuilder self = ((HybridFusionQuery) fused).buildSelfErasedQuery();
        assertEquals("fuses over the surviving leg", 2, self.should().size());
    }

    public void testBuildFusedQuery_allLegsFailed_throws() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(failedItem(), failedItem());

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> HybridFusionOrchestrator.buildFusedQuery(new SearchSourceBuilder(), ms, legs, minMaxArithmetic(), 10, true)
        );
        assertTrue(e.getMessage().contains("all fused-mode sub-queries failed"));
    }

    // ---- knn/neural leg materialized as Ids in the Tail (no second ANN walk) ----

    public void testBuildFusedQuery_knnLeg_materializedAsIdsInTail() {
        // A leg whose writeable name is a materializable one ("knn") — its Lucene match set IS its returned top-k, so
        // survivingLegQueries rewrites it to an IdsQuery in the Tail rather than re-walking the ANN graph. Using a
        // minimal MatchQuery wrapper reporting name "knn" keeps the test off KNN-internal construction/validation.
        QueryBuilder knnLeg = new MatchQueryBuilder("vec", "q") {
            @Override
            public String getWriteableName() {
                return "knn";
            }
        };
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), knnLeg);
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f, "3", 0.7f)));
        // aggregation forces Tail retention so we can inspect leg materialization.
        SearchSourceBuilder source = new SearchSourceBuilder().aggregation(
            org.opensearch.search.aggregations.AggregationBuilders.terms("t").field("f")
        );

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10, true);

        BoolQueryBuilder self = ((HybridFusionQuery) fused).buildSelfErasedQuery();
        BoolQueryBuilder tail = (BoolQueryBuilder) self.filter().get(0);
        assertEquals(2, tail.should().size());
        // lexical leg stays a real query; knn/neural leg is materialized as an IdsQuery of its returned hits.
        long idsClauses = tail.should().stream().filter(q -> q instanceof IdsQueryBuilder).count();
        assertEquals("knn leg materialized as IdsQuery", 1, idsClauses);
    }

    // ---- weighted combination + explain/highlight tail triggers ----

    public void testBuildFusedQuery_withPerLegWeights_fusesWithoutError() {
        // Weighted arithmetic mean: exercises weightsParams() building the combination technique from FusionSpec weights.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f, "2", 0.5f)), legItem(Map.of("2", 0.8f, "3", 0.4f)));
        FusionSpec weighted = new FusionSpec(
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
            10,
            true
        );

        assertTrue(fused instanceof HybridFusionQuery);
        assertEquals(3, ((HybridFusionQuery) fused).buildSelfErasedQuery().should().size());
    }

    public void testBuildFusedQuery_explainTriggersTail() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)));
        SearchSourceBuilder source = new SearchSourceBuilder().trackTotalHits(false).explain(true);

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10, true);

        assertEquals("explain → Tail retained", 1, ((HybridFusionQuery) fused).buildSelfErasedQuery().filter().size());
    }

    public void testBuildFusedQuery_defaultTrackTotalHits_keepsTailForAccurateCount() {
        // No aggs/explain and track_total_hits left at default → wantsTotalsBeyondWindow keeps the Tail for the count.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f, "2", 0.5f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(new SearchSourceBuilder(), ms, legs, minMaxArithmetic(), 10, true);

        assertEquals("default totals → Tail retained", 1, ((HybridFusionQuery) fused).buildSelfErasedQuery().filter().size());
    }

    public void testBuildFusedQuery_nullSource_keepsTail() {
        // A null source (defensive) is treated as "wants totals" → Tail retained.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(null, ms, legs, minMaxArithmetic(), 10, true);

        assertEquals(1, ((HybridFusionQuery) fused).buildSelfErasedQuery().filter().size());
    }

    public void testBuildFusedQuery_neuralNamedLeg_materializedAsIds() {
        // "neural" is also a materializable name → its leg is materialized to IdsQuery in the Tail (not re-walked).
        QueryBuilder neuralLeg = new MatchQueryBuilder("vec", "q") {
            @Override
            public String getWriteableName() {
                return "neural";
            }
        };
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), neuralLeg);
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f)));
        SearchSourceBuilder source = new SearchSourceBuilder().aggregation(
            org.opensearch.search.aggregations.AggregationBuilders.terms("t").field("f")
        );

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10, true);

        BoolQueryBuilder tail = (BoolQueryBuilder) ((HybridFusionQuery) fused).buildSelfErasedQuery().filter().get(0);
        long idsClauses = tail.should().stream().filter(q -> q instanceof IdsQueryBuilder).count();
        assertEquals("neural leg materialized as IdsQuery", 1, idsClauses);
    }

    public void testBuildFusedQuery_failedLegExcludedFromTail() {
        // A failed leg (null hits slot) is dropped from the Tail — only the surviving leg's real query remains.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f, "2", 0.5f)), failedItem());
        SearchSourceBuilder source = new SearchSourceBuilder().aggregation(
            org.opensearch.search.aggregations.AggregationBuilders.terms("t").field("f")
        );

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10, true);

        BoolQueryBuilder tail = (BoolQueryBuilder) ((HybridFusionQuery) fused).buildSelfErasedQuery().filter().get(0);
        assertEquals("only the surviving leg is in the Tail", 1, tail.should().size());
    }
}
