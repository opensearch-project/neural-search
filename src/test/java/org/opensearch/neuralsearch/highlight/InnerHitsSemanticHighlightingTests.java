/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.neuralsearch.highlight.batch.HighlightContext;
import org.opensearch.neuralsearch.highlight.batch.config.HighlightConfig;
import org.opensearch.neuralsearch.highlight.batch.config.HighlightContextBuilder;
import org.opensearch.neuralsearch.highlight.utils.HighlightConfigBuilder;
import org.opensearch.neuralsearch.highlight.utils.InnerHitsHighlightLocator;
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for batch semantic highlighting on nested fields retrieved through
 * {@code inner_hits}.
 */
public class InnerHitsSemanticHighlightingTests extends OpenSearchTestCase {

    private static final String MODEL_ID = "test-model";
    private static final String NESTED_PATH = "chunks";
    private static final String SEMANTIC_FIELD = "chunks.text";
    private static final String QUERY_TEXT = "company earnings";

    /**
     * HighlightConfigBuilder must discover a semantic highlight attached to inner_hits
     * when no top-level highlighter is present
     */
    public void testConfigBuilderDiscoversHighlightOnInnerHits() {
        SearchRequest request = buildInnerHitsRequest(NESTED_PATH, SEMANTIC_FIELD);
        SearchResponse response = buildResponseWithInnerHits(List.of(List.of("Apple revenue hit 90 billion dollars this quarter.")));

        HighlightConfig config = HighlightConfigBuilder.buildFromSearchRequest(request, response);

        assertNull("config should not have a validation error", config.getValidationError());
        assertNull("top-level field must be null when only inner_hits declare semantic", config.getFieldName());
        assertEquals(MODEL_ID, config.getModelId());
        assertEquals(QUERY_TEXT, config.getQueryText());
        assertTrue("batch_inference option should be carried over", config.isBatchInference());
        assertTrue("config must carry inner_hits targets", config.hasInnerHitsTargets());

        assertEquals(1, config.getInnerHitsTargets().size());
        HighlightConfig.InnerHitsTarget target = config.getInnerHitsTargets().get(0);
        assertEquals(NESTED_PATH, target.getInnerHitName());
        assertEquals(NESTED_PATH, target.getNestedPath());
        assertEquals(SEMANTIC_FIELD, target.getFieldName());
    }

    /**
     * Multiple nested clauses under a bool query should each contribute a target so
     * all of them receive semantic highlighting.
     */
    public void testConfigBuilderCollectsAllInnerHitsTargetsFromBoolQuery() {
        NestedQueryBuilder reviewsNested = buildNestedWithSemanticInnerHits("reviews", "reviews.text");
        NestedQueryBuilder qaNested = buildNestedWithSemanticInnerHits("qa", "qa.answer");

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().must(reviewsNested).must(qaNested);

        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder().query(boolQuery));

        List<InnerHitsHighlightLocator.Location> locations = InnerHitsHighlightLocator.findAll(boolQuery);
        assertEquals("both nested inner_hits highlights should be discovered", 2, locations.size());

        HighlightConfig config = HighlightConfigBuilder.buildFromSearchRequest(request, buildEmptyResponseWithOneHit());
        assertTrue(config.hasInnerHitsTargets());
        assertNull(config.getFieldName());
        assertEquals(2, config.getInnerHitsTargets().size());
        assertEquals("reviews", config.getInnerHitsTargets().get(0).getInnerHitName());
        assertEquals("reviews.text", config.getInnerHitsTargets().get(0).getFieldName());
        assertEquals("qa", config.getInnerHitsTargets().get(1).getInnerHitName());
        assertEquals("qa.answer", config.getInnerHitsTargets().get(1).getFieldName());
    }

    /**
     * The locator must not claim inner_hits that carry a non-semantic highlighter
     */
    public void testLocatorSkipsInnerHitsWithNonSemanticHighlight() {
        HighlightBuilder plainHighlighter = new HighlightBuilder().field(new HighlightBuilder.Field(SEMANTIC_FIELD));
        NestedQueryBuilder nested = QueryBuilders.nestedQuery(NESTED_PATH, new MatchQueryBuilder(SEMANTIC_FIELD, QUERY_TEXT), ScoreMode.Avg)
            .innerHit(new InnerHitBuilder().setHighlightBuilder(plainHighlighter));

        assertTrue("non-semantic inner_hits highlight should be ignored", InnerHitsHighlightLocator.findAll(nested).isEmpty());
    }

    /**
     * HighlightContextBuilder must collect text from inner hits and store the inner
     * hits themselves as validHits
     */
    public void testContextBuilderCollectsTextAndInnerHits() {
        HighlightConfig config = innerHitsConfig(NESTED_PATH, SEMANTIC_FIELD);
        SearchResponse response = buildResponseWithInnerHits(
            List.of(
                List.of("Apple revenue hit 90 billion dollars.", "iPhone sales grew 8 percent."),
                List.of("Huawei released Mate 60 today.")
            )
        );

        HighlightContext context = new HighlightContextBuilder().build(config, response, 0L);

        assertEquals("one request per inner hit across all articles", 3, context.size());
        assertEquals("field name on context keeps the full nested path", SEMANTIC_FIELD, context.getFieldName());

        List<SentenceHighlightingRequest> requests = context.getRequests();
        assertEquals("Apple revenue hit 90 billion dollars.", requests.get(0).getContext());
        assertEquals("iPhone sales grew 8 percent.", requests.get(1).getContext());
        assertEquals("Huawei released Mate 60 today.", requests.get(2).getContext());

        List<SearchHit> validHits = context.getValidHits();
        assertEquals(3, validHits.size());
        for (SearchHit hit : validHits) {
            assertTrue("validHits must be inner hits (source has leaf key 'text')", hit.getSourceAsMap().containsKey("text"));
        }
    }

    /**
     * In multi-target mode, validHits come from different inner_hits buckets and the
     * per-hit field names track which bucket each hit belongs to.
     */
    public void testContextBuilderMultiTargetRecordsPerHitFieldName() {
        HighlightConfig config = HighlightConfig.builder()
            .fieldName("reviews.text")
            .modelId(MODEL_ID)
            .queryText(QUERY_TEXT)
            .batchInference(true)
            .innerHitsTargets(
                List.of(
                    new HighlightConfig.InnerHitsTarget("reviews", "reviews", "reviews.text"),
                    new HighlightConfig.InnerHitsTarget("qa", "qa", "qa.answer")
                )
            )
            .build();

        SearchResponse response = buildResponseWithMultipleInnerHits();

        HighlightContext context = new HighlightContextBuilder().build(config, response, 0L);

        assertEquals("one request per inner hit across both buckets", 2, context.size());
        assertNotNull("per-hit field names must be populated in multi-target mode", context.getFieldNames());
        assertEquals(2, context.getFieldNames().size());
        assertEquals("reviews.text", context.getFieldNames().get(0));
        assertEquals("qa.answer", context.getFieldNames().get(1));
    }

    /**
     * A request that declares both a top-level semantic highlight <i>and</i> a
     * nested inner_hits semantic highlight should produce a config carrying both.
     */
    public void testConfigBuilderKeepsBothTopLevelAndInnerHitsTargets() {
        NestedQueryBuilder nested = buildNestedWithSemanticInnerHits(NESTED_PATH, SEMANTIC_FIELD);

        HighlightBuilder.Field topLevelField = new HighlightBuilder.Field("title").highlighterType(
            SemanticHighlightingConstants.HIGHLIGHTER_TYPE
        );
        Map<String, Object> topOptions = new HashMap<>();
        topOptions.put(SemanticHighlightingConstants.MODEL_ID, MODEL_ID);
        topOptions.put(SemanticHighlightingConstants.BATCH_INFERENCE, true);
        HighlightBuilder topHighlighter = new HighlightBuilder().field(topLevelField).options(topOptions);

        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder().query(nested).highlighter(topHighlighter));

        HighlightConfig config = HighlightConfigBuilder.buildFromSearchRequest(request, buildEmptyResponseWithOneHit());

        assertNull(config.getValidationError());
        assertEquals("top-level field must be captured", "title", config.getFieldName());
        assertTrue("inner_hits targets must also be captured", config.hasInnerHitsTargets());
        assertEquals(1, config.getInnerHitsTargets().size());
        assertEquals(SEMANTIC_FIELD, config.getInnerHitsTargets().get(0).getFieldName());
    }

    /**
     * HighlightContextBuilder must emit requests for both the top-level field and
     * the nested inner_hits buckets when a request mixes the two, with per-hit
     * field names letting the applier tag each highlight under the right key.
     */
    public void testContextBuilderCoversTopLevelAndInnerHitsTogether() {
        HighlightConfig config = HighlightConfig.builder()
            .fieldName("title")
            .modelId(MODEL_ID)
            .queryText(QUERY_TEXT)
            .batchInference(true)
            .innerHitsTargets(List.of(new HighlightConfig.InnerHitsTarget(NESTED_PATH, NESTED_PATH, SEMANTIC_FIELD)))
            .build();

        SearchResponse response = buildResponseWithInnerHits(
            List.of(List.of("Apple revenue hit 90 billion dollars.", "iPhone sales grew 8 percent."))
        );

        HighlightContext context = new HighlightContextBuilder().build(config, response, 0L);

        // 1 top-level hit + 2 inner hits
        assertEquals(3, context.size());
        assertEquals(3, context.getFieldNames().size());
        assertEquals("title", context.getFieldNames().get(0));
        assertEquals(SEMANTIC_FIELD, context.getFieldNames().get(1));
        assertEquals(SEMANTIC_FIELD, context.getFieldNames().get(2));

        // First valid hit is the top-level doc, the next two are inner hits.
        assertTrue(context.getValidHits().get(0).getSourceAsMap().containsKey("title"));
        assertTrue(context.getValidHits().get(1).getSourceAsMap().containsKey("text"));
        assertTrue(context.getValidHits().get(2).getSourceAsMap().containsKey("text"));
    }

    // ------------------------- helpers -------------------------

    private HighlightConfig innerHitsConfig(String path, String field) {
        return HighlightConfig.builder()
            .fieldName(field)
            .modelId(MODEL_ID)
            .queryText(QUERY_TEXT)
            .batchInference(true)
            .innerHitsTargets(List.of(new HighlightConfig.InnerHitsTarget(path, path, field)))
            .build();
    }

    private SearchRequest buildInnerHitsRequest(String path, String field) {
        NestedQueryBuilder nested = buildNestedWithSemanticInnerHits(path, field);
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder().query(nested));
        return request;
    }

    private NestedQueryBuilder buildNestedWithSemanticInnerHits(String path, String field) {
        HighlightBuilder.Field hlField = new HighlightBuilder.Field(field).highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);
        Map<String, Object> options = new HashMap<>();
        options.put(SemanticHighlightingConstants.MODEL_ID, MODEL_ID);
        options.put(SemanticHighlightingConstants.BATCH_INFERENCE, true);
        HighlightBuilder innerHighlight = new HighlightBuilder().field(hlField).options(options);

        return QueryBuilders.nestedQuery(path, new MatchQueryBuilder(field, QUERY_TEXT), ScoreMode.Avg)
            .innerHit(new InnerHitBuilder().setHighlightBuilder(innerHighlight));
    }

    /**
     * Build a response whose top hit has two inner_hits buckets: "reviews" and "qa".
     */
    private SearchResponse buildResponseWithMultipleInnerHits() {
        SearchHit reviewHit = new SearchHit(0, "r-0", Collections.emptyMap(), Collections.emptyMap());
        reviewHit.sourceRef(new BytesArray("{\"text\":\"Great battery life\"}"));

        SearchHit qaHit = new SearchHit(0, "q-0", Collections.emptyMap(), Collections.emptyMap());
        qaHit.sourceRef(new BytesArray("{\"answer\":\"IP68 rated, yes\"}"));

        SearchHit top = new SearchHit(0, "product-0", Collections.emptyMap(), Collections.emptyMap());
        top.sourceRef(new BytesArray("{\"name\":\"iPhone 15 Pro\"}"));
        Map<String, SearchHits> innerMap = new HashMap<>();
        innerMap.put("reviews", new SearchHits(new SearchHit[] { reviewHit }, null, 1.0f));
        innerMap.put("qa", new SearchHits(new SearchHit[] { qaHit }, null, 1.0f));
        top.setInnerHits(innerMap);

        SearchHits hits = new SearchHits(new SearchHit[] { top }, null, 1.0f);
        InternalSearchResponse internal = new InternalSearchResponse(hits, null, null, null, false, null, 0);
        return new SearchResponse(internal, null, 1, 1, 0, 100, null, null);
    }

    private SearchResponse buildResponseWithInnerHits(List<List<String>> articlesChunks) {
        SearchHit[] topHits = new SearchHit[articlesChunks.size()];

        for (int i = 0; i < articlesChunks.size(); i++) {
            List<String> chunks = articlesChunks.get(i);
            SearchHit[] innerHitArr = new SearchHit[chunks.size()];
            for (int j = 0; j < chunks.size(); j++) {
                SearchHit inner = new SearchHit(j, "chunk-" + i + "-" + j, Collections.emptyMap(), Collections.emptyMap());
                inner.sourceRef(new BytesArray("{\"text\":\"" + chunks.get(j) + "\"}"));
                innerHitArr[j] = inner;
            }

            SearchHit top = new SearchHit(i, "doc-" + i, Collections.emptyMap(), Collections.emptyMap());
            top.sourceRef(new BytesArray("{\"title\":\"article " + i + "\"}"));
            Map<String, SearchHits> innerMap = new HashMap<>();
            innerMap.put(NESTED_PATH, new SearchHits(innerHitArr, null, 1.0f));
            top.setInnerHits(innerMap);
            topHits[i] = top;
        }

        SearchHits hits = new SearchHits(topHits, null, 1.0f);
        InternalSearchResponse internal = new InternalSearchResponse(hits, null, null, null, false, null, 0);
        return new SearchResponse(internal, null, 1, 1, 0, 100, null, null);
    }

    private SearchResponse buildEmptyResponseWithOneHit() {
        SearchHit hit = new SearchHit(0, "d0", Collections.emptyMap(), Collections.emptyMap());
        hit.sourceRef(new BytesArray("{}"));
        SearchHits hits = new SearchHits(new SearchHit[] { hit }, null, 1.0f);
        InternalSearchResponse internal = new InternalSearchResponse(hits, null, null, null, false, null, 0);
        return new SearchResponse(internal, null, 1, 1, 0, 100, null, null);
    }
}
