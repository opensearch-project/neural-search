/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.common.bytes.BytesArray;
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
        SearchRequest request = buildInnerHitsRequest();
        SearchResponse response = buildResponseWithInnerHits(List.of(List.of("Apple revenue hit 90 billion dollars this quarter.")));

        HighlightConfig config = HighlightConfigBuilder.buildFromSearchRequest(request, response);

        assertNull("config should not have a validation error", config.getValidationError());
        assertEquals(SEMANTIC_FIELD, config.getFieldName());
        assertEquals(MODEL_ID, config.getModelId());
        assertEquals(QUERY_TEXT, config.getQueryText());
        assertTrue("batch_inference option should be carried over", config.isBatchInference());
        assertTrue("config should be flagged as inner_hits scoped", config.isInnerHitsScoped());
        assertEquals(NESTED_PATH, config.getInnerHitName());
        assertEquals(NESTED_PATH, config.getNestedPath());
    }

    /**
     * The locator must not claim inner_hits that carry a non-semantic highlighter
     */
    public void testLocatorSkipsInnerHitsWithNonSemanticHighlight() {
        HighlightBuilder plainHighlighter = new HighlightBuilder().field(new HighlightBuilder.Field(SEMANTIC_FIELD));
        NestedQueryBuilder nested = QueryBuilders.nestedQuery(NESTED_PATH, new MatchQueryBuilder(SEMANTIC_FIELD, QUERY_TEXT), ScoreMode.Avg)
            .innerHit(new InnerHitBuilder().setHighlightBuilder(plainHighlighter));

        assertNull(InnerHitsHighlightLocator.find(nested));
    }

    /**
     * HighlightContextBuilder must collect text from inner hits and store the inner
     * hits themselves as validHits
     */
    public void testContextBuilderCollectsTextAndInnerHits() {
        HighlightConfig config = innerHitsConfig();
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

    // ------------------------- helpers -------------------------

    private HighlightConfig innerHitsConfig() {
        return HighlightConfig.builder()
            .fieldName(SEMANTIC_FIELD)
            .modelId(MODEL_ID)
            .queryText(QUERY_TEXT)
            .batchInference(true)
            .innerHitName(NESTED_PATH)
            .nestedPath(NESTED_PATH)
            .build();
    }

    private SearchRequest buildInnerHitsRequest() {
        HighlightBuilder.Field field = new HighlightBuilder.Field(SEMANTIC_FIELD).highlighterType(
            SemanticHighlightingConstants.HIGHLIGHTER_TYPE
        );
        Map<String, Object> options = new HashMap<>();
        options.put(SemanticHighlightingConstants.MODEL_ID, MODEL_ID);
        options.put(SemanticHighlightingConstants.BATCH_INFERENCE, true);

        HighlightBuilder innerHitsHighlighter = new HighlightBuilder().field(field).options(options);

        NestedQueryBuilder nested = QueryBuilders.nestedQuery(NESTED_PATH, new MatchQueryBuilder(SEMANTIC_FIELD, QUERY_TEXT), ScoreMode.Avg)
            .innerHit(new InnerHitBuilder().setHighlightBuilder(innerHitsHighlighter));

        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder().query(nested));
        return request;
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
}
