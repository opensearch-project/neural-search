/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import org.junit.Before;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.neuralsearch.highlight.batch.config.HighlightContextBuilder;
import org.opensearch.neuralsearch.highlight.batch.config.HighlightConfig;
import org.opensearch.neuralsearch.highlight.batch.HighlightContext;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HighlightContextBuilderTests extends OpenSearchTestCase {

    private HighlightContextBuilder contextBuilder;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        contextBuilder = new HighlightContextBuilder();
    }

    public void testBuildWithMultipleHits() {
        // Setup
        HighlightConfig config = HighlightConfig.builder()
            .fieldName("content")
            .modelId("test-model")
            .queryText("test query")
            .preTag("<em>")
            .postTag("</em>")
            .build();

        SearchHit hit1 = new SearchHit(1, "doc1", Collections.emptyMap(), Collections.emptyMap());
        hit1.sourceRef(new BytesArray("{\"content\":\"First document content\"}"));

        SearchHit hit2 = new SearchHit(2, "doc2", Collections.emptyMap(), Collections.emptyMap());
        hit2.sourceRef(new BytesArray("{\"content\":\"Second document content\"}"));

        SearchHit hit3 = new SearchHit(3, "doc3", Collections.emptyMap(), Collections.emptyMap());
        hit3.sourceRef(new BytesArray("{\"content\":\"Third document content\"}"));

        SearchHits hits = new SearchHits(new SearchHit[] { hit1, hit2, hit3 }, null, 1.0f);
        SearchResponse response = createSearchResponse(hits);

        long startTime = System.currentTimeMillis();

        // Execute
        HighlightContext context = contextBuilder.build(config, response, startTime);

        // Verify
        assertNotNull(context);
        assertFalse(context.isEmpty());
        assertEquals(3, context.size());
        assertEquals("content", context.getFieldName());
        assertEquals("<em>", context.getPreTag());
        assertEquals("</em>", context.getPostTag());
        assertEquals(startTime, context.getStartTime());

        List<SentenceHighlightingRequest> requests = context.getRequests();
        assertEquals(3, requests.size());

        // Verify first request
        SentenceHighlightingRequest request1 = requests.get(0);
        assertEquals("test-model", request1.getModelId());
        assertEquals("test query", request1.getQuestion());
        assertEquals("First document content", request1.getContext());

        // Verify second request
        SentenceHighlightingRequest request2 = requests.get(1);
        assertEquals("test-model", request2.getModelId());
        assertEquals("test query", request2.getQuestion());
        assertEquals("Second document content", request2.getContext());

        // Verify the valid hits are stored
        List<SearchHit> validHits = context.getValidHits();
        assertEquals(3, validHits.size());
        assertEquals("doc1", validHits.get(0).getId());
        assertEquals("doc2", validHits.get(1).getId());
        assertEquals("doc3", validHits.get(2).getId());
    }

    public void testBuildWithEmptyHits() {
        // Setup
        HighlightConfig config = HighlightConfig.builder().fieldName("content").modelId("test-model").queryText("test query").build();

        SearchHits hits = new SearchHits(new SearchHit[0], null, 0.0f);
        SearchResponse response = createSearchResponse(hits);

        long startTime = System.currentTimeMillis();

        // Execute
        HighlightContext context = contextBuilder.build(config, response, startTime);

        // Verify
        assertNotNull(context);
        assertTrue(context.isEmpty());
        assertEquals(0, context.size());
        assertEquals("content", context.getFieldName());
        assertNotNull(context.getRequests());
        assertNotNull(context.getValidHits());
        assertEquals(0, context.getRequests().size());
        assertEquals(0, context.getValidHits().size());
    }

    public void testBuildWithNullFieldValues() {
        // Setup
        HighlightConfig config = HighlightConfig.builder().fieldName("content").modelId("test-model").queryText("test query").build();

        // Hit with content field
        SearchHit hit1 = new SearchHit(1, "doc1", Collections.emptyMap(), Collections.emptyMap());
        hit1.sourceRef(new BytesArray("{\"content\":\"Valid content\"}"));

        // Hit without content field
        SearchHit hit2 = new SearchHit(2, "doc2", Collections.emptyMap(), Collections.emptyMap());
        hit2.sourceRef(new BytesArray("{\"title\":\"Only title, no content\"}"));

        // Hit with null content
        SearchHit hit3 = new SearchHit(3, "doc3", Collections.emptyMap(), Collections.emptyMap());
        hit3.sourceRef(new BytesArray("{\"content\":null}"));

        // Hit with empty content
        SearchHit hit4 = new SearchHit(4, "doc4", Collections.emptyMap(), Collections.emptyMap());
        hit4.sourceRef(new BytesArray("{\"content\":\"\"}"));

        SearchHits hits = new SearchHits(new SearchHit[] { hit1, hit2, hit3, hit4 }, null, 1.0f);
        SearchResponse response = createSearchResponse(hits);

        long startTime = System.currentTimeMillis();

        // Execute
        HighlightContext context = contextBuilder.build(config, response, startTime);

        // Verify - only hit1 should be included
        assertNotNull(context);
        assertEquals(1, context.size());
        assertEquals("Valid content", context.getRequests().get(0).getContext());
        assertEquals("doc1", context.getValidHits().get(0).getId());
    }

    public void testBuildWithMixedFieldTypes() {
        // Setup
        HighlightConfig config = HighlightConfig.builder().fieldName("content").modelId("test-model").queryText("test query").build();

        // Hit with string content
        SearchHit hit1 = new SearchHit(1, "doc1", Collections.emptyMap(), Collections.emptyMap());
        hit1.sourceRef(new BytesArray("{\"content\":\"String content\"}"));

        // Hit with array content
        SearchHit hit2 = new SearchHit(2, "doc2", Collections.emptyMap(), Collections.emptyMap());
        hit2.sourceRef(new BytesArray("{\"content\":[\"First part\",\"Second part\"]}"));

        // Hit with numeric content (should be converted to string)
        SearchHit hit3 = new SearchHit(3, "doc3", Collections.emptyMap(), Collections.emptyMap());
        Map<String, Object> sourceMap3 = new HashMap<>();
        sourceMap3.put("content", 12345);
        hit3.sourceRef(new BytesArray("{\"content\":12345}"));

        // Hit with boolean content
        SearchHit hit4 = new SearchHit(4, "doc4", Collections.emptyMap(), Collections.emptyMap());
        Map<String, Object> sourceMap4 = new HashMap<>();
        sourceMap4.put("content", true);
        hit4.sourceRef(new BytesArray("{\"content\":true}"));

        SearchHits hits = new SearchHits(new SearchHit[] { hit1, hit2, hit3, hit4 }, null, 1.0f);
        SearchResponse response = createSearchResponse(hits);

        long startTime = System.currentTimeMillis();

        // Execute
        HighlightContext context = contextBuilder.build(config, response, startTime);

        // Verify
        assertNotNull(context);
        assertEquals(4, context.size());

        List<SentenceHighlightingRequest> requests = context.getRequests();
        assertEquals("String content", requests.get(0).getContext());
        assertEquals("First part Second part", requests.get(1).getContext());  // Array joined with spaces
        assertEquals("12345", requests.get(2).getContext());  // Numeric converted to string
        assertEquals("true", requests.get(3).getContext());  // Boolean converted to string
    }

    public void testBuildWithNullHits() {
        // Setup
        HighlightConfig config = HighlightConfig.builder().fieldName("content").modelId("test-model").queryText("test query").build();

        InternalSearchResponse internalResponse = new InternalSearchResponse(
            null,  // null hits
            null,
            null,
            null,
            false,
            null,
            0
        );
        SearchResponse response = new SearchResponse(internalResponse, null, 1, 1, 0, 100, null, null);

        long startTime = System.currentTimeMillis();

        // Execute
        HighlightContext context = contextBuilder.build(config, response, startTime);

        // Verify
        assertNotNull(context);
        assertTrue(context.isEmpty());
        assertEquals(0, context.size());
    }

    public void testStartTimePreservation() {
        // Setup
        HighlightConfig config = HighlightConfig.builder().fieldName("content").modelId("test-model").queryText("test query").build();

        SearchHit hit = new SearchHit(1, "doc1", Collections.emptyMap(), Collections.emptyMap());
        hit.sourceRef(new BytesArray("{\"content\":\"Test content\"}"));

        SearchHits hits = new SearchHits(new SearchHit[] { hit }, null, 1.0f);
        SearchResponse response = createSearchResponse(hits);

        long startTime = 1234567890L;

        // Execute
        HighlightContext context = contextBuilder.build(config, response, startTime);

        // Verify
        assertEquals(startTime, context.getStartTime());
    }

    public void testBuildWithArrayFieldHandling() {
        // Setup
        HighlightConfig config = HighlightConfig.builder().fieldName("tags").modelId("test-model").queryText("search query").build();

        SearchHit hit = new SearchHit(1, "doc1", Collections.emptyMap(), Collections.emptyMap());
        // Array with mixed types
        hit.sourceRef(new BytesArray("{\"tags\":[\"tag1\",\"tag2\",null,\"tag3\"]}"));

        SearchHits hits = new SearchHits(new SearchHit[] { hit }, null, 1.0f);
        SearchResponse response = createSearchResponse(hits);

        long startTime = System.currentTimeMillis();

        // Execute
        HighlightContext context = contextBuilder.build(config, response, startTime);

        // Verify
        assertNotNull(context);
        assertEquals(1, context.size());
        // Nulls should be skipped in the joined string
        assertEquals("tag1 tag2 tag3", context.getRequests().get(0).getContext());
    }

    public void testBuildWithEmptyArrayField() {
        // Setup
        HighlightConfig config = HighlightConfig.builder().fieldName("tags").modelId("test-model").queryText("search query").build();

        SearchHit hit = new SearchHit(1, "doc1", Collections.emptyMap(), Collections.emptyMap());
        hit.sourceRef(new BytesArray("{\"tags\":[]}"));

        SearchHits hits = new SearchHits(new SearchHit[] { hit }, null, 1.0f);
        SearchResponse response = createSearchResponse(hits);

        long startTime = System.currentTimeMillis();

        // Execute
        HighlightContext context = contextBuilder.build(config, response, startTime);

        // Verify - empty array should result in no requests
        assertNotNull(context);
        assertEquals(0, context.size());
        assertTrue(context.isEmpty());
    }

    public void testBuildWithDefaultTags() {
        // Setup
        HighlightConfig config = HighlightConfig.builder().fieldName("content").modelId("test-model").queryText("test query").build();

        SearchHit hit = new SearchHit(1, "doc1", Collections.emptyMap(), Collections.emptyMap());
        hit.sourceRef(new BytesArray("{\"content\":\"Test content\"}"));

        SearchHits hits = new SearchHits(new SearchHit[] { hit }, null, 1.0f);
        SearchResponse response = createSearchResponse(hits);

        long startTime = System.currentTimeMillis();

        // Execute
        HighlightContext context = contextBuilder.build(config, response, startTime);

        // Verify default tags are preserved
        assertEquals(SemanticHighlightingConstants.DEFAULT_PRE_TAG, context.getPreTag());
        assertEquals(SemanticHighlightingConstants.DEFAULT_POST_TAG, context.getPostTag());
    }

    // Helper method to create SearchResponse
    private SearchResponse createSearchResponse(SearchHits hits) {
        InternalSearchResponse internalResponse = new InternalSearchResponse(hits, null, null, null, false, null, 0);

        return new SearchResponse(internalResponse, null, 1, 1, 0, 100, null, null);
    }
}
