/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.neuralsearch.highlight.batch.HighlightContext;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HighlightContextTests extends OpenSearchTestCase {

    public void testIsEmpty() {
        // Test with empty requests
        HighlightContext emptyContext = HighlightContext.builder()
            .requests(new ArrayList<>())
            .validHits(new ArrayList<>())
            .fieldName("content")
            .originalResponse(createSearchResponse())
            .startTime(System.currentTimeMillis())
            .preTag("<em>")
            .postTag("</em>")
            .build();

        assertTrue(emptyContext.isEmpty());
        assertEquals(0, emptyContext.size());

        // Test with null requests
        HighlightContext nullContext = HighlightContext.builder()
            .requests(null)
            .validHits(new ArrayList<>())
            .fieldName("content")
            .originalResponse(createSearchResponse())
            .startTime(System.currentTimeMillis())
            .preTag("<em>")
            .postTag("</em>")
            .build();

        assertTrue(nullContext.isEmpty());
        assertEquals(0, nullContext.size());

        // Test with non-empty requests
        List<SentenceHighlightingRequest> requests = new ArrayList<>();
        requests.add(createRequest("test"));

        HighlightContext nonEmptyContext = HighlightContext.builder()
            .requests(requests)
            .validHits(Collections.singletonList(createHit(0)))
            .fieldName("content")
            .originalResponse(createSearchResponse())
            .startTime(System.currentTimeMillis())
            .preTag("<em>")
            .postTag("</em>")
            .build();

        assertFalse(nonEmptyContext.isEmpty());
        assertEquals(1, nonEmptyContext.size());
    }

    public void testDocumentManagement() {
        // Create context with multiple documents
        List<SentenceHighlightingRequest> requests = new ArrayList<>();
        List<SearchHit> validHits = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            requests.add(createRequest("context" + i));
            validHits.add(createHit(i));
        }

        HighlightContext context = HighlightContext.builder()
            .requests(requests)
            .validHits(validHits)
            .fieldName("content")
            .originalResponse(createSearchResponse())
            .startTime(System.currentTimeMillis())
            .preTag("<mark>")
            .postTag("</mark>")
            .build();

        // Verify document management
        assertEquals(5, context.size());
        assertNotNull(context.getRequests());
        assertNotNull(context.getValidHits());
        assertEquals(5, context.getRequests().size());
        assertEquals(5, context.getValidHits().size());

        // Verify requests and hits are aligned
        for (int i = 0; i < 5; i++) {
            assertEquals("context" + i, context.getRequests().get(i).getContext());
            assertEquals("doc" + i, context.getValidHits().get(i).getId());
        }
    }

    public void testFieldExtraction() {
        // Test field name storage
        String fieldName = "description";

        HighlightContext context = HighlightContext.builder()
            .requests(new ArrayList<>())
            .validHits(new ArrayList<>())
            .fieldName(fieldName)
            .originalResponse(createSearchResponse())
            .startTime(System.currentTimeMillis())
            .preTag("<em>")
            .postTag("</em>")
            .build();

        assertEquals(fieldName, context.getFieldName());
    }

    public void testMetricsTracking() {
        // Test start time tracking
        long startTime = 1234567890L;

        HighlightContext context = HighlightContext.builder()
            .requests(new ArrayList<>())
            .validHits(new ArrayList<>())
            .fieldName("content")
            .originalResponse(createSearchResponse())
            .startTime(startTime)
            .preTag("<em>")
            .postTag("</em>")
            .build();

        assertEquals(startTime, context.getStartTime());

        // Calculate elapsed time
        long currentTime = System.currentTimeMillis();
        long elapsed = currentTime - context.getStartTime();
        assertTrue(elapsed >= 0);
    }

    public void testTagManagement() {
        // Test custom tags
        String customPreTag = "{{";
        String customPostTag = "}}";

        HighlightContext context = HighlightContext.builder()
            .requests(new ArrayList<>())
            .validHits(new ArrayList<>())
            .fieldName("content")
            .originalResponse(createSearchResponse())
            .startTime(System.currentTimeMillis())
            .preTag(customPreTag)
            .postTag(customPostTag)
            .build();

        assertEquals(customPreTag, context.getPreTag());
        assertEquals(customPostTag, context.getPostTag());
    }

    public void testOriginalResponsePreservation() {
        // Create a specific search response
        SearchHit hit1 = createHit(1);
        SearchHit hit2 = createHit(2);
        SearchHits hits = new SearchHits(new SearchHit[] { hit1, hit2 }, null, 2.0f);

        InternalSearchResponse internalResponse = new InternalSearchResponse(hits, null, null, null, false, null, 0);

        SearchResponse originalResponse = new SearchResponse(internalResponse, null, 2, 2, 0, 150, null, null);

        HighlightContext context = HighlightContext.builder()
            .requests(new ArrayList<>())
            .validHits(new ArrayList<>())
            .fieldName("content")
            .originalResponse(originalResponse)
            .startTime(System.currentTimeMillis())
            .preTag("<em>")
            .postTag("</em>")
            .build();

        // Verify original response is preserved
        assertNotNull(context.getOriginalResponse());
        assertEquals(originalResponse, context.getOriginalResponse());
        assertEquals(2, context.getOriginalResponse().getHits().getHits().length);
    }

    public void testBuilderWithAllFields() {
        // Test builder with all fields populated
        List<SentenceHighlightingRequest> requests = new ArrayList<>();
        requests.add(createRequest("test1"));
        requests.add(createRequest("test2"));

        List<SearchHit> validHits = new ArrayList<>();
        validHits.add(createHit(1));
        validHits.add(createHit(2));

        String fieldName = "text_field";
        SearchResponse originalResponse = createSearchResponse();
        long startTime = 9876543210L;
        String preTag = "<strong>";
        String postTag = "</strong>";

        HighlightContext context = HighlightContext.builder()
            .requests(requests)
            .validHits(validHits)
            .fieldName(fieldName)
            .originalResponse(originalResponse)
            .startTime(startTime)
            .preTag(preTag)
            .postTag(postTag)
            .build();

        // Verify all fields
        assertEquals(requests, context.getRequests());
        assertEquals(validHits, context.getValidHits());
        assertEquals(fieldName, context.getFieldName());
        assertEquals(originalResponse, context.getOriginalResponse());
        assertEquals(startTime, context.getStartTime());
        assertEquals(preTag, context.getPreTag());
        assertEquals(postTag, context.getPostTag());
        assertEquals(2, context.size());
        assertFalse(context.isEmpty());
    }

    public void testImmutability() {
        // Create context
        List<SentenceHighlightingRequest> originalRequests = new ArrayList<>();
        originalRequests.add(createRequest("original"));

        List<SearchHit> originalHits = new ArrayList<>();
        originalHits.add(createHit(0));

        HighlightContext context = HighlightContext.builder()
            .requests(originalRequests)
            .validHits(originalHits)
            .fieldName("content")
            .originalResponse(createSearchResponse())
            .startTime(System.currentTimeMillis())
            .preTag("<em>")
            .postTag("</em>")
            .build();

        // Get references from context
        List<SentenceHighlightingRequest> retrievedRequests = context.getRequests();
        List<SearchHit> retrievedHits = context.getValidHits();

        // Verify we got the actual lists (not copies)
        // This is expected behavior for performance reasons
        assertEquals(originalRequests, retrievedRequests);
        assertEquals(originalHits, retrievedHits);
    }

    // Helper methods
    private SentenceHighlightingRequest createRequest(String context) {
        return SentenceHighlightingRequest.builder().modelId("test-model").question("query").context(context).build();
    }

    private SearchHit createHit(int id) {
        SearchHit hit = new SearchHit(id, "doc" + id, Collections.emptyMap(), Collections.emptyMap());
        hit.sourceRef(new BytesArray("{\"content\":\"content" + id + "\"}"));
        return hit;
    }

    public void testModelTypeAndModelId() {
        // Test with REMOTE model type
        HighlightContext remoteContext = HighlightContext.builder()
            .requests(new ArrayList<>())
            .validHits(new ArrayList<>())
            .fieldName("content")
            .originalResponse(createSearchResponse())
            .startTime(System.currentTimeMillis())
            .preTag("<em>")
            .postTag("</em>")
            .modelId("remote-model-123")
            .modelType(FunctionName.REMOTE)
            .build();

        assertEquals("remote-model-123", remoteContext.getModelId());
        assertEquals(FunctionName.REMOTE, remoteContext.getModelType());

        // Test with QUESTION_ANSWERING model type
        HighlightContext localContext = HighlightContext.builder()
            .requests(new ArrayList<>())
            .validHits(new ArrayList<>())
            .fieldName("content")
            .originalResponse(createSearchResponse())
            .startTime(System.currentTimeMillis())
            .preTag("<em>")
            .postTag("</em>")
            .modelId("local-model-456")
            .modelType(FunctionName.QUESTION_ANSWERING)
            .build();

        assertEquals("local-model-456", localContext.getModelId());
        assertEquals(FunctionName.QUESTION_ANSWERING, localContext.getModelType());

        // Test with null model type
        HighlightContext nullTypeContext = HighlightContext.builder()
            .requests(new ArrayList<>())
            .validHits(new ArrayList<>())
            .fieldName("content")
            .originalResponse(createSearchResponse())
            .startTime(System.currentTimeMillis())
            .preTag("<em>")
            .postTag("</em>")
            .modelId("model-789")
            .modelType(null)
            .build();

        assertEquals("model-789", nullTypeContext.getModelId());
        assertNull(nullTypeContext.getModelType());
    }

    private SearchResponse createSearchResponse() {
        SearchHits hits = new SearchHits(new SearchHit[0], null, 0.0f);
        InternalSearchResponse internalResponse = new InternalSearchResponse(hits, null, null, null, false, null, 0);

        return new SearchResponse(internalResponse, null, 0, 0, 0, 100, null, null);
    }
}
