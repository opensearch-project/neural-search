/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.batch.processor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.search.TotalHits;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.util.TestUtils;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class SemanticHighlightingProcessorTests extends OpenSearchTestCase {

    @Mock
    private MLCommonsClientAccessor mlClientAccessor;

    private SemanticHighlightingProcessor processor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        TestUtils.initializeEventStatsManager();
        processor = new SemanticHighlightingProcessor(false, mlClientAccessor);
    }

    public void testProcessResponseThrowsUnsupported() {
        SearchRequest request = new SearchRequest();
        SearchResponse response = mockResponse(new SearchHit[0]);
        expectThrows(UnsupportedOperationException.class, () -> processor.processResponse(request, response));
    }

    public void testPassesThroughWhenNoTargets() throws Exception {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(new MatchQueryBuilder("title", "test"));
        // No highlight at all
        request.source(source);

        SearchResponse response = mockResponse(new SearchHit[] { hitWithSource("1", Map.of("title", "test")) });
        AtomicReference<SearchResponse> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        processor.processResponseAsync(request, response, mock(PipelineProcessingContext.class), ActionListener.wrap(r -> {
            result.set(r);
            latch.countDown();
        }, e -> {
            fail("unexpected failure: " + e.getMessage());
            latch.countDown();
        }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertSame(response, result.get());
        verify(mlClientAccessor, never()).batchInferenceSentenceHighlighting(anyString(), anyList(), any(), any());
    }

    public void testPassesThroughWhenQueryTextIsNull() throws Exception {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        // No query set — queryText will be null
        HighlightBuilder hl = new HighlightBuilder();
        hl.field(new HighlightBuilder.Field("body").highlighterType("semantic"));
        hl.options(Map.of("model_id", "m1"));
        source.highlighter(hl);
        request.source(source);

        SearchResponse response = mockResponse(new SearchHit[] { hitWithSource("1", Map.of("body", "text")) });
        AtomicReference<SearchResponse> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        processor.processResponseAsync(request, response, mock(PipelineProcessingContext.class), ActionListener.wrap(r -> {
            result.set(r);
            latch.countDown();
        }, e -> {
            fail("unexpected failure: " + e.getMessage());
            latch.countDown();
        }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertSame(response, result.get());
    }

    public void testFailsWhenModelIdMissing() throws Exception {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(new MatchQueryBuilder("body", "treatments"));
        HighlightBuilder hl = new HighlightBuilder();
        hl.field(new HighlightBuilder.Field("body").highlighterType("semantic"));
        // No model_id in options — context builder skips the target, so processor passes through
        source.highlighter(hl);
        request.source(source);

        SearchResponse response = mockResponse(new SearchHit[] { hitWithSource("1", Map.of("body", "text")) });
        AtomicReference<SearchResponse> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        processor.processResponseAsync(request, response, mock(PipelineProcessingContext.class), ActionListener.wrap(r -> {
            result.set(r);
            latch.countDown();
        }, e -> {
            fail("unexpected failure: " + e.getMessage());
            latch.countDown();
        }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        // No model_id means target is skipped → empty context → pass through
        assertSame(response, result.get());
        verify(mlClientAccessor, never()).batchInferenceSentenceHighlighting(anyString(), anyList(), any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testSingleBatchCallsMLAndAppliesHighlights() throws Exception {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(new MatchQueryBuilder("body", "treatments"));
        HighlightBuilder hl = new HighlightBuilder();
        hl.field(new HighlightBuilder.Field("body").highlighterType("semantic"));
        hl.options(Map.of("model_id", "m1"));
        source.highlighter(hl);
        request.source(source);

        SearchHit hit = hitWithSource("1", Map.of("body", "alpha beta gamma"));
        SearchResponse response = mockResponse(new SearchHit[] { hit });

        // Mock ML client to return spans
        doAnswer(invocation -> {
            ActionListener<List<List<Map<String, Object>>>> listener = invocation.getArgument(3);
            listener.onResponse(List.of(List.of(Map.of("start", 6, "end", 10))));
            return null;
        }).when(mlClientAccessor).batchInferenceSentenceHighlighting(eq("m1"), anyList(), eq(FunctionName.REMOTE), any());

        AtomicReference<SearchResponse> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        processor.processResponseAsync(request, response, mock(PipelineProcessingContext.class), ActionListener.wrap(r -> {
            result.set(r);
            latch.countDown();
        }, e -> {
            fail("unexpected failure: " + e.getMessage());
            latch.countDown();
        }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(result.get());
        // Verify highlight was applied
        SearchHit resultHit = result.get().getHits().getHits()[0];
        assertNotNull(resultHit.getHighlightFields().get("body"));
        String highlighted = resultHit.getHighlightFields().get("body").fragments()[0].string();
        assertEquals("alpha <em>beta</em> gamma", highlighted);
    }

    @SuppressWarnings("unchecked")
    public void testHandlesMLClientError() throws Exception {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(new MatchQueryBuilder("body", "treatments"));
        HighlightBuilder hl = new HighlightBuilder();
        hl.field(new HighlightBuilder.Field("body").highlighterType("semantic"));
        hl.options(Map.of("model_id", "m1"));
        source.highlighter(hl);
        request.source(source);

        SearchHit hit = hitWithSource("1", Map.of("body", "alpha beta gamma"));
        SearchResponse response = mockResponse(new SearchHit[] { hit });

        // Mock ML client to return error
        doAnswer(invocation -> {
            ActionListener<List<List<Map<String, Object>>>> listener = invocation.getArgument(3);
            listener.onFailure(new RuntimeException("ML model unavailable"));
            return null;
        }).when(mlClientAccessor).batchInferenceSentenceHighlighting(eq("m1"), anyList(), eq(FunctionName.REMOTE), any());

        AtomicReference<Exception> error = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        processor.processResponseAsync(request, response, mock(PipelineProcessingContext.class), ActionListener.wrap(r -> {
            fail("expected failure");
            latch.countDown();
        }, e -> {
            error.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(error.get());
        assertTrue(error.get().getMessage().contains("ML model unavailable"));
    }

    @SuppressWarnings("unchecked")
    public void testIgnoreFailureReturnsOriginalResponse() throws Exception {
        SemanticHighlightingProcessor ignoreProcessor = new SemanticHighlightingProcessor(true, mlClientAccessor);

        SearchRequest request = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(new MatchQueryBuilder("body", "treatments"));
        HighlightBuilder hl = new HighlightBuilder();
        hl.field(new HighlightBuilder.Field("body").highlighterType("semantic"));
        hl.options(Map.of("model_id", "m1"));
        source.highlighter(hl);
        request.source(source);

        SearchHit hit = hitWithSource("1", Map.of("body", "alpha beta gamma"));
        SearchResponse response = mockResponse(new SearchHit[] { hit });

        doAnswer(invocation -> {
            ActionListener<List<List<Map<String, Object>>>> listener = invocation.getArgument(3);
            listener.onFailure(new RuntimeException("ML model unavailable"));
            return null;
        }).when(mlClientAccessor).batchInferenceSentenceHighlighting(eq("m1"), anyList(), eq(FunctionName.REMOTE), any());

        AtomicReference<SearchResponse> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        ignoreProcessor.processResponseAsync(request, response, mock(PipelineProcessingContext.class), ActionListener.wrap(r -> {
            result.set(r);
            latch.countDown();
        }, e -> {
            fail("should not fail with ignoreFailure=true");
            latch.countDown();
        }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertSame(response, result.get());
    }

    public void testPassesThroughWhenContextIsEmpty() throws Exception {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(new MatchQueryBuilder("body", "treatments"));
        HighlightBuilder hl = new HighlightBuilder();
        hl.field(new HighlightBuilder.Field("body").highlighterType("semantic"));
        hl.options(Map.of("model_id", "m1"));
        source.highlighter(hl);
        request.source(source);

        // Response with hits that don't have the "body" field in source
        SearchHit hit = hitWithSource("1", Map.of("title", "no body here"));
        SearchResponse response = mockResponse(new SearchHit[] { hit });

        AtomicReference<SearchResponse> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        processor.processResponseAsync(request, response, mock(PipelineProcessingContext.class), ActionListener.wrap(r -> {
            result.set(r);
            latch.countDown();
        }, e -> {
            fail("unexpected failure: " + e.getMessage());
            latch.countDown();
        }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertSame(response, result.get());
        verify(mlClientAccessor, never()).batchInferenceSentenceHighlighting(anyString(), anyList(), any(), any());
    }

    public void testGetTypeReturnsCorrectValue() {
        assertEquals(SemanticHighlightingConstants.PROCESSOR_TYPE, processor.getType());
    }

    public void testGetTagReturnsDefault() {
        assertEquals(SemanticHighlightingConstants.DEFAULT_PROCESSOR_TAG, processor.getTag());
    }

    public void testIsIgnoreFailureReturnsFalse() {
        assertFalse(processor.isIgnoreFailure());
    }

    public void testIsIgnoreFailureReturnsTrue() {
        SemanticHighlightingProcessor ignoreProcessor = new SemanticHighlightingProcessor(true, mlClientAccessor);
        assertTrue(ignoreProcessor.isIgnoreFailure());
    }

    private static SearchHit hitWithSource(String id, Map<String, Object> source) {
        SearchHit hit = new SearchHit(0, id, new HashMap<>(), new HashMap<>());
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            sb.append('"').append(entry.getKey()).append("\":\"").append(entry.getValue()).append('"');
        }
        sb.append('}');
        BytesReference src = new BytesArray(sb.toString());
        hit.sourceRef(src);
        return hit;
    }

    private static SearchResponse mockResponse(SearchHit[] hits) {
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponseSections sections = new SearchResponseSections(searchHits, null, null, false, false, null, 1);
        return new SearchResponse(sections, null, 1, 1, 0, 1, null, SearchResponse.Clusters.EMPTY);
    }
}
