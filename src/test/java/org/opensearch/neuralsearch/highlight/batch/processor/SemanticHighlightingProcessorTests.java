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
import org.opensearch.ml.common.MLModel;
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
import static org.mockito.Mockito.when;

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
        // By default the processor sees a REMOTE-typed model so existing tests exercise
        // the success path. Tests that verify the local-model rejection override this.
        stubModelTypeAs(FunctionName.REMOTE);
    }

    private void stubModelTypeAs(FunctionName algorithm) {
        MLModel model = mock(MLModel.class);
        when(model.getAlgorithm()).thenReturn(algorithm);
        doAnswer(invocation -> {
            ActionListener<MLModel> listener = invocation.getArgument(1);
            listener.onResponse(model);
            return null;
        }).when(mlClientAccessor).getModel(anyString(), any());
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
        AtomicReference<Exception> error = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        processor.processResponseAsync(request, response, mock(PipelineProcessingContext.class), ActionListener.wrap(r -> {
            fail("expected failure when query text cannot be extracted");
            latch.countDown();
        }, e -> {
            error.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(error.get());
        assertTrue(error.get().getMessage(), error.get().getMessage().contains("Could not extract query text"));
    }

    public void testFailsWhenModelIdMissing() throws Exception {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(new MatchQueryBuilder("body", "treatments"));
        HighlightBuilder hl = new HighlightBuilder();
        hl.field(new HighlightBuilder.Field("body").highlighterType("semantic"));
        // No model_id in options — context builder throws so the customer notices.
        source.highlighter(hl);
        request.source(source);

        SearchResponse response = mockResponse(new SearchHit[] { hitWithSource("1", Map.of("body", "text")) });
        AtomicReference<Exception> error = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        processor.processResponseAsync(request, response, mock(PipelineProcessingContext.class), ActionListener.wrap(r -> {
            fail("expected failure when model_id is missing");
            latch.countDown();
        }, e -> {
            error.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(error.get());
        assertTrue(error.get().getMessage(), error.get().getMessage().contains("model_id is required"));
        verify(mlClientAccessor, never()).batchInferenceSentenceHighlighting(anyString(), anyList(), any(), any());
    }

    /** Local-typed model + batch path → processor rejects up-front with a clear error. */
    public void testFailsWhenModelTypeIsNotRemote() throws Exception {
        stubModelTypeAs(FunctionName.QUESTION_ANSWERING);

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

        AtomicReference<Exception> error = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        processor.processResponseAsync(request, response, mock(PipelineProcessingContext.class), ActionListener.wrap(r -> {
            fail("expected failure when model is local-typed");
            latch.countDown();
        }, e -> {
            error.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(error.get());
        assertTrue(error.get().getMessage(), error.get().getMessage().contains("only supported for REMOTE models"));
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

    public void testGetExecutionStageReturnsPostUserDefined() {
        assertEquals(
            org.opensearch.search.pipeline.SystemGeneratedProcessor.ExecutionStage.POST_USER_DEFINED,
            processor.getExecutionStage()
        );
    }

    public void testGetDescriptionReturnsDefault() {
        assertEquals(SemanticHighlightingConstants.DEFAULT_PROCESSOR_DESCRIPTION, processor.getDescription());
    }

    @SuppressWarnings("unchecked")
    public void testPaginatedBatchProcessesAllSlices() throws Exception {
        // 3 hits with maxBatchSize=2 should trigger 2 batches: [0,2) and [2,3)
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(new MatchQueryBuilder("body", "treatments"));
        HighlightBuilder hl = new HighlightBuilder();
        HighlightBuilder.Field field = new HighlightBuilder.Field("body");
        field.highlighterType("semantic");
        hl.field(field);
        hl.options(Map.of("model_id", "m1", "max_inference_batch_size", 2));
        source.highlighter(hl);
        request.source(source);

        SearchHit hit1 = hitWithSource("1", Map.of("body", "alpha beta gamma"));
        SearchHit hit2 = hitWithSource("2", Map.of("body", "delta epsilon zeta"));
        SearchHit hit3 = hitWithSource("3", Map.of("body", "eta theta iota"));
        SearchResponse response = mockResponse(new SearchHit[] { hit1, hit2, hit3 });

        // Track invocations and respond with span at offset 6-10 each time
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        doAnswer(invocation -> {
            calls.incrementAndGet();
            ActionListener<List<List<Map<String, Object>>>> listener = invocation.getArgument(3);
            List<org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest> slice = invocation.getArgument(1);
            // Return the same number of result rows as the slice size
            List<List<Map<String, Object>>> results = new java.util.ArrayList<>();
            for (int i = 0; i < slice.size(); i++) {
                results.add(List.of(Map.of("start", 6, "end", 10)));
            }
            listener.onResponse(results);
            return null;
        }).when(mlClientAccessor).batchInferenceSentenceHighlighting(eq("m1"), anyList(), any(FunctionName.class), any());

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
        assertEquals("ML must be called twice for 3 hits with maxBatchSize=2", 2, calls.get());
        // Every hit got its highlight
        assertNotNull(result.get().getHits().getHits()[0].getHighlightFields().get("body"));
        assertNotNull(result.get().getHits().getHits()[1].getHighlightFields().get("body"));
        assertNotNull(result.get().getHits().getHits()[2].getHighlightFields().get("body"));
    }

    @SuppressWarnings("unchecked")
    public void testApplyTimeExceptionRoutesThroughHandleError() throws Exception {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(new MatchQueryBuilder("body", "treatments"));
        HighlightBuilder hl = new HighlightBuilder();
        HighlightBuilder.Field field = new HighlightBuilder.Field("body");
        field.highlighterType("semantic");
        hl.field(field);
        hl.options(Map.of("model_id", "m1"));
        source.highlighter(hl);
        request.source(source);

        SearchHit hit = hitWithSource("1", Map.of("body", "alpha"));
        SearchResponse response = mockResponse(new SearchHit[] { hit });

        // ML returns more rows than expected → applier throws batch-size mismatch
        doAnswer(invocation -> {
            ActionListener<List<List<Map<String, Object>>>> listener = invocation.getArgument(3);
            listener.onResponse(List.of(List.of(), List.of()));
            return null;
        }).when(mlClientAccessor).batchInferenceSentenceHighlighting(eq("m1"), anyList(), any(FunctionName.class), any());

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
    }

    @SuppressWarnings("unchecked")
    public void testIgnoreFailureWithMLErrorReturnsOriginalResponse() throws Exception {
        SemanticHighlightingProcessor ignoreProcessor = new SemanticHighlightingProcessor(true, mlClientAccessor);

        SearchRequest request = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(new MatchQueryBuilder("body", "treatments"));
        HighlightBuilder hl = new HighlightBuilder();
        HighlightBuilder.Field field = new HighlightBuilder.Field("body");
        field.highlighterType("semantic");
        hl.field(field);
        hl.options(Map.of("model_id", "m1"));
        source.highlighter(hl);
        request.source(source);

        SearchHit hit = hitWithSource("1", Map.of("body", "alpha beta gamma"));
        SearchResponse response = mockResponse(new SearchHit[] { hit });

        // Throwable (not Exception) — exercises the new RuntimeException wrap branch
        doAnswer(invocation -> {
            ActionListener<List<List<Map<String, Object>>>> listener = invocation.getArgument(3);
            listener.onFailure(new RuntimeException("ML model unavailable"));
            return null;
        }).when(mlClientAccessor).batchInferenceSentenceHighlighting(eq("m1"), anyList(), any(FunctionName.class), any());

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
