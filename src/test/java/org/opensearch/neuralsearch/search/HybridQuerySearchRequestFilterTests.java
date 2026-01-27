/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;

public class HybridQuerySearchRequestFilterTests extends OpenSearchQueryTestCase {

    private HybridQuerySearchRequestFilter filter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        filter = new HybridQuerySearchRequestFilter();
    }

    public void testOrder_thenReturnsZero() {
        assertEquals(0, filter.order());
    }

    @SuppressWarnings("unchecked")
    public void testApply_whenHybridQueryWithDefaultBatchReduceSize_thenDisablesBatchedReduction() {
        // Setup
        HybridQueryBuilder hybridQuery = new HybridQueryBuilder();
        hybridQuery.add(new MatchQueryBuilder("field", "value"));
        hybridQuery.add(new MatchAllQueryBuilder());

        SearchRequest searchRequest = new SearchRequest("test_index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(hybridQuery);
        searchRequest.source(sourceBuilder);

        // Verify default batch reduce size before filter
        assertEquals(SearchRequest.DEFAULT_BATCHED_REDUCE_SIZE, searchRequest.getBatchedReduceSize());

        Task task = mock(Task.class);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionFilterChain<SearchRequest, ActionResponse> chain = mock(ActionFilterChain.class);

        // Execute
        filter.apply(task, SearchAction.NAME, searchRequest, listener, chain);

        // Verify batch reduce size was changed to MAX_VALUE
        assertEquals(Integer.MAX_VALUE, searchRequest.getBatchedReduceSize());
        verify(chain).proceed(eq(task), eq(SearchAction.NAME), eq(searchRequest), eq(listener));
    }

    @SuppressWarnings("unchecked")
    public void testApply_whenHybridQueryWithCustomBatchReduceSize_thenOverridesUserConfig() {
        // Setup - user explicitly set a custom batch reduce size
        int customBatchReduceSize = 1024;

        HybridQueryBuilder hybridQuery = new HybridQueryBuilder();
        hybridQuery.add(new MatchQueryBuilder("field", "value"));

        SearchRequest searchRequest = new SearchRequest("test_index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(hybridQuery);
        searchRequest.source(sourceBuilder);
        searchRequest.setBatchedReduceSize(customBatchReduceSize);

        Task task = mock(Task.class);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionFilterChain<SearchRequest, ActionResponse> chain = mock(ActionFilterChain.class);

        // Execute
        filter.apply(task, SearchAction.NAME, searchRequest, listener, chain);

        // Verify batch reduce size was overridden - batched reduction is incompatible with hybrid queries
        assertEquals(Integer.MAX_VALUE, searchRequest.getBatchedReduceSize());

        // Verify chain.proceed was called
        verify(chain).proceed(eq(task), eq(SearchAction.NAME), eq(searchRequest), eq(listener));
    }

    @SuppressWarnings("unchecked")
    public void testApply_whenNonHybridQuery_thenDoesNotModifyBatchReduceSize() {
        // Setup with regular match query (not hybrid)
        SearchRequest searchRequest = new SearchRequest("test_index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(new MatchQueryBuilder("field", "value"));
        searchRequest.source(sourceBuilder);

        int originalBatchReduceSize = searchRequest.getBatchedReduceSize();

        Task task = mock(Task.class);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionFilterChain<SearchRequest, ActionResponse> chain = mock(ActionFilterChain.class);

        // Execute
        filter.apply(task, SearchAction.NAME, searchRequest, listener, chain);

        // Verify batch reduce size was not changed
        assertEquals(originalBatchReduceSize, searchRequest.getBatchedReduceSize());

        // Verify chain.proceed was called
        verify(chain).proceed(eq(task), eq(SearchAction.NAME), eq(searchRequest), eq(listener));
    }

    @SuppressWarnings("unchecked")
    public void testApply_whenNullSource_thenDoesNotModifyRequest() {
        // Setup with null source
        SearchRequest searchRequest = new SearchRequest("test_index");
        // source is null by default

        int originalBatchReduceSize = searchRequest.getBatchedReduceSize();

        Task task = mock(Task.class);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionFilterChain<SearchRequest, ActionResponse> chain = mock(ActionFilterChain.class);

        // Execute
        filter.apply(task, SearchAction.NAME, searchRequest, listener, chain);

        // Verify batch reduce size was not changed
        assertEquals(originalBatchReduceSize, searchRequest.getBatchedReduceSize());

        // Verify chain.proceed was called
        verify(chain).proceed(eq(task), eq(SearchAction.NAME), eq(searchRequest), eq(listener));
    }

    @SuppressWarnings("unchecked")
    public void testApply_whenNullQuery_thenDoesNotModifyRequest() {
        // Setup with source but null query
        SearchRequest searchRequest = new SearchRequest("test_index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // query is null
        searchRequest.source(sourceBuilder);

        int originalBatchReduceSize = searchRequest.getBatchedReduceSize();

        Task task = mock(Task.class);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionFilterChain<SearchRequest, ActionResponse> chain = mock(ActionFilterChain.class);

        // Execute
        filter.apply(task, SearchAction.NAME, searchRequest, listener, chain);

        // Verify batch reduce size was not changed
        assertEquals(originalBatchReduceSize, searchRequest.getBatchedReduceSize());

        // Verify chain.proceed was called
        verify(chain).proceed(eq(task), eq(SearchAction.NAME), eq(searchRequest), eq(listener));
    }

    @SuppressWarnings("unchecked")
    public void testApply_whenNonSearchAction_thenDoesNotModifyRequest() {
        // Setup with non-search action (e.g., bulk)
        BulkRequest bulkRequest = new BulkRequest();

        Task task = mock(Task.class);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionFilterChain<BulkRequest, ActionResponse> chain = mock(ActionFilterChain.class);

        // Execute with bulk action
        filter.apply(task, BulkAction.NAME, bulkRequest, listener, chain);

        // Verify chain.proceed was called (request passed through unchanged)
        verify(chain).proceed(eq(task), eq(BulkAction.NAME), eq(bulkRequest), eq(listener));
    }

    @SuppressWarnings("unchecked")
    public void testApply_whenMatchAllQuery_thenDoesNotModifyBatchReduceSize() {
        // Setup with match_all query (not hybrid)
        SearchRequest searchRequest = new SearchRequest("test_index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(new MatchAllQueryBuilder());
        searchRequest.source(sourceBuilder);

        int originalBatchReduceSize = searchRequest.getBatchedReduceSize();

        Task task = mock(Task.class);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionFilterChain<SearchRequest, ActionResponse> chain = mock(ActionFilterChain.class);

        // Execute
        filter.apply(task, SearchAction.NAME, searchRequest, listener, chain);

        // Verify batch reduce size was not changed
        assertEquals(originalBatchReduceSize, searchRequest.getBatchedReduceSize());

        // Verify chain.proceed was called
        verify(chain).proceed(eq(task), eq(SearchAction.NAME), eq(searchRequest), eq(listener));
    }

    @SuppressWarnings("unchecked")
    public void testApply_whenHybridQueryWithSmallBatchReduceSize_thenOverridesUserConfig() {
        // Setup - user explicitly set batchReduceSize to a small value that would cause failures
        HybridQueryBuilder hybridQuery = new HybridQueryBuilder();
        hybridQuery.add(new MatchQueryBuilder("field", "value"));

        SearchRequest searchRequest = new SearchRequest("test_index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(hybridQuery);
        searchRequest.source(sourceBuilder);
        searchRequest.setBatchedReduceSize(100); // small value that would cause hybrid query to fail

        Task task = mock(Task.class);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionFilterChain<SearchRequest, ActionResponse> chain = mock(ActionFilterChain.class);

        // Execute
        filter.apply(task, SearchAction.NAME, searchRequest, listener, chain);

        // Verify batch reduce size was overridden - hybrid queries don't honor this setting
        assertEquals(Integer.MAX_VALUE, searchRequest.getBatchedReduceSize());

        // Verify chain.proceed was called
        verify(chain).proceed(eq(task), eq(SearchAction.NAME), eq(searchRequest), eq(listener));
    }

    @SuppressWarnings("unchecked")
    public void testApply_whenSearchActionNameButNotSearchRequestType_thenPassesThrough() {
        // Setup - edge case where action name is SearchAction but request is not SearchRequest
        // This tests the "request instanceof SearchRequest" check
        BulkRequest bulkRequest = new BulkRequest();

        Task task = mock(Task.class);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionFilterChain<BulkRequest, ActionResponse> chain = mock(ActionFilterChain.class);

        // Execute with search action name but non-search request type
        // This is an edge case that shouldn't happen in normal operation but tests the instanceof check
        filter.apply(task, SearchAction.NAME, bulkRequest, listener, chain);

        // Verify chain.proceed was called (request passed through unchanged)
        verify(chain).proceed(eq(task), eq(SearchAction.NAME), eq(bulkRequest), eq(listener));
    }

    @SuppressWarnings("unchecked")
    public void testApply_whenEmptyHybridQuery_thenDisablesBatchedReduction() {
        // Setup - hybrid query with no sub-queries (edge case)
        HybridQueryBuilder hybridQuery = new HybridQueryBuilder();
        // Note: HybridQueryBuilder can exist without sub-queries

        SearchRequest searchRequest = new SearchRequest("test_index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(hybridQuery);
        searchRequest.source(sourceBuilder);

        // Verify default batch reduce size before filter
        assertEquals(SearchRequest.DEFAULT_BATCHED_REDUCE_SIZE, searchRequest.getBatchedReduceSize());

        Task task = mock(Task.class);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionFilterChain<SearchRequest, ActionResponse> chain = mock(ActionFilterChain.class);

        // Execute
        filter.apply(task, SearchAction.NAME, searchRequest, listener, chain);

        // Verify batch reduce size was changed to MAX_VALUE (still a hybrid query even if empty)
        assertEquals(Integer.MAX_VALUE, searchRequest.getBatchedReduceSize());
        verify(chain).proceed(eq(task), eq(SearchAction.NAME), eq(searchRequest), eq(listener));
    }
}
