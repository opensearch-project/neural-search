/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.mockito.ArgumentCaptor;
import org.opensearch.Version;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.util.HybridQueryUtil;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.tasks.Task;

public class HybridQuerySearchRequestFilterTests extends OpenSearchQueryTestCase {

    private static final String TEST_INDEX = "test_index";

    private HybridQuerySearchRequestFilter filter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        filter = new HybridQuerySearchRequestFilter();
        // by default, resolve a default search pipeline for TEST_INDEX so existing tests that
        // don't care about pipeline resolution keep exercising the batched-reduce-size behavior
        setUpDefaultSearchPipeline(TEST_INDEX, "test-pipeline");
    }

    private void setUpDefaultSearchPipeline(String indexName, String defaultSearchPipelineId) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        if (defaultSearchPipelineId != null) {
            settingsBuilder.put(IndexSettings.DEFAULT_SEARCH_PIPELINE.getKey(), defaultSearchPipelineId);
        }
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settingsBuilder).build();
        Metadata metadata = Metadata.builder().put(indexMetadata, false).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        NeuralSearchClusterUtil.instance().initialize(clusterService, indexNameExpressionResolver);
    }

    public void testOrder_thenReturnsZero() {
        assertEquals(0, filter.order());
    }

    @SuppressWarnings("unchecked")
    public void testApply_whenHybridQueryWithDfsQueryThenFetchSearchType_thenFails() {
        HybridQueryBuilder hybridQuery = new HybridQueryBuilder();
        hybridQuery.add(new MatchQueryBuilder("field", "value"));

        SearchRequest searchRequest = new SearchRequest("test_index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(hybridQuery);
        searchRequest.source(sourceBuilder);
        searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);

        Task task = mock(Task.class);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionFilterChain<SearchRequest, ActionResponse> chain = mock(ActionFilterChain.class);

        filter.apply(task, SearchAction.NAME, searchRequest, listener, chain);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        verify(chain, never()).proceed(eq(task), eq(SearchAction.NAME), eq(searchRequest), eq(listener));
        assertTrue(exceptionCaptor.getValue() instanceof IllegalArgumentException);
        assertThat(
            exceptionCaptor.getValue().getMessage(),
            containsString("hybrid query does not support search_type [dfs_query_then_fetch]")
        );
    }

    @SuppressWarnings("unchecked")
    public void testApply_whenHybridQueryWithQueryThenFetchSearchType_thenDisablesBatchedReduction() {
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

    @SuppressWarnings("unchecked")
    public void testApply_whenHybridQueryWithNoResolvableSearchPipeline_thenFails() {
        // no default search pipeline configured on the index, no request-level pipeline set
        setUpDefaultSearchPipeline(TEST_INDEX, null);

        HybridQueryBuilder hybridQuery = new HybridQueryBuilder();
        hybridQuery.add(new MatchQueryBuilder("field", "value"));

        SearchRequest searchRequest = new SearchRequest(TEST_INDEX);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(hybridQuery);
        searchRequest.source(sourceBuilder);

        Task task = mock(Task.class);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionFilterChain<SearchRequest, ActionResponse> chain = mock(ActionFilterChain.class);

        filter.apply(task, SearchAction.NAME, searchRequest, listener, chain);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        verify(chain, never()).proceed(eq(task), eq(SearchAction.NAME), eq(searchRequest), eq(listener));
        assertTrue(exceptionCaptor.getValue() instanceof IllegalArgumentException);
        assertThat(exceptionCaptor.getValue().getMessage(), containsString(HybridQueryUtil.HYBRID_QUERY_REQUIRES_SEARCH_PIPELINE_MESSAGE));
    }

    @SuppressWarnings("unchecked")
    public void testApply_whenHybridQueryWithNoopRequestPipelineAndNoIndexDefault_thenFails() {
        // request explicitly disables the pipeline (search_pipeline=_none), and index has no default either
        setUpDefaultSearchPipeline(TEST_INDEX, null);

        HybridQueryBuilder hybridQuery = new HybridQueryBuilder();
        hybridQuery.add(new MatchQueryBuilder("field", "value"));

        SearchRequest searchRequest = new SearchRequest(TEST_INDEX);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(hybridQuery);
        searchRequest.source(sourceBuilder);
        searchRequest.pipeline(SearchPipelineService.NOOP_PIPELINE_ID);

        Task task = mock(Task.class);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionFilterChain<SearchRequest, ActionResponse> chain = mock(ActionFilterChain.class);

        filter.apply(task, SearchAction.NAME, searchRequest, listener, chain);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        verify(chain, never()).proceed(eq(task), eq(SearchAction.NAME), eq(searchRequest), eq(listener));
        assertThat(exceptionCaptor.getValue().getMessage(), containsString(HybridQueryUtil.HYBRID_QUERY_REQUIRES_SEARCH_PIPELINE_MESSAGE));
    }

    @SuppressWarnings("unchecked")
    public void testApply_whenHybridQueryWithRequestLevelPipelineAndNoIndexDefault_thenProceeds() {
        // no default search pipeline on the index, but the request explicitly names one
        setUpDefaultSearchPipeline(TEST_INDEX, null);

        HybridQueryBuilder hybridQuery = new HybridQueryBuilder();
        hybridQuery.add(new MatchQueryBuilder("field", "value"));

        SearchRequest searchRequest = new SearchRequest(TEST_INDEX);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(hybridQuery);
        searchRequest.source(sourceBuilder);
        searchRequest.pipeline("my-normalization-pipeline");

        Task task = mock(Task.class);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionFilterChain<SearchRequest, ActionResponse> chain = mock(ActionFilterChain.class);

        filter.apply(task, SearchAction.NAME, searchRequest, listener, chain);

        verify(listener, never()).onFailure(org.mockito.ArgumentMatchers.any());
        verify(chain).proceed(eq(task), eq(SearchAction.NAME), eq(searchRequest), eq(listener));
        assertEquals(Integer.MAX_VALUE, searchRequest.getBatchedReduceSize());
    }

    @SuppressWarnings("unchecked")
    public void testApply_whenHybridQueryWithIndexDefaultSearchPipeline_thenProceeds() {
        // index has a default search pipeline configured, request doesn't specify one
        setUpDefaultSearchPipeline(TEST_INDEX, "index-default-pipeline");

        HybridQueryBuilder hybridQuery = new HybridQueryBuilder();
        hybridQuery.add(new MatchQueryBuilder("field", "value"));

        SearchRequest searchRequest = new SearchRequest(TEST_INDEX);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(hybridQuery);
        searchRequest.source(sourceBuilder);

        Task task = mock(Task.class);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionFilterChain<SearchRequest, ActionResponse> chain = mock(ActionFilterChain.class);

        filter.apply(task, SearchAction.NAME, searchRequest, listener, chain);

        verify(listener, never()).onFailure(org.mockito.ArgumentMatchers.any());
        verify(chain).proceed(eq(task), eq(SearchAction.NAME), eq(searchRequest), eq(listener));
    }
}
