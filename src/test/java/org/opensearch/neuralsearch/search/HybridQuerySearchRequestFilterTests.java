/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.search.explain.FusedDocExplanations;
import org.opensearch.neuralsearch.search.profile.FusedCoordinatorTimings;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.profile.NetworkTime;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.profile.aggregation.AggregationProfileShardResult;
import org.opensearch.search.profile.fetch.FetchProfileShardResult;
import org.opensearch.tasks.Task;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.neuralsearch.query.FusedWindowGuardRescorerBuilder;

public class HybridQuerySearchRequestFilterTests extends OpenSearchQueryTestCase {

    /** The shape a profile entry's key has: node, index, shard. Leg labels are inserted after it. */
    private static final String SHARD_KEY = "[node][index][0]";

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

    /**
     * The wiring that makes the fused rescore guard invisible to a user: a fused hybrid carrying a {@code rescore} comes
     * back from the shards with unranked documents demoted to {@code FusedWindowGuardRescorerBuilder.UNRANKED_SCORE}, and
     * this filter's response wrap maps that back to {@code 0.0} — the score those documents carry when the same request
     * has no rescore. Driven through the real listener the filter hands the chain, because the normalisation happens on
     * the response path.
     */
    @SuppressWarnings("unchecked")
    public void testApply_whenFusedHybridWithARescore_thenTheGuardSentinelIsNormalisedOutOfTheResponse() {
        HybridQueryBuilder fused = new HybridQueryBuilder();
        fused.add(new MatchQueryBuilder("field", "value"));
        fused.add(new MatchAllQueryBuilder());
        fused.fusion(new java.util.HashMap<>(java.util.Map.of("normalization", java.util.Map.of("technique", "min_max"))));

        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(fused).addRescorer(new QueryRescorerBuilder(new MatchAllQueryBuilder())));

        Task task = mock(Task.class);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionFilterChain<SearchRequest, ActionResponse> chain = mock(ActionFilterChain.class);

        filter.apply(task, SearchAction.NAME, searchRequest, listener, chain);

        ArgumentCaptor<ActionListener<ActionResponse>> wrapped = ArgumentCaptor.forClass(ActionListener.class);
        verify(chain).proceed(eq(task), eq(SearchAction.NAME), eq(searchRequest), wrapped.capture());
        assertNotSame("a fused request with a rescore must get a wrapped listener", listener, wrapped.getValue());

        wrapped.getValue().onResponse(responseWithScores(FusedWindowGuardRescorerBuilder.UNRANKED_SCORE, 3.0f));

        ArgumentCaptor<ActionResponse> delivered = ArgumentCaptor.forClass(ActionResponse.class);
        verify(listener).onResponse(delivered.capture());
        SearchHit[] hits = ((SearchResponse) delivered.getValue()).getInternalResponse().hits().getHits();
        assertEquals("the sentinel must never reach a response", 0.0f, hits[0].getScore(), 0.0f);
        assertEquals("a ranked score is untouched", 3.0f, hits[1].getScore(), 0.0f);
    }

    /** A fused hybrid with no rescore has no sentinel to normalise, so the caller's listener is handed straight through. */
    @SuppressWarnings("unchecked")
    public void testApply_whenFusedHybridWithoutARescore_thenTheListenerIsNotWrappedForNormalisation() {
        HybridQueryBuilder fused = new HybridQueryBuilder();
        fused.add(new MatchAllQueryBuilder());
        fused.fusion(new java.util.HashMap<>(java.util.Map.of("normalization", java.util.Map.of("technique", "min_max"))));

        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(fused));

        Task task = mock(Task.class);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionFilterChain<SearchRequest, ActionResponse> chain = mock(ActionFilterChain.class);

        filter.apply(task, SearchAction.NAME, searchRequest, listener, chain);

        verify(chain).proceed(eq(task), eq(SearchAction.NAME), eq(searchRequest), eq(listener));
    }

    private SearchResponse responseWithScores(final float... scores) {
        SearchHit[] hits = new SearchHit[scores.length];
        for (int i = 0; i < scores.length; i++) {
            hits[i] = new SearchHit(i, String.valueOf(i), null, null);
            hits[i].score(scores[i]);
        }
        SearchHits searchHits = new SearchHits(hits, new TotalHits(scores.length, TotalHits.Relation.EQUAL_TO), scores[0]);
        return new SearchResponse(
            new InternalSearchResponse(searchHits, null, null, null, false, null, 1),
            null,
            1,
            1,
            0,
            1L,
            new ShardSearchFailure[0],
            null,
            null
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

    /** Per-leg profiling attaches when the request asks for {@code profile} and carries a fused hybrid. */
    @SuppressWarnings("unchecked")
    public void testApply_whenProfiledFusedHybrid_thenLegProfilingIsAttached() {
        HybridQueryBuilder hybridQuery = fusedHybrid();

        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(hybridQuery).profile(true));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionListener<ActionResponse> proceeded = proceedListener(searchRequest, listener);

        assertNotNull("the fused hybrid must be handed somewhere to publish its leg trees", hybridQuery.legProfileConsumer());
        assertNotNull("and somewhere to publish what the coordinator itself spent", hybridQuery.fusionTimingConsumer());
        assertNotSame("and the response listener must be wrapped so those trees reach the response", listener, proceeded);
    }

    /** Without {@code profile} there is nothing to collect, so neither the query nor the listener is touched. */
    @SuppressWarnings("unchecked")
    public void testApply_whenFusedHybridIsNotProfiled_thenNothingIsAttached() {
        HybridQueryBuilder hybridQuery = fusedHybrid();

        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(hybridQuery));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        assertNull(hybridQuery.legProfileConsumer());
        assertNull(hybridQuery.fusionTimingConsumer());
        assertNull("and a request that cannot time out has no timeout to watch for either", hybridQuery.legTimeoutConsumer());
        assertSame(listener, proceedListener(searchRequest, listener));
    }

    /** The per-leg breakdown attaches when the request asks to be explained and the fused hybrid is its own query. */
    @SuppressWarnings("unchecked")
    public void testApply_whenAnExplainedFusedHybridIsTheRequestQuery_thenTheBreakdownIsAttached() {
        HybridQueryBuilder hybridQuery = fusedHybrid();

        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(hybridQuery).explain(true));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionListener<ActionResponse> proceeded = proceedListener(searchRequest, listener);

        assertNotNull("the legs have to have somewhere to publish what they explained", hybridQuery.fusedExplanationConsumer());
        assertNull("explain alone collects no leg trees", hybridQuery.legProfileConsumer());
        assertNotSame("and the listener has to be wrapped for the tree to reach the response", listener, proceeded);
    }

    /**
     * A hit's explanation is one tree describing that hit's score. For a fused hybrid nested inside a container the fused
     * score is an input to the enclosing query's scoring rather than the hit's score, so replacing the tree would describe
     * the wrong thing — the request keeps core's own explanation of the query round 2 ran.
     */
    @SuppressWarnings("unchecked")
    public void testApply_whenAnExplainedFusedHybridIsNestedInTheRequestQuery_thenNothingIsAttached() {
        HybridQueryBuilder hybridQuery = fusedHybrid();

        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(
            new SearchSourceBuilder().query(new BoolQueryBuilder().must(hybridQuery).filter(new MatchAllQueryBuilder())).explain(true)
        );

        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        assertNull("this hybrid's fused score is not the hit's score", hybridQuery.fusedExplanationConsumer());
        assertSame("and with nothing to report the listener is handed on as it came", listener, proceedListener(searchRequest, listener));
    }

    /**
     * The same reasoning, on the shape the identity check is actually written against: the finder stops at a fused hybrid,
     * so two of them found means neither can be the request's own query, and neither may claim the hit's score.
     */
    @SuppressWarnings("unchecked")
    public void testApply_whenTwoExplainedFusedHybridsAreSiblings_thenNothingIsAttached() {
        HybridQueryBuilder first = fusedHybrid();
        HybridQueryBuilder second = fusedHybrid();

        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(new BoolQueryBuilder().should(first).should(second)).explain(true));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        proceedListener(searchRequest, listener);

        assertNull(first.fusedExplanationConsumer());
        assertNull("neither sibling's fused score is the hit's score", second.fusedExplanationConsumer());
    }

    /**
     * {@code explain} writes onto the response's hits while {@code profile} replaces the response around them, so asking for
     * both has to yield both — that outcome is what this pins.
     *
     * <p>It does not pin the order, and cannot: the mergers run profile-first, so the explanation is written onto whatever
     * hits the rebuilt response carries, and a rebuild that copied them would receive the write on the copies and still
     * pass here. What makes the order safe is that the profile rebuild passes {@code SearchHits} through by reference,
     * which is pinned where it belongs, by the {@code assertSame} in {@code FusedLegProfileMergerTests}.
     */
    @SuppressWarnings("unchecked")
    public void testWrappedListener_whenExplainedAndProfiled_thenTheResponseCarriesBoth() {
        HybridQueryBuilder hybridQuery = fusedHybrid();
        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(hybridQuery).profile(true).explain(true));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionListener<ActionResponse> proceeded = proceedListener(searchRequest, listener);
        hybridQuery.legProfileConsumer().accept(0, Map.of(SHARD_KEY, legProfile()));

        FusedDocExplanations collected = new FusedDocExplanations();
        collected.combinationDescription("arithmetic_mean combination of:").normalizationDescription("min_max normalization of:");
        collected.addDocument(
            FusedDocExplanations.documentKey("test_index", "1"),
            0.75f,
            List.of(new FusedDocExplanations.LegContribution(0, 0.5f, Explanation.match(2.0f, "leg")))
        );
        hybridQuery.fusedExplanationConsumer().accept(collected);

        SearchResponse merged = merge(proceeded, listener, responseWithRankedHit());

        assertEquals(Set.of(SHARD_KEY + "[fused:hybrid_0.leg_0]"), merged.getProfileResults().keySet());
        Explanation explanation = merged.getHits().getHits()[0].getExplanation();
        assertNotNull("the breakdown must survive the rebuild the profile section needed", explanation);
        assertEquals("arithmetic_mean combination of:", explanation.getDescription());
    }

    /**
     * A soft {@code timeout} is reported whether or not the request asked for a diagnostic: a truncated leg narrows the
     * window the ranking comes out of, which is part of the answer rather than part of the instrumentation.
     */
    @SuppressWarnings("unchecked")
    public void testApply_whenAFusedHybridRequestSetsATimeout_thenTheTimeoutReportAttachesWithoutProfiling() {
        HybridQueryBuilder hybridQuery = fusedHybrid();

        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(hybridQuery).timeout(TimeValue.timeValueMillis(50)));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionListener<ActionResponse> proceeded = proceedListener(searchRequest, listener);

        assertNotNull("a leg can be cut short, so it must have somewhere to say so", hybridQuery.legTimeoutConsumer());
        assertNull("without profile there are still no leg trees to collect", hybridQuery.legProfileConsumer());
        assertNotSame("and the listener has to be wrapped for the flag to reach the response", listener, proceeded);
    }

    /** The flag the truncation sets is the response's own {@code timed_out}, on a request that asked for nothing else. */
    @SuppressWarnings("unchecked")
    public void testWrappedListener_whenALegTimedOut_thenTheResponseSaysSo() {
        HybridQueryBuilder hybridQuery = fusedHybrid();
        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(hybridQuery).timeout(TimeValue.timeValueMillis(50)));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionListener<ActionResponse> proceeded = proceedListener(searchRequest, listener);
        hybridQuery.legTimeoutConsumer().accept(true);

        SearchResponse merged = merge(proceeded, listener, responseWithoutProfile());

        assertTrue("the fusion ranked an incomplete candidate set, and the client has to be able to tell", merged.isTimedOut());
        assertTrue("and an unprofiled request must not grow a profile section on the way", merged.getProfileResults().isEmpty());
    }

    /** The overwhelmingly common case: the timeout was set, no leg hit it, and the response is handed back as it came. */
    @SuppressWarnings("unchecked")
    public void testWrappedListener_whenNoLegTimedOut_thenTheResponseIsNotRebuilt() {
        HybridQueryBuilder hybridQuery = fusedHybrid();
        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(hybridQuery).timeout(TimeValue.timeValueMillis(50)));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionListener<ActionResponse> proceeded = proceedListener(searchRequest, listener);
        hybridQuery.legTimeoutConsumer().accept(false);

        SearchResponse response = responseWithoutProfile();
        assertSame("nothing to report means nothing to rebuild", response, merge(proceeded, listener, response));
    }

    /**
     * Round 2 can time out on its own, entirely apart from the legs. The response's own flag is therefore an input to the
     * OR rather than something the fused report overwrites, and a fan-out that all completed must not clear it.
     */
    @SuppressWarnings("unchecked")
    public void testWrappedListener_whenRoundTwoTimedOutButNoLegDid_thenTheFlagIsNotCleared() {
        HybridQueryBuilder hybridQuery = fusedHybrid();
        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(hybridQuery).timeout(TimeValue.timeValueMillis(50)));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionListener<ActionResponse> proceeded = proceedListener(searchRequest, listener);
        hybridQuery.legTimeoutConsumer().accept(false);

        SearchResponse merged = merge(proceeded, listener, response(null, true));

        assertTrue("the search's own truncation is not the fused report's to undo", merged.isTimedOut());
    }

    /** Two reports on one request are one rebuild, so asking for both must not cost either of them. */
    @SuppressWarnings("unchecked")
    public void testWrappedListener_whenProfiledAndALegTimedOut_thenOneResponseCarriesBoth() {
        HybridQueryBuilder hybridQuery = fusedHybrid();
        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(hybridQuery).profile(true).timeout(TimeValue.timeValueMillis(50)));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionListener<ActionResponse> proceeded = proceedListener(searchRequest, listener);
        hybridQuery.legProfileConsumer().accept(1, Map.of(SHARD_KEY, legProfile()));
        hybridQuery.legTimeoutConsumer().accept(true);

        SearchResponse merged = merge(proceeded, listener, responseWithProfile(Map.of(SHARD_KEY, legProfile())));

        assertTrue(merged.isTimedOut());
        assertEquals(Set.of(SHARD_KEY + "[fused:rewrite]", SHARD_KEY + "[fused:hybrid_0.leg_1]"), merged.getProfileResults().keySet());
    }

    /** One truncated hybrid makes the whole answer incomplete, so a completed sibling must not talk it back down. */
    @SuppressWarnings("unchecked")
    public void testWrappedListener_whenOnlyOneOfTwoFusedHybridsTimedOut_thenTheResponseStillSaysSo() {
        HybridQueryBuilder first = fusedHybrid();
        HybridQueryBuilder second = fusedHybrid();

        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(
            new SearchSourceBuilder().query(new BoolQueryBuilder().should(first).should(second)).timeout(TimeValue.timeValueMillis(50))
        );

        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionListener<ActionResponse> proceeded = proceedListener(searchRequest, listener);
        first.legTimeoutConsumer().accept(true);
        second.legTimeoutConsumer().accept(false);

        assertTrue(merge(proceeded, listener, responseWithoutProfile()).isTimedOut());
    }

    /** A classic hybrid's sub-queries run inside the search phases, where core's own counters already see a timeout. */
    @SuppressWarnings("unchecked")
    public void testApply_whenAClassicHybridRequestSetsATimeout_thenNothingIsAttached() {
        HybridQueryBuilder hybridQuery = new HybridQueryBuilder();
        hybridQuery.add(new MatchQueryBuilder("field", "value"));

        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(hybridQuery).timeout(TimeValue.timeValueMillis(50)));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        assertSame("a classic hybrid times out as it always has", listener, proceedListener(searchRequest, listener));
        assertNull(hybridQuery.legTimeoutConsumer());
    }

    /**
     * The precondition of this seam, stated as a test. An ActionFilter runs <b>before</b> search request processors, so it
     * only ever sees the query as it arrived. A processor that <i>replaces</i> the query rather than mutating it — as
     * {@code AgenticQueryTranslatorProcessor} does — therefore produces a fused hybrid the filter never walked: leg
     * profiling silently does not attach, and the response carries round 2 only. Nothing breaks, the request still answers
     * correctly, the per-leg detail is simply missing.
     */
    @SuppressWarnings("unchecked")
    public void testApply_whenAProcessorCreatesTheFusedHybridAfterTheFilter_thenLegProfilingDoesNotAttach() {
        SearchRequest searchRequest = new SearchRequest("test_index");
        // What the user sent: no hybrid anywhere in it, but profiling asked for.
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value")).profile(true));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionListener<ActionResponse> proceeded = proceedListener(searchRequest, listener);
        assertSame("no hybrid was there to attach to", listener, proceeded);

        // What a request processor then does, downstream of every ActionFilter.
        HybridQueryBuilder createdByProcessor = fusedHybrid();
        searchRequest.source().query(createdByProcessor);

        assertNull("the filter cannot reach a hybrid that did not exist when it ran", createdByProcessor.legProfileConsumer());
    }

    /** A fused hybrid nested below the top level is still found: the finder walks the whole tree, not just the root. */
    @SuppressWarnings("unchecked")
    public void testApply_whenProfiledFusedHybridIsNested_thenLegProfilingIsAttached() {
        HybridQueryBuilder hybridQuery = fusedHybrid();

        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(
            new SearchSourceBuilder().query(new BoolQueryBuilder().must(hybridQuery).filter(new MatchAllQueryBuilder())).profile(true)
        );

        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionListener<ActionResponse> proceeded = proceedListener(searchRequest, listener);

        assertNotNull(hybridQuery.legProfileConsumer());
        assertNotSame(listener, proceeded);
    }

    /** A classic hybrid has no legs to profile, so a profiled classic request is left exactly as it arrives. */
    @SuppressWarnings("unchecked")
    public void testApply_whenClassicHybridIsProfiled_thenNothingIsAttached() {
        HybridQueryBuilder hybridQuery = new HybridQueryBuilder();
        hybridQuery.add(new MatchQueryBuilder("field", "value"));

        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(hybridQuery).profile(true));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        assertSame("a classic hybrid profiles as it always has", listener, proceedListener(searchRequest, listener));
        assertNull(hybridQuery.legProfileConsumer());
        assertEquals(
            "and the classic-only batched-reduce workaround still applies",
            Integer.MAX_VALUE,
            searchRequest.getBatchedReduceSize()
        );
    }

    /**
     * Only search actions are touched. Nothing else carries a {@link SearchRequest} today, but the gate is what keeps a
     * future action that does from having its listener silently wrapped by this filter.
     */
    @SuppressWarnings("unchecked")
    public void testApply_whenTheActionIsNotSearch_thenTheListenerIsNotWrapped() {
        HybridQueryBuilder hybridQuery = fusedHybrid();
        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(hybridQuery).profile(true));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionFilterChain<SearchRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        filter.apply(mock(Task.class), "indices:data/read/some_other_action", searchRequest, listener, chain);

        assertNull(hybridQuery.legProfileConsumer());
        verify(chain).proceed(any(), eq("indices:data/read/some_other_action"), eq(searchRequest), eq(listener));
    }

    /** Sibling fused hybrids are numbered in walk order, so their legs stay apart in the merged profile section. */
    @SuppressWarnings("unchecked")
    public void testApply_whenTwoFusedHybridsAreSiblings_thenTheirLegsAreLabelledApart() {
        HybridQueryBuilder first = fusedHybrid();
        HybridQueryBuilder second = fusedHybrid();

        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(new BoolQueryBuilder().should(first).should(second)).profile(true));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionListener<ActionResponse> proceeded = proceedListener(searchRequest, listener);

        first.legProfileConsumer().accept(0, Map.of(SHARD_KEY, legProfile()));
        second.legProfileConsumer().accept(0, Map.of(SHARD_KEY, legProfile()));

        assertEquals(
            Set.of(SHARD_KEY + "[fused:hybrid_0.leg_0]", SHARD_KEY + "[fused:hybrid_1.leg_0]"),
            merge(proceeded, listener, responseWithoutProfile()).getProfileResults().keySet()
        );
    }

    /**
     * A fused hybrid below another one is a leg of it, and a leg sub-search re-enters this filter and numbers its own legs
     * from {@code hybrid_0}. So the outer walk must stop at the outer hybrid: numbering the inner one too would burn an
     * index nothing ever emits, and would hand the same builder two consumers on the paths where the leg is not copied.
     */
    @SuppressWarnings("unchecked")
    public void testApply_whenAFusedHybridIsNestedInAFusedHybrid_thenOnlyTheOuterOneIsNumbered() {
        HybridQueryBuilder inner = fusedHybrid();
        HybridQueryBuilder outer = new HybridQueryBuilder();
        outer.add(inner);
        outer.add(new MatchAllQueryBuilder());
        outer.fusion(Map.of("window_size", 10));

        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(outer).profile(true));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        proceedListener(searchRequest, listener);

        assertNotNull("the hybrid this request fans out is numbered", outer.legProfileConsumer());
        assertNull("the one below it labels itself, from its own leg sub-search", inner.legProfileConsumer());
    }

    /** A classic hybrid is walked through rather than stopped at: it fans nothing out, and may still contain a fused one. */
    @SuppressWarnings("unchecked")
    public void testApply_whenAFusedHybridIsNestedInAClassicHybrid_thenItIsStillFound() {
        HybridQueryBuilder fused = fusedHybrid();
        HybridQueryBuilder classic = new HybridQueryBuilder();
        classic.add(fused);

        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(classic).profile(true));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        proceedListener(searchRequest, listener);

        assertNotNull(fused.legProfileConsumer());
        assertNull("and the classic wrapper has no legs of its own to profile", classic.legProfileConsumer());
    }

    /** The response the user gets carries the leg trees, with round 2 relabelled so it cannot read as the user's query. */
    @SuppressWarnings("unchecked")
    public void testWrappedListener_whenTheResponseComesBack_thenLegTreesAreMergedIntoIt() {
        HybridQueryBuilder hybridQuery = fusedHybrid();
        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(hybridQuery).profile(true));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionListener<ActionResponse> proceeded = proceedListener(searchRequest, listener);
        hybridQuery.legProfileConsumer().accept(1, Map.of(SHARD_KEY, legProfile()));

        SearchResponse merged = merge(proceeded, listener, responseWithProfile(Map.of(SHARD_KEY, legProfile())));

        assertEquals(Set.of(SHARD_KEY + "[fused:rewrite]", SHARD_KEY + "[fused:hybrid_0.leg_1]"), merged.getProfileResults().keySet());
    }

    /**
     * The two consumers a hybrid is handed have to name it the same way, or the fan-out cost and the legs it paid for would
     * read as belonging to different queries. Asserted through the merged response, which is where the labels are visible.
     */
    @SuppressWarnings("unchecked")
    public void testWrappedListener_whenTheResponseComesBack_thenTheCoordinatorEntryShareTheHybridsLabel() {
        HybridQueryBuilder first = fusedHybrid();
        HybridQueryBuilder second = fusedHybrid();

        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(new BoolQueryBuilder().should(first).should(second)).profile(true));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionListener<ActionResponse> proceeded = proceedListener(searchRequest, listener);

        first.legProfileConsumer().accept(0, Map.of(SHARD_KEY, legProfile()));
        first.fusionTimingConsumer().accept(new FusedCoordinatorTimings());
        second.fusionTimingConsumer().accept(new FusedCoordinatorTimings());

        assertEquals(
            Set.of(SHARD_KEY + "[fused:hybrid_0.leg_0]", "[coordinator][fused:hybrid_0]", "[coordinator][fused:hybrid_1]"),
            merge(proceeded, listener, responseWithoutProfile()).getProfileResults().keySet()
        );
    }

    /** Nothing else the chain can answer with is a search response, so anything else is handed on untouched. */
    @SuppressWarnings("unchecked")
    public void testWrappedListener_whenTheResponseIsNotASearchResponse_thenItIsPassedThrough() {
        HybridQueryBuilder hybridQuery = fusedHybrid();
        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(hybridQuery).profile(true));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionListener<ActionResponse> proceeded = proceedListener(searchRequest, listener);
        hybridQuery.legProfileConsumer().accept(0, Map.of(SHARD_KEY, legProfile()));

        ActionResponse notASearchResponse = mock(ActionResponse.class);
        proceeded.onResponse(notASearchResponse);

        verify(listener).onResponse(same(notASearchResponse));
    }

    /** A failure is the search's own and must arrive unchanged — there is no response to merge into. */
    @SuppressWarnings("unchecked")
    public void testWrappedListener_whenTheSearchFails_thenTheFailureIsForwarded() {
        HybridQueryBuilder hybridQuery = fusedHybrid();
        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(new SearchSourceBuilder().query(hybridQuery).profile(true));

        ActionListener<ActionResponse> listener = mock(ActionListener.class);
        ActionListener<ActionResponse> proceeded = proceedListener(searchRequest, listener);

        IllegalStateException failure = new IllegalStateException("leg fan-out failed");
        proceeded.onFailure(failure);

        verify(listener).onFailure(same(failure));
        verify(listener, never()).onResponse(any());
    }

    private HybridQueryBuilder fusedHybrid() {
        HybridQueryBuilder hybridQuery = new HybridQueryBuilder();
        hybridQuery.add(new MatchQueryBuilder("field", "value"));
        hybridQuery.add(new MatchAllQueryBuilder());
        hybridQuery.fusion(Map.of("window_size", 10));
        return hybridQuery;
    }

    /** Feeds {@code response} to the wrapped listener and returns what the caller's own listener was handed. */
    @SuppressWarnings("unchecked")
    private SearchResponse merge(
        final ActionListener<ActionResponse> proceeded,
        final ActionListener<ActionResponse> listener,
        final SearchResponse response
    ) {
        proceeded.onResponse(response);
        ArgumentCaptor<ActionResponse> captor = ArgumentCaptor.forClass(ActionResponse.class);
        verify(listener).onResponse(captor.capture());
        return (SearchResponse) captor.getValue();
    }

    private static ProfileShardResult legProfile() {
        return new ProfileShardResult(
            List.of(),
            new AggregationProfileShardResult(List.of()),
            new FetchProfileShardResult(List.of()),
            new NetworkTime(0, 0)
        );
    }

    private static SearchResponse responseWithoutProfile() {
        return responseWithProfile(null);
    }

    private static SearchResponse responseWithProfile(final Map<String, ProfileShardResult> profiles) {
        return response(profiles, false);
    }

    /** A response as the search phases built it, carrying the given profile section and its own {@code timed_out}. */
    private static SearchResponse response(final Map<String, ProfileShardResult> profiles, final boolean timedOut) {
        InternalSearchResponse sections = new InternalSearchResponse(
            SearchHits.empty(),
            InternalAggregations.EMPTY,
            null,
            Objects.isNull(profiles) ? null : new SearchProfileShardResults(profiles),
            timedOut,
            null,
            1
        );
        return new SearchResponse(sections, null, 1, 1, 0, 3L, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    /** A profiled response carrying one hit, for the reports that write onto the hits rather than around them. */
    private static SearchResponse responseWithRankedHit() {
        SearchHit hit = new SearchHit(0, "1", null, null);
        // The shard target is what gives a hit its _index, which is half of the key the breakdown is correlated by.
        hit.shard(new SearchShardTarget("node", new ShardId("test_index", "test_index-uuid", 0), null, OriginalIndices.NONE));
        hit.score(0.75f);
        SearchHits hits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 0.75f);
        InternalSearchResponse sections = new InternalSearchResponse(
            hits,
            InternalAggregations.EMPTY,
            null,
            new SearchProfileShardResults(Map.of()),
            false,
            null,
            1
        );
        return new SearchResponse(sections, null, 1, 1, 0, 3L, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    /** Runs the filter and returns the listener it handed down the chain. */
    @SuppressWarnings("unchecked")
    private ActionListener<ActionResponse> proceedListener(
        final SearchRequest searchRequest,
        final ActionListener<ActionResponse> listener
    ) {
        Task task = mock(Task.class);
        ActionFilterChain<SearchRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        filter.apply(task, SearchAction.NAME, searchRequest, listener, chain);
        ArgumentCaptor<ActionListener<ActionResponse>> captor = ArgumentCaptor.forClass(ActionListener.class);
        verify(chain).proceed(eq(task), eq(SearchAction.NAME), eq(searchRequest), captor.capture());
        return captor.getValue();
    }
}
