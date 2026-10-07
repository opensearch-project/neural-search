/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.rerank.context;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.query.HybridFusionQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.ext.RerankSearchExtBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@code query_text_path} resolves a path into the request source rendered as XContent, so it reads whatever query the
 * source holds at the time the response processor runs. In fused mode that is the substitute the coordinator self-erased
 * into, not the hybrid the path was written against — these pin that a path written for a classic hybrid keeps resolving.
 */
public class QueryContextSourceFetcherTests extends OpenSearchTestCase {

    private static final String HYBRID_LEG_PATH = "query.hybrid.queries.0.match.body.query";

    private QueryContextSourceFetcher fetcher;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        fetcher = new QueryContextSourceFetcher(clusterService);
    }

    public void testQueryTextPathResolvesThroughAFusedSubstitute() throws Exception {
        HybridQueryBuilder original = new HybridQueryBuilder();
        original.add(new MatchQueryBuilder("body", "dolphins"));

        Map<String, Object> context = fetch(requestWithQueryTextPath(fusedSubstitute(original)));
        assertEquals("dolphins", context.get(QueryContextSourceFetcher.QUERY_TEXT_FIELD));
    }

    /** Control: the same path against the classic hybrid, so the two are compared rather than asserted separately. */
    public void testQueryTextPathResolvesThroughAClassicHybrid() throws Exception {
        HybridQueryBuilder classic = new HybridQueryBuilder();
        classic.add(new MatchQueryBuilder("body", "dolphins"));

        Map<String, Object> context = fetch(requestWithQueryTextPath(classic));
        assertEquals("dolphins", context.get(QueryContextSourceFetcher.QUERY_TEXT_FIELD));
    }

    /** With nothing carried the substitute renders its own informational form, and the path no longer resolves. */
    public void testQueryTextPathFailsWhenTheSubstituteCarriesNoOriginal() {
        SearchRequest request = requestWithQueryTextPath(fusedSubstitute(null));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> fetch(request));
        assertEquals(QueryContextSourceFetcher.QUERY_TEXT_PATH_FIELD + " must point to a string field", exception.getMessage());
    }

    private Map<String, Object> fetch(SearchRequest request) throws Exception {
        // fetchContext hands every failure to the listener rather than throwing, so unwrap it here to keep the tests
        // asserting on the exception the request would actually fail with.
        Exception[] failure = new Exception[1];
        @SuppressWarnings("unchecked")
        Map<String, Object>[] result = new Map[1];
        fetcher.fetchContext(request, null, ActionListener.wrap(context -> result[0] = context, e -> failure[0] = e));
        if (failure[0] != null) {
            throw failure[0];
        }
        return result[0];
    }

    private static SearchRequest requestWithQueryTextPath(org.opensearch.index.query.QueryBuilder query) {
        SearchSourceBuilder source = new SearchSourceBuilder().query(query)
            .ext(
                List.of(
                    new RerankSearchExtBuilder(
                        new HashMap<>(
                            Map.of(
                                QueryContextSourceFetcher.NAME,
                                new HashMap<>(Map.of(QueryContextSourceFetcher.QUERY_TEXT_PATH_FIELD, HYBRID_LEG_PATH))
                            )
                        )
                    )
                )
            );
        return new SearchRequest().source(source);
    }

    private static HybridFusionQueryBuilder fusedSubstitute(HybridQueryBuilder original) {
        return new HybridFusionQueryBuilder(
            new String[] { "d1" },
            new String[] { "idx" },
            new float[] { 1.0f },
            List.of(),
            List.of(),
            List.of(),
            original
        );
    }
}
