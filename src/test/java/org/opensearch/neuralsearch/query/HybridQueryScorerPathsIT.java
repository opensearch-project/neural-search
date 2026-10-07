/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregationValues;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregations;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getTotalHits;
import static org.opensearch.neuralsearch.util.TestUtils.RELATION_EQUAL_TO;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import lombok.SneakyThrows;

/**
 * Integration tests for the request shapes that score a hybrid query through {@link HybridQueryScorer}
 * instead of the bulk scorer. The default search path never builds a {@code HybridQueryScorer} at all -
 * {@code HybridScorerSupplier.bulkScorer()} returns a {@code HybridBulkScorer} which drives each raw
 * sub-scorer's own iterator. A caller only gets a real {@code Scorer} when it asks the weight for one,
 * which is what the profiler, an aggregation filter and a nested query do.
 * <p>
 * Every query here pairs such a caller with a sub-query that has a two-phase iterator - a bool with a
 * partial {@code minimum_should_match} over {@code match_phrase} clauses. {@code minimum_should_match}
 * must stay below the number of clauses: equal to it, the bool rewrites to a conjunction and the
 * two-phase path is never taken.
 * <p>
 * The documents are laid out in blocks of ascending document id so that the two sub-queries match
 * overlapping but differently placed sets: the bool sub-query matches the first 60% of the index, the
 * match sub-query the 50% starting at 40%, and the last 10% matches neither. The bool sub-query therefore
 * runs out of documents while the match sub-query still has 30% of the index to go, which keeps the two
 * sub-scorers on different documents over most of the iteration. That is the fixture
 * that surfaces <a href="https://github.com/opensearch-project/neural-search/issues/1946">#1946</a>: when
 * both sub-queries match the same documents, or when they exhaust together, the stale queue is
 * accidentally correct. Per-document score correctness is asserted at unit level in
 * {@code HybridQueryScorerTests}.
 */
public class HybridQueryScorerPathsIT extends BaseNeuralSearchIT {

    private static final String TEST_INDEX = "test-hybrid-scorer-paths-index";
    private static final String TEST_NESTED_INDEX = "test-hybrid-scorer-paths-nested-index";
    private static final String SEARCH_PIPELINE = "phase-results-hybrid-scorer-paths-pipeline";
    private static final String TITLE_FIELD = "title";
    private static final String BODY_FIELD = "body";
    private static final String NESTED_PATH = "chapters";
    private static final String AGGREGATION_NAME = "hybrid_filter";
    private static final int DOC_COUNT = 1000;
    private static final int NESTED_DOC_COUNT = 400;
    // the last tenth of each index matches neither sub-query, see titleAndBody
    private static final int MATCHING_DOC_COUNT = DOC_COUNT / 10 * 9;
    private static final int MATCHING_NESTED_DOC_COUNT = NESTED_DOC_COUNT / 10 * 9;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @SneakyThrows
    public void testHybridQuery_whenProfileEnabledAndSubQueryIsTwoPhase_thenAllMatchingDocsReturned() {
        initializeIndexIfNotExist();
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);

        String query = String.format(
            Locale.ROOT,
            "{ \"profile\": true, \"query\": %s }",
            hybridQueryWithTwoPhaseSubQuery(TITLE_FIELD, BODY_FIELD)
        );
        Map<String, Object> searchResponseAsMap = searchWithRawQuery(TEST_INDEX, query, 10, SEARCH_PIPELINE);

        assertEquals(MATCHING_DOC_COUNT, totalHitsValue(searchResponseAsMap));
        assertEquals(RELATION_EQUAL_TO, getTotalHits(searchResponseAsMap).get("relation"));
        assertTrue(getDocumentIds(searchResponseAsMap).stream().allMatch(docId -> matchesHybridQuery(docId, DOC_COUNT)));
        assertFalse(getProfileShards(searchResponseAsMap).isEmpty());
    }

    @SneakyThrows
    public void testHybridQuery_whenInsideAggregationFilterAndSubQueryIsTwoPhase_thenAllMatchingDocsCounted() {
        initializeIndexIfNotExist();

        // the hybrid query is not the top level query here, so it is scored by the aggregation's own weight and
        // normalization does not apply. doc_count comes straight out of the scorer's iteration, which is what
        // makes it a direct assertion on that iteration
        String query = String.format(
            Locale.ROOT,
            "{ \"query\": { \"match_all\": {} }, \"aggs\": { \"%s\": { \"filter\": %s } } }",
            AGGREGATION_NAME,
            hybridQueryWithTwoPhaseSubQuery(TITLE_FIELD, BODY_FIELD)
        );
        Map<String, Object> searchResponseAsMap = searchWithRawQuery(TEST_INDEX, query, 0, null);

        Map<String, Object> filterAggregation = getAggregationValues(getAggregations(searchResponseAsMap), AGGREGATION_NAME);
        assertEquals(MATCHING_DOC_COUNT, ((Number) filterAggregation.get("doc_count")).intValue());
    }

    @SneakyThrows
    public void testHybridQuery_whenInsideNestedQueryAndSubQueryIsTwoPhase_thenAllMatchingParentsReturned() {
        initializeNestedIndexIfNotExist();

        String query = String.format(
            Locale.ROOT,
            "{ \"query\": { \"nested\": { \"path\": \"%s\", \"query\": %s } } }",
            NESTED_PATH,
            hybridQueryWithTwoPhaseSubQuery(NESTED_PATH + "." + TITLE_FIELD, NESTED_PATH + "." + BODY_FIELD)
        );
        Map<String, Object> searchResponseAsMap = searchWithRawQuery(TEST_NESTED_INDEX, query, 10, null);

        assertEquals(MATCHING_NESTED_DOC_COUNT, totalHitsValue(searchResponseAsMap));
        assertEquals(RELATION_EQUAL_TO, getTotalHits(searchResponseAsMap).get("relation"));
        assertTrue(getDocumentIds(searchResponseAsMap).stream().allMatch(docId -> matchesHybridQuery(docId, NESTED_DOC_COUNT)));
    }

    /**
     * Hybrid of a bool sub-query with a partial minimum_should_match, which contributes the two-phase
     * iterator, and a plain match sub-query.
     */
    private String hybridQueryWithTwoPhaseSubQuery(final String titleField, final String bodyField) {
        return String.format(Locale.ROOT, """
            {
              "hybrid": {
                "queries": [
                  {
                    "bool": {
                      "should": [
                        { "match_phrase": { "%s": "apple banana" } },
                        { "match_phrase": { "%s": "banana elderberry" } },
                        { "match_phrase": { "%s": "cherry banana" } }
                      ],
                      "minimum_should_match": 2
                    }
                  },
                  { "match": { "%s": "common" } }
                ]
              }
            }""", titleField, titleField, titleField, bodyField);
    }

    /**
     * Ascending document ids fall into four blocks: the first 40% match the bool sub-query only, the next
     * 20% match both sub-queries, the next 30% the match sub-query only, and the last 10% match neither -
     * their title holds a single one of the three phrases, so the partial minimum_should_match of 2
     * excludes them. The bool sub-query is exhausted by the end of the second block, the match sub-query
     * only by the end of the third.
     */
    private static String[] titleAndBody(final int docId, final int docCount) {
        int block = docId * 10 / docCount;
        if (block < 4) {
            return new String[] { "apple banana elderberry", "rare" };
        }
        if (block < 6) {
            return new String[] { "cherry banana elderberry", "common" };
        }
        if (block < 9) {
            return new String[] { "durian fig grape", "common" };
        }
        return new String[] { "apple banana", "rare" };
    }

    private static String documentSource(final int docId, final int docCount) {
        String[] titleAndBody = titleAndBody(docId, docCount);
        return String.format(Locale.ROOT, "{ \"%s\": \"%s\", \"%s\": \"%s\" }", TITLE_FIELD, titleAndBody[0], BODY_FIELD, titleAndBody[1]);
    }

    private static boolean matchesHybridQuery(final String docId, final int docCount) {
        return Integer.parseInt(docId) * 10 / docCount < 9;
    }

    @SneakyThrows
    private void initializeIndexIfNotExist() {
        if (indexExists(TEST_INDEX)) {
            return;
        }
        createIndex(TEST_INDEX, """
            {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "properties": {
                        "title": { "type": "text" },
                        "body": { "type": "text" }
                    }
                }
            }""");

        StringBuilder payload = new StringBuilder();
        for (int docId = 0; docId < DOC_COUNT; docId++) {
            payload.append(String.format(Locale.ROOT, "{ \"index\": { \"_index\": \"%s\", \"_id\": \"%d\" } }%n", TEST_INDEX, docId));
            payload.append(documentSource(docId, DOC_COUNT)).append(System.lineSeparator());
        }
        bulkIngest(payload.toString(), null);
        forceMergeToSingleSegment(TEST_INDEX);
        assertEquals(DOC_COUNT, getDocCount(TEST_INDEX));
    }

    /**
     * Parents fall into the same four blocks. A parent matching both sub-queries does so through two
     * different children, so the sub-scorers are positioned on different child documents.
     */
    @SneakyThrows
    private void initializeNestedIndexIfNotExist() {
        if (indexExists(TEST_NESTED_INDEX)) {
            return;
        }
        createIndex(TEST_NESTED_INDEX, """
            {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "properties": {
                        "chapters": {
                            "type": "nested",
                            "properties": {
                                "title": { "type": "text" },
                                "body": { "type": "text" }
                            }
                        }
                    }
                }
            }""");

        StringBuilder payload = new StringBuilder();
        for (int docId = 0; docId < NESTED_DOC_COUNT; docId++) {
            payload.append(
                String.format(Locale.ROOT, "{ \"index\": { \"_index\": \"%s\", \"_id\": \"%d\" } }%n", TEST_NESTED_INDEX, docId)
            );
            int block = docId * 10 / NESTED_DOC_COUNT;
            // a parent in the "both sub-queries" block carries one child per sub-query rather than a single
            // child matching both, so the two sub-scorers are positioned on different child documents
            String chapters = block >= 4 && block < 6
                ? documentSource(0, NESTED_DOC_COUNT) + ", " + documentSource(NESTED_DOC_COUNT * 6 / 10, NESTED_DOC_COUNT)
                : documentSource(docId, NESTED_DOC_COUNT);
            payload.append(String.format(Locale.ROOT, "{ \"%s\": [ %s ] }%n", NESTED_PATH, chapters));
        }
        bulkIngest(payload.toString(), null);
        forceMergeToSingleSegment(TEST_NESTED_INDEX);
        assertEquals(NESTED_DOC_COUNT, getDocCount(TEST_NESTED_INDEX));
    }

    /**
     * A single segment holding the documents in id order is what makes the layout above, and with it the
     * divergence between the sub-scorers, deterministic.
     */
    @SneakyThrows
    private void forceMergeToSingleSegment(final String index) {
        makeRequest(client(), "POST", String.format(Locale.ROOT, "/%s/_forcemerge?max_num_segments=1", index), null, null, null);
    }

    /**
     * Neither an aggregation filter nor the profile flag can be expressed through the query builder based
     * search helpers of {@link BaseNeuralSearchIT}, so these tests send the request body as it is.
     */
    @SneakyThrows
    private Map<String, Object> searchWithRawQuery(
        final String index,
        final String query,
        final int resultSize,
        final String searchPipeline
    ) {
        Request request = new Request("POST", "/" + index + "/_search");
        request.setJsonEntity(query);
        request.addParameter("size", Integer.toString(resultSize));
        request.addParameter("search_type", "query_then_fetch");
        if (searchPipeline != null) {
            request.addParameter("search_pipeline", searchPipeline);
        }
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        String responseBody = EntityUtils.toString(response.getEntity());
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);
    }

    private int totalHitsValue(final Map<String, Object> searchResponseAsMap) {
        return ((Number) getTotalHits(searchResponseAsMap).get("value")).intValue();
    }

    @SuppressWarnings("unchecked")
    private List<String> getDocumentIds(final Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hitsMap.get("hits");
        return hitsList.stream().map(hit -> (String) hit.get("_id")).collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getProfileShards(final Map<String, Object> searchResponseAsMap) {
        Map<String, Object> profileMap = (Map<String, Object>) searchResponseAsMap.get("profile");
        return (List<Map<String, Object>>) profileMap.get("shards");
    }
}
