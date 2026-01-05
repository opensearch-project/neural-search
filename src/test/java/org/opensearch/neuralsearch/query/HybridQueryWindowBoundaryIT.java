/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.opensearch.neuralsearch.util.TestUtils.RELATION_EQUAL_TO;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.SneakyThrows;

import org.opensearch.index.query.QueryBuilders;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

public class HybridQueryWindowBoundaryIT extends BaseNeuralSearchIT {

    private static final String TEST_INDEX_NAME = "test-hybrid-window-boundary";
    private static final String TEXT_FIELD = "text";
    private static final int WINDOW_SIZE = 4096;
    private static final String SEARCH_TERM = "searchterm";
    private static final String SEARCH_PIPELINE = "phase-results-hybrid-pipeline";

    @SneakyThrows
    public void testHybridQuery_whenResultsSpanMultipleWindows_thenNoDocumentsDropped() {
        int totalDocs = 10000;

        initializeIndexIfNotExist(totalDocs);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);

        Map<String, Object> matchResponse = search(
            TEST_INDEX_NAME,
            QueryBuilders.matchQuery(TEXT_FIELD, SEARCH_TERM),
            null,
            totalDocs,
            Map.of("track_total_hits", "true"),
            null
        );
        Set<String> matchResults = getDocumentIds(matchResponse);
        Map<String, Object> matchTotal = getTotalHits(matchResponse);

        HybridQueryBuilder hybridQuery = new HybridQueryBuilder();
        hybridQuery.add(QueryBuilders.matchQuery(TEXT_FIELD, SEARCH_TERM));

        Map<String, Object> hybridResponse = search(
            TEST_INDEX_NAME,
            hybridQuery,
            null,
            totalDocs,
            Map.of("search_pipeline", SEARCH_PIPELINE, "track_total_hits", "true"),
            null
        );
        Set<String> hybridResults = getDocumentIds(hybridResponse);
        Map<String, Object> hybridTotal = getTotalHits(hybridResponse);

        // Verify document IDs match
        assertEquals(matchResults.size(), hybridResults.size());
        assertEquals(totalDocs, matchResults.size());

        // Verify window boundary documents are included
        assertTrue(hybridResults.contains(String.valueOf(WINDOW_SIZE - 1)));
        assertTrue(hybridResults.contains(String.valueOf(WINDOW_SIZE)));
        assertTrue(hybridResults.contains(String.valueOf(WINDOW_SIZE + 1)));

        // Verify total hits tracking returns same values for both queries
        assertNotNull(matchTotal.get("value"));
        assertNotNull(hybridTotal.get("value"));
        assertEquals(matchTotal.get("value"), hybridTotal.get("value"));
        assertEquals(totalDocs, matchTotal.get("value"));
        assertEquals(totalDocs, hybridTotal.get("value"));

        // Verify relation is "eq" for both
        assertNotNull(matchTotal.get("relation"));
        assertNotNull(hybridTotal.get("relation"));
        assertEquals(RELATION_EQUAL_TO, matchTotal.get("relation"));
        assertEquals(RELATION_EQUAL_TO, hybridTotal.get("relation"));
    }

    @SneakyThrows
    private void initializeIndexIfNotExist(int totalDocs) {
        if (!indexExists(TEST_INDEX_NAME)) {
            String indexMapping = """
                {
                    "mappings": {
                        "properties": {
                            "text": { "type": "text" }
                        }
                    }
                }""";

            createIndex(TEST_INDEX_NAME, indexMapping);

            for (int i = 0; i < totalDocs; i++) {
                String docContent = String.format(Locale.ROOT, "test document number %d contains %s", i, SEARCH_TERM);
                indexDocument(TEST_INDEX_NAME, String.valueOf(i), TEXT_FIELD, docContent);
            }
            refreshIndex(TEST_INDEX_NAME);
        }
    }

    private void refreshIndex(String indexName) throws Exception {
        makeRequest(client(), "POST", String.format(Locale.ROOT, "/%s/_refresh", indexName), null, null, null);
    }

    private void indexDocument(String index, String id, String field, String content) throws Exception {
        String doc = String.format(Locale.ROOT, "{\"%s\": \"%s\"}", field, content);
        makeRequest(client(), "PUT", String.format(Locale.ROOT, "/%s/_doc/%s", index, id), null, toHttpEntity(doc), null);
    }

    @SuppressWarnings("unchecked")
    private Set<String> getDocumentIds(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hitsMap.get("hits");

        return hitsList.stream().map(hit -> (String) hit.get("_id")).collect(Collectors.toSet());
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getTotalHits(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return (Map<String, Object>) hitsMap.get("total");
    }
}
