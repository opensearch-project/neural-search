/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.client.Request;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.search.sort.SortOrder;

import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_COMBINATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_NORMALIZATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.PARAM_NAME_WEIGHTS;
import static org.opensearch.neuralsearch.util.TestUtils.TEXT_EMBEDDING_PROCESSOR;
import static org.opensearch.neuralsearch.util.TestUtils.getModelId;

public class HybridSearchWithSearchAfterIT extends AbstractRollingUpgradeTestCase {

    private static final String PIPELINE_NAME = "nlp-hybrid-search-after-pipeline";
    private static final String SEARCH_PIPELINE_NAME = "nlp-hybrid-search-after-search-pipeline";
    private static final String TEST_FIELD = "passage_text";
    private static final String SORT_FIELD = "stock";
    private static final String VECTOR_EMBEDDING_FIELD = "passage_embedding";
    // fixed routing so all docs land on the same shard, keeping enough hits per shard for the
    // hybrid query's per-shard score combination to have sort field data to work with
    private static final String ROUTING_KEY = "search-after-routing";
    private static final String QUERY = "Hi world";
    private static final int QUERY_SIZE = 10;
    private static final List<String> TEXTS = List.of(
        "Hello world",
        "Hi planet",
        "Hi earth",
        "Hi amazon",
        "Hi mars",
        "Hi opensearch",
        "Hi neptune"
    );
    // stock value for doc with id i is (i + 1) * 10
    private static String modelId = "";

    // Test rolling-upgrade with hybrid query using sort and search_after (deep pagination)
    // Create Text Embedding Processor, Ingestion Pipeline, add documents with a numeric sort field,
    // and a search pipeline with normalization processor.
    // Validate that sort + search_after returns correctly ordered pages in mixed and upgraded clusters.
    public void testHybridSearchWithSearchAfter_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        switch (getClusterType()) {
            case OLD:
                modelId = uploadTextEmbeddingModel();
                createPipelineProcessor(modelId, PIPELINE_NAME);
                createIndexWithConfiguration(
                    getIndexNameForTest(),
                    Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
                    PIPELINE_NAME
                );
                // docs 0..4 with stock values 10, 20, 30, 40, 50
                for (int docId = 0; docId < 5; docId++) {
                    addDocumentWithSortField(getIndexNameForTest(), String.valueOf(docId), TEXTS.get(docId), (docId + 1) * 10);
                }
                createSearchPipeline(
                    SEARCH_PIPELINE_NAME,
                    DEFAULT_NORMALIZATION_METHOD,
                    DEFAULT_COMBINATION_METHOD,
                    Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.3f, 0.7f }))
                );
                break;
            case MIXED:
                modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                loadAndWaitForModelToBeReady(modelId);
                if (isFirstMixedRound()) {
                    // page after stock 45 out of [10, 20, 30, 40, 50]
                    validateSearchAfterQuery(5, 45, List.of(40, 30, 20, 10));
                    // doc 5 with stock value 60
                    addDocumentWithSortField(getIndexNameForTest(), "5", TEXTS.get(5), 60);
                } else {
                    // stock 60 is before the cursor and must be excluded from the page
                    validateSearchAfterQuery(6, 45, List.of(40, 30, 20, 10));
                    validateSearchAfterQuery(6, 65, List.of(60, 50, 40, 30, 20, 10));
                }
                break;
            case UPGRADED:
                try {
                    modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                    loadAndWaitForModelToBeReady(modelId);
                    // doc 6 with stock value 70
                    addDocumentWithSortField(getIndexNameForTest(), "6", TEXTS.get(6), 70);
                    validateSearchAfterQuery(7, 65, List.of(60, 50, 40, 30, 20, 10));
                    validateSearchAfterQuery(7, 35, List.of(30, 20, 10));
                } finally {
                    wipeOfTestResources(getIndexNameForTest(), PIPELINE_NAME, modelId, SEARCH_PIPELINE_NAME);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }
    }

    private void validateSearchAfterQuery(final int expectedDocCount, final int searchAfterValue, final List<Integer> expectedStockValues) {
        int docCount = getDocCount(getIndexNameForTest());
        assertEquals(expectedDocCount, docCount);

        Map<String, SortOrder> fieldSortOrderMap = new LinkedHashMap<>();
        fieldSortOrderMap.put(SORT_FIELD, SortOrder.DESC);
        List<Object> searchAfter = new ArrayList<>();
        searchAfter.add(searchAfterValue);

        Map<String, Object> searchResponseAsMap = search(
            getIndexNameForTest(),
            getQueryBuilder(modelId),
            null,
            QUERY_SIZE,
            Map.of("search_pipeline", SEARCH_PIPELINE_NAME),
            null,
            null,
            createSortBuilders(fieldSortOrderMap, false),
            false,
            searchAfter,
            0,
            null
        );
        assertNotNull(searchResponseAsMap);
        assertEquals(expectedStockValues.size(), getHitCount(searchResponseAsMap));
        List<Integer> actualStockValues = getStockValuesFromSortFields(searchResponseAsMap);
        assertEquals(expectedStockValues, actualStockValues);
    }

    @SuppressWarnings("unchecked")
    private List<Integer> getStockValuesFromSortFields(final Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hitsMap.get("hits");
        List<Integer> stockValues = new ArrayList<>();
        for (Map<String, Object> hit : hitsList) {
            List<Object> sortValues = (List<Object>) hit.get("sort");
            assertNotNull(sortValues);
            assertEquals(1, sortValues.size());
            stockValues.add(((Number) sortValues.get(0)).intValue());
        }
        return stockValues;
    }

    private void addDocumentWithSortField(final String index, final String docId, final String text, final int sortFieldValue)
        throws IOException {
        Request request = new Request("PUT", "/" + index + "/_doc/" + docId + "?refresh=true&routing=" + ROUTING_KEY);
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder.field(TEST_FIELD, text);
        builder.field(SORT_FIELD, sortFieldValue);
        builder.endObject();
        request.setJsonEntity(builder.toString());
        client().performRequest(request);
    }

    private HybridQueryBuilder getQueryBuilder(final String modelId) {
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(VECTOR_EMBEDDING_FIELD)
            .modelId(modelId)
            .queryText(QUERY)
            // k is intentionally larger than the total document count so that every document
            // is a candidate of the neural sub-query and pages have deterministic sizes
            .k(100)
            .build();

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(TEST_FIELD, QUERY);

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(matchQueryBuilder);
        hybridQueryBuilder.add(neuralQueryBuilder);
        return hybridQueryBuilder;
    }
}
