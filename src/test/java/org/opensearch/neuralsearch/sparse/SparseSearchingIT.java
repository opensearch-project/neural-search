/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import org.junit.Before;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.sparse.query.SparseAnnQueryBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SEISMIC;

/**
 * Integration tests for sparse index search feature
 */
public class SparseSearchingIT extends SparseBaseIT {

    private static final String TEST_INDEX_NAME = "test-sparse-index";
    private static final String TEST_SPARSE_FIELD_NAME = "sparse_field";
    private static final String TEST_TEXT_FIELD_NAME = "text";
    private static final String PIPELINE_NAME = "seismic_test_pipeline";

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testIngestDocumentsAllSeismicPostingPruning() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 4, 0.4f, 0.5f, 8);

        ingestDocumentsAndForceMerge(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.1f, "2000", 0.1f),
                Map.of("1000", 0.2f, "2000", 0.2f),
                Map.of("1000", 0.3f, "2000", 0.3f),
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            )
        );

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(4, getHitCount(searchResults));
        List<String> actualIds = getDocIDs(searchResults);
        assertEquals(List.of("8", "7", "6", "5"), actualIds);
    }

    public void testIngestDocumentsMixSeismicWithRankFeatures() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 4, 0.4f, 0.5f, 4);

        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(Map.of("1000", 0.1f, "2000", 0.1f), Map.of("1000", 0.2f, "2000", 0.2f), Map.of("1000", 0.3f, "2000", 0.3f)),
            null,
            1
        );
        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            ),
            null,
            4
        );

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(7, getHitCount(searchResults));
        List<String> actualIds = getDocIDs(searchResults);
        assertEquals(List.of("8", "7", "6", "5", "3", "2", "1"), actualIds);
    }

    public void testIngestDocumentsWithAllRankFeatures() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 4, 0.4f, 0.5f, 100);

        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(Map.of("1000", 0.1f, "2000", 0.1f), Map.of("1000", 0.2f, "2000", 0.2f), Map.of("1000", 0.3f, "2000", 0.3f)),
            null,
            1
        );
        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(Map.of("1000", 0.4f, "2000", 0.4f), Map.of("1000", 0.5f, "2000", 0.5f)),
            null,
            4
        );

        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(Map.of("1000", 0.6f, "2000", 0.6f), Map.of("1000", 0.7f, "2000", 0.7f), Map.of("1000", 0.8f, "2000", 0.8f)),
            null,
            6
        );

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(8, getHitCount(searchResults));
        List<String> actualIds = getDocIDs(searchResults);
        assertEquals(List.of("8", "7", "6", "5", "4", "3", "2", "1"), actualIds);
    }

    public void testIngestDocumentsAllSeismicWithCut() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 4, 0.4f, 0.5f, 8);

        ingestDocumentsAndForceMerge(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.1f, "2000", 0.1f),
                Map.of("1000", 0.2f, "2000", 0.2f),
                Map.of("1000", 0.3f, "2000", 0.3f),
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f),
                Map.of("3000", 0.0001f)
            )
        );

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            1,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f, "3000", 64.0f)
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(1, getHitCount(searchResults));
        List<String> actualIds = getDocIDs(searchResults);
        assertEquals(List.of("9"), actualIds);
    }

    public void testIngestDocumentsSeismicHeapFactor() throws Exception {
        final int docCount = 100;
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, docCount, 1.0f, 0.5f, docCount);

        List<Map<String, Float>> docs = new ArrayList<>();
        for (int i = 0; i < docCount; ++i) {
            Map<String, Float> tokens = new HashMap<>();
            tokens.put("1000", randomFloat());
            tokens.put("2000", randomFloat());
            tokens.put("3000", randomFloat());
            tokens.put("4000", randomFloat());
            tokens.put("5000", randomFloat());
            docs.add(tokens);
        }

        ingestDocumentsAndForceMerge(TEST_INDEX_NAME, TEST_TEXT_FIELD_NAME, TEST_SPARSE_FIELD_NAME, docs);

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            0.000001f,
            docCount,
            Map.of("1000", 0.12f, "2000", 0.64f, "3000", 0.87f, "4000", 0.53f)
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, docCount);
        assertNotNull(searchResults);
        assertTrue(getHitCount(searchResults) < docCount);

        neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            1,
            100000,
            docCount,
            Map.of("1000", 0.12f, "2000", 0.64f, "3000", 0.87f, "4000", 0.53f)
        );
        searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, docCount);
        assertNotNull(searchResults);
        assertEquals(docCount, getHitCount(searchResults));
    }

    public void testIngestDocumentsAllSeismicWithPreFiltering() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 8, 0.4f, 0.5f, 8);

        ingestDocumentsAndForceMerge(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.1f, "2000", 0.1f),
                Map.of("1000", 0.2f, "2000", 0.2f),
                Map.of("1000", 0.3f, "2000", 0.3f),
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            ),
            List.of("apple", "tree", "apple", "tree", "apple", "tree", "apple", "tree")
        );

        // filter apple
        BoolQueryBuilder filter = new BoolQueryBuilder();
        filter.must(new MatchQueryBuilder(TEST_TEXT_FIELD_NAME, "apple"));

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f),
            filter
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(4, getHitCount(searchResults));
        List<String> actualIds = getDocIDs(searchResults);
        assertEquals(List.of("7", "5", "3", "1"), actualIds);
        // filter tree
        filter = new BoolQueryBuilder();
        filter.must(new MatchQueryBuilder(TEST_TEXT_FIELD_NAME, "tree"));
        neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f),
            filter
        );

        searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(4, getHitCount(searchResults));
        actualIds = getDocIDs(searchResults);
        assertEquals(List.of("8", "6", "4", "2"), actualIds);
    }

    public void testIngestDocumentsAllSeismicWithPostFiltering() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 8, 0.4f, 0.5f, 8);

        ingestDocumentsAndForceMerge(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.1f, "2000", 0.1f),
                Map.of("1000", 0.2f, "2000", 0.2f),
                Map.of("1000", 0.3f, "2000", 0.3f),
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            ),
            List.of("apple", "apple", "apple", "apple", "apple", "apple", "apple", "tree")
        );

        // filter apple
        BoolQueryBuilder filter = new BoolQueryBuilder();
        filter.must(new MatchQueryBuilder(TEST_TEXT_FIELD_NAME, "apple"));

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            4,
            Map.of("1000", 0.1f, "2000", 0.2f),
            filter
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(3, getHitCount(searchResults));
        List<String> actualIds = getDocIDs(searchResults);
        // results with k = 4 are 5, 6, 7, 8, filter results are 1, 2, 3, 4, 5, 6, 7
        // intersection of both are 5, 6, 7
        assertEquals(List.of("7", "6", "5"), actualIds);
    }

    public void testIngestDocumentsRankFeaturesWithFiltering() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 1, 0.4f, 0.5f, 100);

        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.1f, "2000", 0.1f),
                Map.of("1000", 0.2f, "2000", 0.2f),
                Map.of("1000", 0.3f, "2000", 0.3f),
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            ),
            List.of("apple", "tree", "apple", "tree", "apple", "tree", "apple", "tree"),
            1
        );

        // filter apple
        BoolQueryBuilder filter = new BoolQueryBuilder();
        filter.must(new MatchQueryBuilder(TEST_TEXT_FIELD_NAME, "apple"));

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f),
            filter
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(4, getHitCount(searchResults));
        List<String> actualIds = getDocIDs(searchResults);
        assertEquals(List.of("7", "5", "3", "1"), actualIds);
    }

    public void testIngestDocumentsMultipleShards() throws Exception {
        int shards = 3;
        int docCount = 20;
        // effective number of replica is capped by the number of OpenSearch nodes minus 1
        int replicas = Math.min(3, getNodeCount() - 1);
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 5, 0.4f, 0.5f, docCount, shards, replicas);

        List<Map<String, Float>> docs = new ArrayList<>();
        List<String> text = new ArrayList<>();
        for (int i = 0; i < docCount; ++i) {
            Map<String, Float> tokens = new HashMap<>();
            tokens.put("1000", randomFloat());
            tokens.put("2000", randomFloat());
            tokens.put("3000", randomFloat());
            tokens.put("4000", randomFloat());
            tokens.put("5000", randomFloat());
            docs.add(tokens);
            if (i % 2 == 0) {
                text.add("apple");
            } else {
                text.add("tree");
            }
        }
        List<String> routingIds = generateUniqueRoutingIds(shards);
        for (int i = 0; i < shards; ++i) {
            ingestDocuments(TEST_INDEX_NAME, TEST_TEXT_FIELD_NAME, TEST_SPARSE_FIELD_NAME, docs, text, i * docCount + 1, routingIds.get(i));
        }

        forceMerge(TEST_INDEX_NAME);
        // wait until force merge complete
        waitForSegmentMerge(TEST_INDEX_NAME, shards, replicas);
        // there are replica segments
        assertEquals(shards * (replicas + 1), getSegmentCount(TEST_INDEX_NAME));

        // filter apple
        BoolQueryBuilder filter = new BoolQueryBuilder();
        filter.must(new MatchQueryBuilder(TEST_TEXT_FIELD_NAME, "apple"));

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            9,
            Map.of("1000", 0.1f),
            filter
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 20);
        assertNotNull(searchResults);
        assertTrue(getHitCount(searchResults) <= 15);
    }

    public void testSearchDocumentsWithTwoPhaseSearchProcessorThenThrowException() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 4, 0.4f, 0.5f, 8);

        ingestDocumentsAndForceMerge(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.1f, "2000", 0.1f),
                Map.of("1000", 0.2f, "2000", 0.2f),
                Map.of("1000", 0.3f, "2000", 0.3f),
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            )
        );

        String twoPhaseSearchPipeline = "two-phase-search-pipeline";
        createNeuralSparseTwoPhaseSearchProcessor(twoPhaseSearchPipeline);
        updateIndexSettings(TEST_INDEX_NAME, Settings.builder().put("index.search.default_pipeline", twoPhaseSearchPipeline));

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );

        Exception exception = assertThrows(Exception.class, () -> search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10));
        assert (exception.getMessage()
            .contains(String.format(Locale.ROOT, "Two phase search processor is not compatible with [%s] field for now", SEISMIC)));
    }

    public void testSeismicWithModelInferencing() throws Exception {
        String modelId = prepareSparseEncodingModel();
        String sparseFieldName = "title_sparse"; // configured in SparseEncodingPipelineConfiguration.json
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.SPARSE_ENCODING, 2);
        createSparseIndex(TEST_INDEX_NAME, sparseFieldName, 4, 0.4f, 0.5f, 8);
        String payload = prepareSparseBulkIngestPayload(
            TEST_INDEX_NAME,
            "title",
            null,
            List.of(),
            List.of("one", "two", "three", "four", "five", "six", "seven", "eight", "night", "ten"),
            1
        );
        bulkIngest(payload, PIPELINE_NAME);
        forceMerge(TEST_INDEX_NAME);
        waitForSegmentMerge(TEST_INDEX_NAME);

        assertEquals(10, getDocCount(TEST_INDEX_NAME));

        SparseAnnQueryBuilder annQueryBuilder = new SparseAnnQueryBuilder().queryCut(2).fieldName(sparseFieldName).heapFactor(1.0f).k(9);

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder().sparseAnnQueryBuilder(annQueryBuilder)
            .fieldName(sparseFieldName)
            .modelId(modelId)
            .queryText("one two");

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(2, getHitCount(searchResults));
        Set<String> actualIds = new HashSet<>(getDocIDs(searchResults));
        assertEquals(Set.of("1", "2"), actualIds);
    }

    private List<String> getDocIDs(Map<String, Object> searchResults) {
        Map<String, Object> hits1map = (Map<String, Object>) searchResults.get("hits");
        List<String> actualIds = new ArrayList<>();
        List<Object> hits1List = (List<Object>) hits1map.get("hits");
        for (Object hits1Object : hits1List) {
            Map<String, Object> mapObject = (Map<String, Object>) hits1Object;
            String id = mapObject.get("_id").toString();
            actualIds.add(id);
        }
        return actualIds;
    }
}
