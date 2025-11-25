/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.SneakyThrows;
import org.apache.commons.lang3.math.NumberUtils;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.SparseTestCommon;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.sparse.query.SparseAnnQueryBuilder;

import java.util.ArrayList;
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

    public void testSearchDocumentsAllSeismicPostingPruning() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 4, 0.4f, 0.5f, 8);

        ingestDocumentsAndForceMergeForSingleShard(
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

    public void testSearchDocumentsMixSeismicWithRankFeatures() throws Exception {
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

    public void testSearchDocumentsWithAllRankFeatures() throws Exception {
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

    public void testSearchDocumentsAllSeismicWithCut() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 4, 0.4f, 0.5f, 8);

        ingestDocumentsAndForceMergeForSingleShard(
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

    public void testSearchDocumentsSeismicHeapFactor() throws Exception {
        final int docCount = 100;
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, docCount, 1.0f, 0.5f, docCount);

        List<Map<String, Float>> docs = prepareIngestDocuments(docCount);

        ingestDocumentsAndForceMergeForSingleShard(TEST_INDEX_NAME, TEST_TEXT_FIELD_NAME, TEST_SPARSE_FIELD_NAME, docs);

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            0.000001f,
            docCount,
            Map.of("1000", 0.12f, "2000", 0.64f, "3000", 0.87f, "4000", 0.53f)
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, docCount);
        assertNotNull(searchResults);
        assertTrue(getHitCount(searchResults) <= docCount);

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

    public void testSearchDocumentsAllSeismicWithPreFiltering() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 8, 0.4f, 0.5f, 8);

        ingestDocumentsAndForceMergeForSingleShard(
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

    public void testSearchDocumentsAllSeismicWithPostFiltering() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 8, 0.4f, 0.5f, 8);

        ingestDocumentsAndForceMergeForSingleShard(
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

    public void testSearchDocumentsRankFeaturesWithFiltering() throws Exception {
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

    public void testSearchDocumentsMultipleShards() throws Exception {
        int shards = 3;
        int replicas = getEffectiveReplicaCount(3);
        int docCount = 20;
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 5, 0.4f, 0.5f, docCount, shards, replicas);

        List<Map<String, Float>> docs = prepareIngestDocuments(docCount);
        List<String> text = new ArrayList<>();
        for (int i = 0; i < docCount; ++i) {
            if (i % 2 == 0) {
                text.add("apple");
            } else {
                text.add("tree");
            }
        }
        List<String> routingIds = SparseTestCommon.generateUniqueRoutingIds(shards);
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

        ingestDocumentsAndForceMergeForSingleShard(
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

    public void testSearchSeismicWithModelInferencing() throws Exception {
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

    public void testQuerySeismicWithAnalyzer() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 4, 1.0f, 0.5f, 8);

        ingestDocumentsAndForceMergeForSingleShard(
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
                Map.of("1000", 0.7f, "7592", 0.7f), // "7592" is token id of "world"
                Map.of("2088", 0.8f, "2000", 0.8f)  // "2088" is token id of "hello"
            )
        );
        SparseAnnQueryBuilder annQueryBuilder = new SparseAnnQueryBuilder().queryCut(2)
            .fieldName(TEST_SPARSE_FIELD_NAME)
            .heapFactor(1.0f)
            .k(10);

        // inference result: {2088: 3.42, 7592: 6.94}, the "world" document should rank first
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder().sparseAnnQueryBuilder(annQueryBuilder)
            .fieldName(TEST_SPARSE_FIELD_NAME)
            .queryText("hello world")
            .searchAnalyzer("bert-uncased");

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(2, getHitCount(searchResults));
        List<String> actualIds = getDocIDs(searchResults);
        assertEquals(List.of("7", "8"), actualIds);
    }

    /**
     * Test searching across multiple seismic fields
     */
    public void testSearchWithMultipleSeismicFields() {
        String field1 = TEST_SPARSE_FIELD_NAME + "_1";
        String field2 = TEST_SPARSE_FIELD_NAME + "_2";

        createIndexWithMultipleSeismicFields(TEST_INDEX_NAME, List.of(field1, field2));

        // Index multiple documents with different token distributions
        addSparseEncodingDoc(
            TEST_INDEX_NAME,
            "1",
            List.of(field1, field2),
            List.of(Map.of("1000", 0.8f, "2000", 0.2f), Map.of("3000", 0.1f, "4000", 0.9f))
        );
        addSparseEncodingDoc(
            TEST_INDEX_NAME,
            "2",
            List.of(field1, field2),
            List.of(Map.of("1000", 0.3f, "2000", 0.7f), Map.of("3000", 0.6f, "4000", 0.4f))
        );
        addSparseEncodingDoc(
            TEST_INDEX_NAME,
            "3",
            List.of(field1, field2),
            List.of(Map.of("1000", 0.1f, "5000", 0.9f), Map.of("6000", 0.8f, "4000", 0.2f))
        );

        // Search on first field
        NeuralSparseQueryBuilder queryBuilder1 = getNeuralSparseQueryBuilder(field1, 2, 1.0f, 10, Map.of("1000", 0.5f, "2000", 0.5f));

        Map<String, Object> searchResults1 = search(TEST_INDEX_NAME, queryBuilder1, 10);
        assertNotNull(searchResults1);
        assertTrue(getHitCount(searchResults1) > 0);

        // Search on second field
        NeuralSparseQueryBuilder queryBuilder2 = getNeuralSparseQueryBuilder(field2, 2, 1.0f, 10, Map.of("3000", 0.5f, "4000", 0.5f));

        Map<String, Object> searchResults2 = search(TEST_INDEX_NAME, queryBuilder2, 10);
        assertNotNull(searchResults2);
        assertTrue(getHitCount(searchResults2) > 0);
    }

    @SneakyThrows
    public void testSearchWithCustomizedQuantizationCeil() {
        Settings indexSettings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.sparse", true)
            .build();
        String indexMappings = prepareIndexMapping(1, 0.4f, 0.1f, 1, 5.0f, 6.0f, TEST_SPARSE_FIELD_NAME);

        Request request = new Request("PUT", "/" + TEST_INDEX_NAME);
        String body = String.format(
            Locale.ROOT,
            "{\n" + "  \"settings\": %s,\n" + "  \"mappings\": %s\n" + "}",
            indexSettings,
            indexMappings
        );
        request.setJsonEntity(body);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        ingestDocumentsAndForceMergeForSingleShard(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(Map.of("1000", 4.0f, "2000", 5.0f, "3000", 6.0f))
        );

        NeuralSparseQueryBuilder neuralSparseQueryBuilder1 = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            1,
            1.0f,
            1,
            Map.of("1000", 5.0f)
        );

        NeuralSparseQueryBuilder neuralSparseQueryBuilder2 = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            1,
            1.0f,
            1,
            Map.of("1000", 6.0f)
        );

        NeuralSparseQueryBuilder neuralSparseQueryBuilder3 = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            1,
            1.0f,
            1,
            Map.of("1000", 7.0f)
        );

        NeuralSparseQueryBuilder neuralSparseQueryBuilder4 = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            1,
            1.0f,
            1,
            Map.of("2000", 5.0f)
        );

        NeuralSparseQueryBuilder neuralSparseQueryBuilder5 = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            1,
            1.0f,
            1,
            Map.of("2000", 6.0f)
        );

        NeuralSparseQueryBuilder neuralSparseQueryBuilder6 = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            1,
            1.0f,
            1,
            Map.of("2000", 7.0f)
        );

        NeuralSparseQueryBuilder neuralSparseQueryBuilder7 = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            1,
            1.0f,
            1,
            Map.of("3000", 5.0f)
        );

        NeuralSparseQueryBuilder neuralSparseQueryBuilder8 = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            1,
            1.0f,
            1,
            Map.of("3000", 6.0f)
        );

        NeuralSparseQueryBuilder neuralSparseQueryBuilder9 = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            1,
            1.0f,
            1,
            Map.of("3000", 7.0f)
        );

        verifyTopDocScoreExpected(TEST_INDEX_NAME, neuralSparseQueryBuilder1, 20.0f);
        verifyTopDocScoreExpected(TEST_INDEX_NAME, neuralSparseQueryBuilder2, 24.0f);
        verifyTopDocScoreExpected(TEST_INDEX_NAME, neuralSparseQueryBuilder3, 24.0f);
        verifyTopDocScoreExpected(TEST_INDEX_NAME, neuralSparseQueryBuilder4, 25.0f);
        verifyTopDocScoreExpected(TEST_INDEX_NAME, neuralSparseQueryBuilder5, 30.0f);
        verifyTopDocScoreExpected(TEST_INDEX_NAME, neuralSparseQueryBuilder6, 30.0f);
        verifyTopDocScoreExpected(TEST_INDEX_NAME, neuralSparseQueryBuilder7, 25.0f);
        verifyTopDocScoreExpected(TEST_INDEX_NAME, neuralSparseQueryBuilder8, 30.0f);
        verifyTopDocScoreExpected(TEST_INDEX_NAME, neuralSparseQueryBuilder9, 30.0f);
    }

    public void testSearchDocumentsWithNoMethodParameters() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 4, 0.4f, 0.5f, 8);

        ingestDocumentsAndForceMergeForSingleShard(
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

        // Create query without specifying method_parameters (sparseAnnQueryBuilder is null)
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_SPARSE_FIELD_NAME)
            .queryTokensMapSupplier(() -> Map.of("1000", 0.1f, "2000", 0.2f));

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(4, getHitCount(searchResults));
        List<String> actualIds = getDocIDs(searchResults);
        assertEquals(List.of("8", "7", "6", "5"), actualIds);
    }

    @SuppressWarnings("unchecked")
    private void verifyTopDocScoreExpected(String indexName, NeuralSparseQueryBuilder neuralSparseQueryBuilder, float expectedScore) {
        float deltaForScoreAssertion = 1f; // score delta needs to be bigger due to quantization
        Map<String, Object> searchResults1 = search(indexName, neuralSparseQueryBuilder, 1);
        assertNotNull(searchResults1);
        Map<String, Object> hitsMap = (Map<String, Object>) searchResults1.get("hits");
        List<Object> hitsList = (List<Object>) hitsMap.get("hits");
        for (Object hitsObject : hitsList) {
            Map<String, Object> mapObject = (Map<String, Object>) hitsObject;
            float score = NumberUtils.createFloat(mapObject.get("_score").toString());
            assertEquals(expectedScore, score, deltaForScoreAssertion);
        }
    }
}
