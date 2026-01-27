/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.SneakyThrows;
import org.junit.After;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.settings.NeuralSearchSettings;

import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.util.TestUtils.getTotalHits;

public class SparseCircuitBreakerIT extends SparseBaseIT {

    private static final String TEST_INDEX_NAME = "test-sparse-circuit-breaker";
    private static final String TEST_TEXT_FIELD_NAME = "text";
    private static final String TEST_SPARSE_FIELD_NAME = "sparse_field";

    /**
     * Resets circuit breaker to default settings
     */
    @After
    @Override
    @SneakyThrows
    public void tearDown() {
        updateClusterSettings(NeuralSearchSettings.NEURAL_CIRCUIT_BREAKER_LIMIT.getKey(), "50%");
        super.tearDown();
    }

    /**
     * By setting circuit breaker limit to be zero, the cache will be disabled.
     * Given the same index, the Seismic query should return the same results with an empty cache
     */
    @SneakyThrows
    public void testQueryWithZeroCircuitBreakerLimit() {
        int docCount = 100;
        List<Map<String, Float>> docs = prepareIngestDocuments(docCount);
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );

        Map<String, Object> expectedHits = ingestSparseIndexAndSearch(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            100,
            0.4f,
            0.1f,
            100,
            docs,
            neuralSparseQueryBuilder
        );

        // Delete index, disable cache and then search the seismic index again
        deleteIndex(TEST_INDEX_NAME);
        updateClusterSettings(NeuralSearchSettings.NEURAL_CIRCUIT_BREAKER_LIMIT.getKey(), "0%");
        Map<String, Object> hits = ingestSparseIndexAndSearch(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            100,
            0.4f,
            0.1f,
            100,
            docs,
            neuralSparseQueryBuilder
        );

        assertEquals(expectedHits, hits);
    }

    /**
     * By setting circuit breaker limit to be zero, the LRU cache eviction will be enabled.
     * Given the same index, the Seismic query should return the same results with cache eviction
     */
    @SneakyThrows
    public void testQueryWithLRUEviction() {
        int docCount = 100;
        List<Map<String, Float>> docs1 = prepareIngestDocuments(docCount);
        List<Map<String, Float>> docs2 = prepareIngestDocuments(docCount);
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );

        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, 0.4f, 0.1f, 100);
        ingestDocumentsAndForceMergeForSingleShard(TEST_INDEX_NAME, TEST_TEXT_FIELD_NAME, TEST_SPARSE_FIELD_NAME, docs1);
        ingestDocumentsAndForceMergeForSingleShard(TEST_INDEX_NAME, TEST_TEXT_FIELD_NAME, TEST_SPARSE_FIELD_NAME, docs2);

        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
        Map<String, Object> expectedHits = getTotalHits(search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10));
        Map<String, Object> expectedIngestedDocuments = getTotalHits(search(TEST_INDEX_NAME, matchAllQueryBuilder, 200));

        // Delete index, ingest half documents and enable cache eviction by setting circuit breaker limit to zero
        deleteIndex(TEST_INDEX_NAME);
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, 0.4f, 0.1f, 100);
        ingestDocumentsAndForceMergeForSingleShard(TEST_INDEX_NAME, TEST_TEXT_FIELD_NAME, TEST_SPARSE_FIELD_NAME, docs1);
        updateClusterSettings(NeuralSearchSettings.NEURAL_CIRCUIT_BREAKER_LIMIT.getKey(), "0%");
        ingestDocumentsAndForceMergeForSingleShard(TEST_INDEX_NAME, TEST_TEXT_FIELD_NAME, TEST_SPARSE_FIELD_NAME, docs2);

        Map<String, Object> hits = getTotalHits(search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10));
        Map<String, Object> ingestedDocuments = getTotalHits(search(TEST_INDEX_NAME, matchAllQueryBuilder, 200));

        assertEquals(expectedHits, hits);
        assertEquals(expectedIngestedDocuments, ingestedDocuments);
    }

    @SneakyThrows
    private Map<String, Object> ingestSparseIndexAndSearch(
        String indexName,
        String textField,
        String sparseField,
        int nPostings,
        float alpha,
        float clusterRatio,
        int approximateThreshold,
        List<Map<String, Float>> docs,
        NeuralSparseQueryBuilder neuralSparseQueryBuilder
    ) {
        createSparseIndex(indexName, sparseField, nPostings, alpha, clusterRatio, approximateThreshold);
        ingestDocumentsAndForceMergeForSingleShard(indexName, textField, sparseField, docs);

        // Verify that without cache, the search results remain the same
        return getTotalHits(search(indexName, neuralSparseQueryBuilder, 10));
    }
}
