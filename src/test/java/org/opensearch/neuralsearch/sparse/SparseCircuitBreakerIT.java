/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.SneakyThrows;
import org.junit.After;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.settings.NeuralSearchSettings;

import java.util.ArrayList;
import java.util.HashMap;
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
        // Create index and perform ingestion
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

        // Delete index, disable cache and then ingest again
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
        ingestDocumentsAndForceMerge(indexName, textField, sparseField, docs);

        // Verify that without cache, the search results remain the same
        return getTotalHits(search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10));
    }

    @SneakyThrows
    private List<Map<String, Float>> prepareIngestDocuments(int docCount) {
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

        return docs;
    }
}
