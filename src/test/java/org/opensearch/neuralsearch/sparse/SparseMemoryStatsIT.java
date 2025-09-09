/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.SneakyThrows;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.junit.After;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.settings.NeuralSearchSettings;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Integration tests for memory stats related features for Seismic algorithm
 */
public class SparseMemoryStatsIT extends SparseBaseIT {

    private static final String TEST_INDEX_NAME = "test-sparse-memory-stats";
    private static final String TEST_TEXT_FIELD_NAME = "text";
    private static final String TEST_SPARSE_FIELD_NAME = "sparse_field";
    private static final double DELTA_FOR_MEMORY_STATS_ASSERTION = 0.01d;

    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        // Only enable stats for stats related tests to prevent collisions
        updateClusterSettings(NeuralSearchSettings.NEURAL_STATS_ENABLED.getKey(), true);
    }

    @After
    @Override
    @SneakyThrows
    public void tearDown() {
        updateClusterSettings(NeuralSearchSettings.NEURAL_STATS_ENABLED.getKey(), false);
        updateClusterSettings(NeuralSearchSettings.NEURAL_CIRCUIT_BREAKER_LIMIT.getKey(), "50%");
        super.tearDown();
    }

    @SneakyThrows
    public void testMemoryStatsIncreaseWithSeismic() {
        // Create Sparse Index
        int docCount = 100;
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, 0.4f, 0.1f, docCount);

        // Verify index exists
        assertTrue(indexExists(TEST_INDEX_NAME));

        // Fetch original memory stats
        List<Double> originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        List<Long> originalCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();
        verityMemoryStatsAlign(originalSparseMemoryUsageStats, originalCircuitBreakerMemoryStats);

        // Ingest documents
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

        // Verify memory stats increase after ingesting documents
        List<Double> currentSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        List<Long> currentCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();

        double originalSparseMemoryUsageSum = originalSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        long originalCircuitBreakerMemoryStatsSum = originalCircuitBreakerMemoryStats.stream().mapToLong(Long::longValue).sum();
        double currentSparseMemoryUsageSum = currentSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        long currentCircuitBreakerMemoryStatsSum = currentCircuitBreakerMemoryStats.stream().mapToLong(Long::longValue).sum();

        assertTrue(currentSparseMemoryUsageSum > originalSparseMemoryUsageSum);
        assertTrue(currentCircuitBreakerMemoryStatsSum > originalCircuitBreakerMemoryStatsSum);
        verityMemoryStatsAlign(currentSparseMemoryUsageStats, currentCircuitBreakerMemoryStats);

        // Verify memory stats are the same as the original after index deletion
        deleteIndex(TEST_INDEX_NAME);
        currentSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        currentCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();
        currentSparseMemoryUsageSum = currentSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        currentCircuitBreakerMemoryStatsSum = currentCircuitBreakerMemoryStats.stream().mapToLong(Long::longValue).sum();
        assertEquals(originalSparseMemoryUsageSum, currentSparseMemoryUsageSum, DELTA_FOR_MEMORY_STATS_ASSERTION);
        assertEquals(originalCircuitBreakerMemoryStatsSum, currentCircuitBreakerMemoryStatsSum);
        verityMemoryStatsAlign(currentSparseMemoryUsageStats, currentCircuitBreakerMemoryStats);
    }

    @SneakyThrows
    public void testMemoryStatsIncreaseWithSeismicAndMultiShard() {
        // Fetch original memory stats
        List<Double> originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        List<Long> originalCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();
        verityMemoryStatsAlign(originalSparseMemoryUsageStats, originalCircuitBreakerMemoryStats);

        // Create Sparse Index
        prepareMultiShardReplicasIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);

        // Verify memory stats increase after ingesting documents
        List<Double> currentSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        List<Long> currentCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();

        double originalSparseMemoryUsageSum = originalSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        long originalCircuitBreakerMemoryStatsSum = originalCircuitBreakerMemoryStats.stream().mapToLong(Long::longValue).sum();
        double currentSparseMemoryUsageSum = currentSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        long currentCircuitBreakerMemoryStatsSum = currentCircuitBreakerMemoryStats.stream().mapToLong(Long::longValue).sum();

        assertTrue(currentSparseMemoryUsageSum > originalSparseMemoryUsageSum);
        assertTrue(currentCircuitBreakerMemoryStatsSum > originalCircuitBreakerMemoryStatsSum);
        verityMemoryStatsAlign(currentSparseMemoryUsageStats, currentCircuitBreakerMemoryStats);

        // Verify memory stats are the same as the original after index deletion
        deleteIndex(TEST_INDEX_NAME);
        currentSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        currentCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();
        currentSparseMemoryUsageSum = currentSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        currentCircuitBreakerMemoryStatsSum = currentCircuitBreakerMemoryStats.stream().mapToLong(Long::longValue).sum();
        assertEquals(originalSparseMemoryUsageSum, currentSparseMemoryUsageSum, DELTA_FOR_MEMORY_STATS_ASSERTION);
        assertEquals(originalCircuitBreakerMemoryStatsSum, currentCircuitBreakerMemoryStatsSum);
        verityMemoryStatsAlign(currentSparseMemoryUsageStats, currentCircuitBreakerMemoryStats);
    }

    @SneakyThrows
    public void testMemoryStatsDoNotIncreaseWithAllRankFeatures() {
        // Fetch original memory stats
        List<Double> originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        List<Long> originalCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();
        verityMemoryStatsAlign(originalSparseMemoryUsageStats, originalCircuitBreakerMemoryStats);

        prepareOnlyRankFeaturesIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);

        // Verify memory stats do not increase after ingesting documents
        List<Double> currentSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        List<Long> currentCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();

        double originalSparseMemoryUsageSum = originalSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        long originalCircuitBreakerMemoryStatsSum = originalCircuitBreakerMemoryStats.stream().mapToLong(Long::longValue).sum();
        double currentSparseMemoryUsageSum = currentSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        long currentCircuitBreakerMemoryStatsSum = currentCircuitBreakerMemoryStats.stream().mapToLong(Long::longValue).sum();

        assertEquals(currentSparseMemoryUsageSum, originalSparseMemoryUsageSum, DELTA_FOR_MEMORY_STATS_ASSERTION);
        assertEquals(currentCircuitBreakerMemoryStatsSum, originalCircuitBreakerMemoryStatsSum);
        verityMemoryStatsAlign(currentSparseMemoryUsageStats, currentCircuitBreakerMemoryStats);

        // Verify memory stats are the same as the original after index deletion
        deleteIndex(TEST_INDEX_NAME);
        currentSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        currentCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();
        currentSparseMemoryUsageSum = currentSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        currentCircuitBreakerMemoryStatsSum = currentCircuitBreakerMemoryStats.stream().mapToLong(Long::longValue).sum();
        assertEquals(originalSparseMemoryUsageSum, currentSparseMemoryUsageSum, DELTA_FOR_MEMORY_STATS_ASSERTION);
        assertEquals(originalCircuitBreakerMemoryStatsSum, currentCircuitBreakerMemoryStatsSum);
        verityMemoryStatsAlign(currentSparseMemoryUsageStats, currentCircuitBreakerMemoryStats);
    }

    @SneakyThrows
    public void testMemoryStatsIncreaseMinimalSizeWithZeroCircuitBreakerLimit() {
        // Disable cache by setting neural circuit breaker limit to zero
        updateClusterSettings(NeuralSearchSettings.NEURAL_CIRCUIT_BREAKER_LIMIT.getKey(), "0%");

        // Create Sparse Index
        int docCount = 100;
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, 0.4f, 0.1f, docCount);

        // Verify index exists
        assertTrue(indexExists(TEST_INDEX_NAME));

        // Fetch original memory stats
        List<Double> originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        List<Long> originalCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();
        verityMemoryStatsAlign(originalSparseMemoryUsageStats, originalCircuitBreakerMemoryStats);

        // Ingest documents
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

        // Verify memory stats only increase by cache registry size
        List<Double> currentSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        List<Long> currentCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();

        double originalSparseMemoryUsageSum = originalSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        long originalCircuitBreakerMemoryStatsSum = originalCircuitBreakerMemoryStats.stream().mapToLong(Long::longValue).sum();
        double currentSparseMemoryUsageSum = currentSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        long currentCircuitBreakerMemoryStatsSum = currentCircuitBreakerMemoryStats.stream().mapToLong(Long::longValue).sum();

        // Increase size consists of two cache keys, one array for forward index and one map for clustered posting
        CacheKey cacheKey = new CacheKey(TestsPrepareUtils.prepareSegmentInfo(), TestsPrepareUtils.prepareKeyFieldInfo());
        long cacheKeySize = RamUsageEstimator.shallowSizeOf(cacheKey);
        long emptyForwardIndexSize = RamUsageEstimator.shallowSizeOf(new AtomicReferenceArray<>(docCount)) + RamUsageEstimator
            .alignObjectSize((long) docCount * RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        long emptyClusteredPostingSize = RamUsageEstimator.shallowSizeOf(new ConcurrentHashMap<>());

        double expectedSize = (cacheKeySize * 2 + emptyClusteredPostingSize + emptyForwardIndexSize) / 1024.0d;

        assertEquals(expectedSize, currentSparseMemoryUsageSum - originalSparseMemoryUsageSum, DELTA_FOR_MEMORY_STATS_ASSERTION);
        verityMemoryStatsAlign(currentSparseMemoryUsageStats, currentCircuitBreakerMemoryStats);

        // Verify memory stats are the same as the original after index deletion
        deleteIndex(TEST_INDEX_NAME);
        currentSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        currentCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();
        currentSparseMemoryUsageSum = currentSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        currentCircuitBreakerMemoryStatsSum = currentCircuitBreakerMemoryStats.stream().mapToLong(Long::longValue).sum();
        assertEquals(originalSparseMemoryUsageSum, currentSparseMemoryUsageSum, DELTA_FOR_MEMORY_STATS_ASSERTION);
        assertEquals(originalCircuitBreakerMemoryStatsSum, currentCircuitBreakerMemoryStatsSum);
        verityMemoryStatsAlign(currentSparseMemoryUsageStats, currentCircuitBreakerMemoryStats);
    }

    @SneakyThrows
    private List<Long> getNeuralCircuitBreakerMemoryStatsAcrossNodes() {
        Request request = new Request("GET", "_nodes/stats/breaker/");

        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        String responseBody = EntityUtils.toString(response.getEntity());
        List<Map<String, Object>> nodeStatsResponseList = parseNodeStatsResponse(responseBody);

        List<Long> circuitBreakerStats = new ArrayList<>();
        for (Map<String, Object> nodeStatsResponse : nodeStatsResponseList) {
            String stringValue = getNestedValue(nodeStatsResponse, "breakers.neural_search.estimated_size_in_bytes").toString();
            circuitBreakerStats.add(NumberUtils.createLong(stringValue));
        }
        return circuitBreakerStats;
    }

    private void verityMemoryStatsAlign(List<Double> memoryUsageStats, List<Long> circuitBreakerStats) {
        assertEquals(memoryUsageStats.size(), circuitBreakerStats.size());
        for (int i = 0; i < memoryUsageStats.size(); ++i) {
            double circuitBreakerKbSize = circuitBreakerStats.get(i) / 1024.0d;
            assertEquals(memoryUsageStats.get(i), circuitBreakerKbSize, DELTA_FOR_MEMORY_STATS_ASSERTION);
        }
    }
}
