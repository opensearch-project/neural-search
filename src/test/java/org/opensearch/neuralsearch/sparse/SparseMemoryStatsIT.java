/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.SneakyThrows;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.junit.After;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.settings.NeuralSearchSettings;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.stats.metrics.MetricStatName;

import java.util.ArrayList;
import java.util.Arrays;
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
    private static final String SPARSE_MEMORY_USAGE_METRIC_NAME = MetricStatName.MEMORY_SPARSE_MEMORY_USAGE.getNameString();
    private static final String SPARSE_MEMORY_USAGE_METRIC_PATH = MetricStatName.MEMORY_SPARSE_MEMORY_USAGE.getFullPath();

    /**
     * Enable neural stats
     */
    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        // Only enable stats for stats related tests to prevent collisions
        updateClusterSettings(NeuralSearchSettings.NEURAL_STATS_ENABLED.getKey(), true);
    }

    /**
     * Resets circuit breaker and neural stats to default settings
     */
    @After
    @Override
    @SneakyThrows
    public void tearDown() {
        updateClusterSettings(NeuralSearchSettings.NEURAL_STATS_ENABLED.getKey(), false);
        updateClusterSettings(NeuralSearchSettings.NEURAL_CIRCUIT_BREAKER_LIMIT.getKey(), "50%");
        super.tearDown();
    }

    /**
     * Seismic features segment increases memory usage
     */
    @SneakyThrows
    public void testMemoryStatsIncreaseWithSeismic() {
        // Create Sparse Index
        int docCount = 100;
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, 0.4f, 0.1f, docCount);

        // Verify index exists
        assertTrue(indexExists(TEST_INDEX_NAME));

        // Fetch original memory stats
        long[] originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        long[] originalCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();
        assertArrayEquals(originalSparseMemoryUsageStats, originalCircuitBreakerMemoryStats);

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
        long[] currentSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        long[] currentCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();

        long originalSparseMemoryUsageSum = Arrays.stream(originalSparseMemoryUsageStats).sum();
        long originalCircuitBreakerMemoryStatsSum = Arrays.stream(originalCircuitBreakerMemoryStats).sum();
        long currentSparseMemoryUsageSum = Arrays.stream(currentSparseMemoryUsageStats).sum();
        long currentCircuitBreakerMemoryStatsSum = Arrays.stream(currentCircuitBreakerMemoryStats).sum();

        assertTrue(currentSparseMemoryUsageSum > originalSparseMemoryUsageSum);
        assertTrue(currentCircuitBreakerMemoryStatsSum > originalCircuitBreakerMemoryStatsSum);
        assertArrayEquals(currentSparseMemoryUsageStats, currentCircuitBreakerMemoryStats);

        // Verify memory stats are the same as the original after index deletion
        deleteIndex(TEST_INDEX_NAME);
        currentSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        currentCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();
        currentSparseMemoryUsageSum = Arrays.stream(currentSparseMemoryUsageStats).sum();
        currentCircuitBreakerMemoryStatsSum = Arrays.stream(currentCircuitBreakerMemoryStats).sum();
        assertEquals(originalSparseMemoryUsageSum, currentSparseMemoryUsageSum);
        assertEquals(originalCircuitBreakerMemoryStatsSum, currentCircuitBreakerMemoryStatsSum);
        assertArrayEquals(currentSparseMemoryUsageStats, currentCircuitBreakerMemoryStats);
    }

    /**
     * Seismic features segment increases memory usage with multiple shards
     */
    @SneakyThrows
    public void testMemoryStatsIncreaseWithSeismicAndMultiShard() {
        // Fetch original memory stats
        long[] originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        long[] originalCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();
        assertArrayEquals(originalSparseMemoryUsageStats, originalCircuitBreakerMemoryStats);

        // Create Sparse Index
        prepareMultiShardReplicasIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);

        // Verify memory stats increase after ingesting documents
        long[] currentSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        long[] currentCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();

        long originalSparseMemoryUsageSum = Arrays.stream(originalSparseMemoryUsageStats).sum();
        long originalCircuitBreakerMemoryStatsSum = Arrays.stream(originalCircuitBreakerMemoryStats).sum();
        long currentSparseMemoryUsageSum = Arrays.stream(currentSparseMemoryUsageStats).sum();
        long currentCircuitBreakerMemoryStatsSum = Arrays.stream(currentCircuitBreakerMemoryStats).sum();

        assertTrue(currentSparseMemoryUsageSum > originalSparseMemoryUsageSum);
        assertTrue(currentCircuitBreakerMemoryStatsSum > originalCircuitBreakerMemoryStatsSum);
        assertArrayEquals(currentSparseMemoryUsageStats, currentCircuitBreakerMemoryStats);

        // Verify memory stats are the same as the original after index deletion
        deleteIndex(TEST_INDEX_NAME);
        currentSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        currentCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();
        currentSparseMemoryUsageSum = Arrays.stream(currentSparseMemoryUsageStats).sum();
        currentCircuitBreakerMemoryStatsSum = Arrays.stream(currentCircuitBreakerMemoryStats).sum();
        assertEquals(originalSparseMemoryUsageSum, currentSparseMemoryUsageSum);
        assertEquals(originalCircuitBreakerMemoryStatsSum, currentCircuitBreakerMemoryStatsSum);
        assertArrayEquals(currentSparseMemoryUsageStats, currentCircuitBreakerMemoryStats);
    }

    /**
     * Rank features segment does not increase memory usage
     */
    @SneakyThrows
    public void testMemoryStatsDoNotIncreaseWithAllRankFeatures() {
        // Fetch original memory stats
        long[] originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        long[] originalCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();
        assertArrayEquals(originalSparseMemoryUsageStats, originalCircuitBreakerMemoryStats);

        // Create Sparse Index
        // int docCount = 100;
        // createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, 0.4f, 0.1f, docCount * 2);
        //
        // // Verify index exists
        // assertTrue(indexExists(TEST_INDEX_NAME));
        //
        //
        //
        // // Ingest documents
        // List<Map<String, Float>> docs = new ArrayList<>();
        // for (int i = 0; i < docCount; ++i) {
        // Map<String, Float> tokens = new HashMap<>();
        // tokens.put("1000", randomFloat());
        // tokens.put("2000", randomFloat());
        // tokens.put("3000", randomFloat());
        // tokens.put("4000", randomFloat());
        // tokens.put("5000", randomFloat());
        // docs.add(tokens);
        // }
        //
        // ingestDocumentsAndForceMerge(TEST_INDEX_NAME, TEST_TEXT_FIELD_NAME, TEST_SPARSE_FIELD_NAME, docs);

        prepareOnlyRankFeaturesIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);
        // Verify memory stats do not increase after ingesting documents
        long[] currentSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        long[] currentCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();

        long originalSparseMemoryUsageSum = Arrays.stream(originalSparseMemoryUsageStats).sum();
        long originalCircuitBreakerMemoryStatsSum = Arrays.stream(originalCircuitBreakerMemoryStats).sum();
        long currentSparseMemoryUsageSum = Arrays.stream(currentSparseMemoryUsageStats).sum();
        long currentCircuitBreakerMemoryStatsSum = Arrays.stream(currentCircuitBreakerMemoryStats).sum();

        assertEquals(currentSparseMemoryUsageSum, originalSparseMemoryUsageSum);
        assertEquals(currentCircuitBreakerMemoryStatsSum, originalCircuitBreakerMemoryStatsSum);
        assertArrayEquals(currentSparseMemoryUsageStats, currentCircuitBreakerMemoryStats);

        // Verify memory stats are the same as the original after index deletion
        deleteIndex(TEST_INDEX_NAME);
        currentSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        currentCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();
        currentSparseMemoryUsageSum = Arrays.stream(currentSparseMemoryUsageStats).sum();
        currentCircuitBreakerMemoryStatsSum = Arrays.stream(currentCircuitBreakerMemoryStats).sum();
        assertEquals(originalSparseMemoryUsageSum, currentSparseMemoryUsageSum);
        assertEquals(originalCircuitBreakerMemoryStatsSum, currentCircuitBreakerMemoryStatsSum);
        assertArrayEquals(currentSparseMemoryUsageStats, currentCircuitBreakerMemoryStats);
    }

    /**
     * Seismic features segment only increases registry size
     */
    @SneakyThrows
    public void testMemoryStatsOnlyIncreaseRegistrySizeWithZeroCircuitBreakerLimit() {
        // Disable cache by setting neural circuit breaker limit to zero
        updateClusterSettings(NeuralSearchSettings.NEURAL_CIRCUIT_BREAKER_LIMIT.getKey(), "0%");

        // Create Sparse Index
        int docCount = 100;
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, 0.4f, 0.1f, docCount);

        // Verify index exists
        assertTrue(indexExists(TEST_INDEX_NAME));

        // Fetch original memory stats
        long[] originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        long[] originalCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();
        assertArrayEquals(originalSparseMemoryUsageStats, originalCircuitBreakerMemoryStats);

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
        long[] currentSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        long[] currentCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();

        long originalSparseMemoryUsageSum = Arrays.stream(originalSparseMemoryUsageStats).sum();
        long originalCircuitBreakerMemoryStatsSum = Arrays.stream(originalCircuitBreakerMemoryStats).sum();
        long currentSparseMemoryUsageSum = Arrays.stream(currentSparseMemoryUsageStats).sum();
        long currentCircuitBreakerMemoryStatsSum = Arrays.stream(currentCircuitBreakerMemoryStats).sum();

        // Cache registry size consists of two cache keys, one array for forward index and one map for clustered posting
        CacheKey cacheKey = new CacheKey(TestsPrepareUtils.prepareSegmentInfo(), TestsPrepareUtils.prepareKeyFieldInfo());
        long cacheKeySize = RamUsageEstimator.shallowSizeOf(cacheKey);
        long emptyForwardIndexSize = RamUsageEstimator.shallowSizeOf(new AtomicReferenceArray<>(docCount));
        long emptyClusteredPostingSize = RamUsageEstimator.shallowSizeOf(new ConcurrentHashMap<>());

        long registrySize = cacheKeySize * 2 + emptyClusteredPostingSize + emptyForwardIndexSize;

        assertEquals(registrySize, currentSparseMemoryUsageSum - originalSparseMemoryUsageSum);
        assertEquals(registrySize, currentCircuitBreakerMemoryStatsSum - originalCircuitBreakerMemoryStatsSum);
        assertArrayEquals(currentSparseMemoryUsageStats, currentCircuitBreakerMemoryStats);

        // Verify memory stats are the same as the original after index deletion
        deleteIndex(TEST_INDEX_NAME);
        currentSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        currentCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();
        currentSparseMemoryUsageSum = Arrays.stream(currentSparseMemoryUsageStats).sum();
        currentCircuitBreakerMemoryStatsSum = Arrays.stream(currentCircuitBreakerMemoryStats).sum();
        assertEquals(originalSparseMemoryUsageSum, currentSparseMemoryUsageSum);
        assertEquals(originalCircuitBreakerMemoryStatsSum, currentCircuitBreakerMemoryStatsSum);
        assertArrayEquals(currentSparseMemoryUsageStats, currentCircuitBreakerMemoryStats);
    }

    @SneakyThrows
    private long[] getNeuralCircuitBreakerMemoryStatsAcrossNodes() {
        Request request = new Request("GET", "_nodes/stats/breaker/");

        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        String responseBody = EntityUtils.toString(response.getEntity());
        List<Map<String, Object>> nodeStatsResponseList = parseNodeStatsResponse(responseBody);

        List<Long> circuitBreakerStats = new ArrayList<>();
        for (Map<String, Object> nodeStatsResponse : nodeStatsResponseList) {
            // we do not use breakers.neural_search.estimated_size_in_bytes due to precision limitation by memory stats
            String stringValue = getNestedValue(nodeStatsResponse, "breakers.neural_search.estimated_size").toString();
            circuitBreakerStats.add(parseFractionalSize(stringValue));
        }
        return circuitBreakerStats.stream().mapToLong(Long::longValue).toArray();
    }
}
