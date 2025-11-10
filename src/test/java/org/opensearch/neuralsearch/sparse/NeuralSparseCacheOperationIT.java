/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.SneakyThrows;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.common.xcontent.XContentType;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;

/**
 * Integration tests for neural sparse cache operations (warm up and clear cache)
 */
public class NeuralSparseCacheOperationIT extends SparseBaseIT {

    private static final String TEST_INDEX_NAME = "test-sparse-cache-index";
    private static final String TEST_TEXT_FIELD_NAME = "text";
    private static final String TEST_SPARSE_FIELD_NAME = "sparse_field";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        enableStats();
    }

    /**
     * Resets neural stats to default settings
     */
    @After
    @Override
    @SneakyThrows
    public void tearDown() {
        disableStats();
        super.tearDown();
    }

    /**
     * Test warm up cache API for sparse index with memory usage verification
     */
    @SneakyThrows
    public void testWarmUpCache() {
        // Create Sparse Index
        prepareSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);
        // First clear cache before warm up
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response clearCacheResponse = client().performRequest(clearCacheRequest);
        assertEquals(RestStatus.OK, RestStatus.fromCode(clearCacheResponse.getStatusLine().getStatusCode()));

        List<Double> originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double originalSparseMemoryUsageSum = originalSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();

        // Execute warm up cache request
        Request warmUpRequest = new Request("POST", "/_plugins/_neural/warmup/" + TEST_INDEX_NAME);
        Response warmUpResponse = client().performRequest(warmUpRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(warmUpResponse.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), warmUpResponse.getEntity().getContent()).map();

        assertNotNull(responseMap);
        assertEquals(Map.of("total", 1, "successful", 1, "failed", 0), responseMap.get("_shards"));

        // Verify memory usage increased after warm up
        List<Double> afterWarmUpSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double afterWarmUpSparseMemoryUsageSum = afterWarmUpSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        assertTrue("Memory usage should increase after warm up", afterWarmUpSparseMemoryUsageSum > originalSparseMemoryUsageSum);
        assertEquals(originalSparseMemoryUsageStats.size(), afterWarmUpSparseMemoryUsageStats.size());
        // In multi-node environment, only nodes with shards will have memory usage changes
        int nodesIncreaseMemoryNumber = 0;
        int nodesRemainMemoryNumber = 0;
        for (int i = 0; i < originalSparseMemoryUsageStats.size(); i++) {
            if (afterWarmUpSparseMemoryUsageStats.get(i) > originalSparseMemoryUsageStats.get(i)) {
                nodesIncreaseMemoryNumber += 1;
            }
            if (Objects.equals(afterWarmUpSparseMemoryUsageStats.get(i), originalSparseMemoryUsageStats.get(i))) {
                nodesRemainMemoryNumber += 1;
            }
        }
        assertEquals("One node should have increased memory usage after warm up", 1, nodesIncreaseMemoryNumber);
        assertEquals(
            "The nodes without shard should have remained the same memory usage after warm up",
            getNodeCount() - 1,
            nodesRemainMemoryNumber
        );
    }

    /**
     * Test clear cache API for sparse index with memory usage verification
     */
    @SneakyThrows
    public void testClearCache() {
        // Create Sparse Index
        prepareSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);
        List<Double> originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double originalSparseMemoryUsageSum = originalSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();

        // Execute clear cache request
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response response = client().performRequest(clearCacheRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), response.getEntity().getContent()).map();

        assertNotNull(responseMap);
        assertEquals(Map.of("total", 1, "successful", 1, "failed", 0), responseMap.get("_shards"));

        // Verify memory usage decreased after clear cache
        List<Double> afterClearCacheSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double afterClearCacheSparseMemoryUsageSum = afterClearCacheSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        assertTrue("Memory usage should decrease after clear cache", afterClearCacheSparseMemoryUsageSum < originalSparseMemoryUsageSum);
        assertEquals(originalSparseMemoryUsageStats.size(), afterClearCacheSparseMemoryUsageStats.size());
        // In multi-node environment, only nodes with shards will have memory usage changes
        int nodesDecreaseMemoryNumber = 0;
        int nodesRemainMemoryNumber = 0;
        for (int i = 0; i < originalSparseMemoryUsageStats.size(); i++) {
            if (afterClearCacheSparseMemoryUsageStats.get(i) < originalSparseMemoryUsageStats.get(i)) {
                nodesDecreaseMemoryNumber += 1;
            }
            if (Objects.equals(afterClearCacheSparseMemoryUsageStats.get(i), originalSparseMemoryUsageStats.get(i))) {
                nodesRemainMemoryNumber += 1;
            }
        }
        assertEquals("One node should have decreased memory usage after clear cache", 1, nodesDecreaseMemoryNumber);
        assertEquals(
            "The nodes without shard should have remained the same memory usage after clear cache",
            getNodeCount() - 1,
            nodesRemainMemoryNumber
        );
    }

    /**
     * Test warm up cache API for sparse index with memory usage verification. Multiple shards and replicas.
     */
    @SneakyThrows
    public void testWarmUpMultiShardReplicasCache() {
        int shards = 3;
        int replicas = getEffectiveReplicaCount(3);
        int totalShards = shards * (replicas + 1);

        // Create Sparse Index
        prepareMultiShardReplicasIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME, shards, replicas);

        // First clear cache before warm up
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response clearCacheResponse = client().performRequest(clearCacheRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(clearCacheResponse.getStatusLine().getStatusCode()));

        List<Double> originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double originalSparseMemoryUsageSum = originalSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();

        // Execute warm up cache request
        Request warmUpRequest = new Request("POST", "/_plugins/_neural/warmup/" + TEST_INDEX_NAME);
        Response warmUpResponse = client().performRequest(warmUpRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(warmUpResponse.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), warmUpResponse.getEntity().getContent()).map();

        assertNotNull(responseMap);
        Map<String, Object> shardsInfo = (Map<String, Object>) responseMap.get("_shards");
        assertEquals(0, shardsInfo.get("failed"));

        // Verify memory usage increased after warm up
        List<Double> afterWarmUpSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double afterWarmUpSparseMemoryUsageSum = afterWarmUpSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        assertTrue("Memory usage should increase after warm up", afterWarmUpSparseMemoryUsageSum > originalSparseMemoryUsageSum);
        assertEquals(originalSparseMemoryUsageStats.size(), afterWarmUpSparseMemoryUsageStats.size());
        int nodesWithMemoryIncrease = 0;
        for (int i = 0; i < originalSparseMemoryUsageStats.size(); i++) {
            if (afterWarmUpSparseMemoryUsageStats.get(i) > originalSparseMemoryUsageStats.get(i)) {
                nodesWithMemoryIncrease++;
            }
        }
        assertEquals("At least data nodes should have memory increase", nodesWithMemoryIncrease, getDataNodeCount());
    }

    /**
     * Test clear cache API for sparse index with memory usage verification. Multiple shards and replicas.
     */
    @SneakyThrows
    public void testClearMultiShardReplicasCache() {
        int shards = 3;
        int replicas = getEffectiveReplicaCount(3);
        int totalShards = shards * (replicas + 1);

        // Create Sparse Index
        prepareMultiShardReplicasIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME, shards, replicas);

        List<Double> originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double originalSparseMemoryUsageSum = originalSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();

        // Execute clear cache request
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response response = client().performRequest(clearCacheRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), response.getEntity().getContent()).map();

        assertNotNull(responseMap);
        Map<String, Object> shardsInfo = (Map<String, Object>) responseMap.get("_shards");
        assertEquals(0, shardsInfo.get("failed"));

        // Verify memory usage decreased after clear cache
        List<Double> afterClearCacheSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double afterClearCacheSparseMemoryUsageSum = afterClearCacheSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        assertTrue("Memory usage should decrease after clear cache", afterClearCacheSparseMemoryUsageSum < originalSparseMemoryUsageSum);
        assertEquals(originalSparseMemoryUsageStats.size(), afterClearCacheSparseMemoryUsageStats.size());
        int nodesWithMemoryDecrease = 0;
        for (int i = 0; i < originalSparseMemoryUsageStats.size(); i++) {
            if (afterClearCacheSparseMemoryUsageStats.get(i) < originalSparseMemoryUsageStats.get(i)) {
                nodesWithMemoryDecrease++;
            }
        }
        assertEquals("At least data nodes should have memory decrease", nodesWithMemoryDecrease, getDataNodeCount());
    }

    /**
     * Test warm up cache API for sparse index with memory usage verification. Mix SEISMIC and Rank Feature.
     */
    @SneakyThrows
    public void testWarmUpCache_MixSeismicAndRankFeatures() {
        // Create Sparse Index
        prepareMixSeismicRankFeaturesIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);

        // First clear cache before warm up
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response clearCacheResponse = client().performRequest(clearCacheRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(clearCacheResponse.getStatusLine().getStatusCode()));

        List<Double> originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double originalSparseMemoryUsageSum = originalSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();

        // Execute warm up cache request
        Request warmUpRequest = new Request("POST", "/_plugins/_neural/warmup/" + TEST_INDEX_NAME);
        Response warmUpResponse = client().performRequest(warmUpRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(warmUpResponse.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), warmUpResponse.getEntity().getContent()).map();

        assertNotNull(responseMap);
        assertEquals(Map.of("total", 1, "successful", 1, "failed", 0), responseMap.get("_shards"));

        // Verify memory usage increased after warm up
        List<Double> afterWarmUpSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double afterWarmUpSparseMemoryUsageSum = afterWarmUpSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        assertTrue("Memory usage should increase after warm up", afterWarmUpSparseMemoryUsageSum > originalSparseMemoryUsageSum);
        assertEquals(originalSparseMemoryUsageStats.size(), afterWarmUpSparseMemoryUsageStats.size());
        // In multi-node environment, only nodes with shards will have memory usage changes
        int nodesIncreaseMemoryNumber = 0;
        int nodesRemainMemoryNumber = 0;
        for (int i = 0; i < originalSparseMemoryUsageStats.size(); i++) {
            if (afterWarmUpSparseMemoryUsageStats.get(i) > originalSparseMemoryUsageStats.get(i)) {
                nodesIncreaseMemoryNumber += 1;
            }
            if (Objects.equals(afterWarmUpSparseMemoryUsageStats.get(i), originalSparseMemoryUsageStats.get(i))) {
                nodesRemainMemoryNumber += 1;
            }
        }
        assertEquals("One node should have increased memory usage after warm up", 1, nodesIncreaseMemoryNumber);
        assertEquals(
            "The nodes without shard should have remained the same memory usage after warm up",
            getNodeCount() - 1,
            nodesRemainMemoryNumber
        );
    }

    /**
     * Test clear cache API for sparse index with memory usage verification. Mix SEISMIC and Rank Feature.
     */
    @SneakyThrows
    public void testClearCache_MixSeismicAndRankFeatures() {
        // Create Sparse Index
        prepareMixSeismicRankFeaturesIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);
        List<Double> originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double originalSparseMemoryUsageSum = originalSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();

        // Execute clear cache request
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response response = client().performRequest(clearCacheRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), response.getEntity().getContent()).map();

        assertNotNull(responseMap);
        assertEquals(Map.of("total", 1, "successful", 1, "failed", 0), responseMap.get("_shards"));

        // Verify memory usage decreased after clear cache
        List<Double> afterClearCacheSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double afterClearCacheSparseMemoryUsageSum = afterClearCacheSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        assertTrue("Memory usage should decrease after clear cache", afterClearCacheSparseMemoryUsageSum < originalSparseMemoryUsageSum);
        assertEquals(originalSparseMemoryUsageStats.size(), afterClearCacheSparseMemoryUsageStats.size());
        // In multi-node environment, only nodes with shards will have memory usage changes
        int nodesDecreaseMemoryNumber = 0;
        int nodesRemainMemoryNumber = 0;
        for (int i = 0; i < originalSparseMemoryUsageStats.size(); i++) {
            if (afterClearCacheSparseMemoryUsageStats.get(i) < originalSparseMemoryUsageStats.get(i)) {
                nodesDecreaseMemoryNumber += 1;
            }
            if (Objects.equals(afterClearCacheSparseMemoryUsageStats.get(i), originalSparseMemoryUsageStats.get(i))) {
                nodesRemainMemoryNumber += 1;
            }
        }
        assertEquals("One node should have decreased memory usage after clear cache", 1, nodesDecreaseMemoryNumber);
        assertEquals(
            "The nodes without shard should have remained the same memory usage after clear cache",
            getNodeCount() - 1,
            nodesRemainMemoryNumber
        );
    }

    /**
     * Test warm up cache API for sparse index with memory usage verification. Mix SEISMIC and Rank Feature.
     */
    @SneakyThrows
    public void testWarmUpCache_OnlyRankFeatures() {
        // Create Sparse Index
        prepareOnlyRankFeaturesIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);

        // First clear cache before warm up
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response clearCacheResponse = client().performRequest(clearCacheRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(clearCacheResponse.getStatusLine().getStatusCode()));

        List<Double> originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double originalSparseMemoryUsageSum = originalSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();

        // Execute warm up cache request
        Request warmUpRequest = new Request("POST", "/_plugins/_neural/warmup/" + TEST_INDEX_NAME);
        Response warmUpResponse = client().performRequest(warmUpRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(warmUpResponse.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), warmUpResponse.getEntity().getContent()).map();

        assertNotNull(responseMap);
        assertEquals(Map.of("total", 1, "successful", 1, "failed", 0), responseMap.get("_shards"));

        // Verify memory usage not changed after warm up
        List<Double> afterWarmUpSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double afterWarmUpSparseMemoryUsageSum = afterWarmUpSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        assertEquals(
            "Memory usage should not change after warm up",
            afterWarmUpSparseMemoryUsageSum,
            originalSparseMemoryUsageSum,
            DELTA_FOR_SCORE_ASSERTION
        );
        assertEquals(originalSparseMemoryUsageStats.size(), afterWarmUpSparseMemoryUsageStats.size());
        for (int i = 0; i < originalSparseMemoryUsageStats.size(); i++) {
            assertEquals(originalSparseMemoryUsageStats.get(i), afterWarmUpSparseMemoryUsageStats.get(i));
        }
    }

    /**
     * Test clear cache API for sparse index with memory usage verification. Mix SEISMIC and Rank Feature.
     */
    @SneakyThrows
    public void testClearCache_OnlyRankFeatures() {
        // Create Sparse Index
        prepareOnlyRankFeaturesIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);
        List<Double> originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double originalSparseMemoryUsageSum = originalSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();

        // Execute clear cache request
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response response = client().performRequest(clearCacheRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), response.getEntity().getContent()).map();

        assertNotNull(responseMap);
        assertEquals(Map.of("total", 1, "successful", 1, "failed", 0), responseMap.get("_shards"));

        // Verify memory usage not changed after clear cache
        List<Double> afterClearCacheSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double afterClearCacheSparseMemoryUsageSum = afterClearCacheSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        assertEquals(
            "Memory usage should not change after clear cache",
            afterClearCacheSparseMemoryUsageSum,
            originalSparseMemoryUsageSum,
            DELTA_FOR_SCORE_ASSERTION
        );
        assertEquals(originalSparseMemoryUsageStats.size(), afterClearCacheSparseMemoryUsageStats.size());
        for (int i = 0; i < originalSparseMemoryUsageStats.size(); i++) {
            assertEquals(afterClearCacheSparseMemoryUsageStats.get(i), originalSparseMemoryUsageStats.get(i));
        }
    }

    /**
     * Test warm up cache API with non-sparse index should fail
     */
    @SneakyThrows
    public void testWarmUpCacheWithNonSparseIndex() {
        String nonSparseIndex = "non-sparse-index";
        // Create Non-Sparse Index
        prepareNonSparseIndex(nonSparseIndex);

        // Try to warm up cache - should fail
        Request warmUpRequest = new Request("POST", "/_plugins/_neural/warmup/" + nonSparseIndex);
        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(warmUpRequest));

        assertEquals(RestStatus.BAD_REQUEST, RestStatus.fromCode(exception.getResponse().getStatusLine().getStatusCode()));
        String responseBody = EntityUtils.toString(exception.getResponse().getEntity());
        assertTrue(responseBody.contains("don't support neural_sparse_warmup_action operation"));
    }

    /**
     * Test clear cache API with non-sparse index should fail
     */
    @SneakyThrows
    public void testClearCacheWithNonSparseIndex() {
        String nonSparseIndex = "non-sparse-index-2";
        // Create Non-Sparse Index
        prepareNonSparseIndex(nonSparseIndex);

        // Try to clear cache - should fail
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + nonSparseIndex);
        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(clearCacheRequest));

        assertEquals(RestStatus.BAD_REQUEST, RestStatus.fromCode(exception.getResponse().getStatusLine().getStatusCode()));
        String responseBody = EntityUtils.toString(exception.getResponse().getEntity());
        assertTrue(responseBody.contains("don't support neural_sparse_clear_cache_action operation"));
    }
}
