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
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.settings.NeuralSearchSettings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;
import static org.opensearch.neuralsearch.util.TestUtils.getTotalHits;

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
        int docCount = 100;
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, 0.4f, 0.5f, docCount);
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
        cleanUp();
        super.tearDown();
    }

    /**
     * Test warm up cache API for sparse index with memory usage verification
     */
    @SneakyThrows
    public void testWarmUpCache() {
        // First clear cache before warm up
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response clearCacheResponse = client().performRequest(clearCacheRequest);
        List<Double> originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double originalSparseMemoryUsageSum = originalSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();

        // Execute warm up cache request
        Request warmUpRequest = new Request("POST", "/_plugins/_neural/warmup/" + TEST_INDEX_NAME);
        Response warmUpResponse = client().performRequest(warmUpRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(clearCacheResponse.getStatusLine().getStatusCode()));
        assertEquals(RestStatus.OK, RestStatus.fromCode(warmUpResponse.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(
            org.opensearch.common.xcontent.XContentType.JSON.xContent(),
            warmUpResponse.getEntity().getContent()
        ).map();

        assertNotNull(responseMap);
        assertTrue(responseMap.containsKey("_shards"));

        // Verify memory usage increased after warm up
        List<Double> afterWarmUpSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double afterWarmUpSparseMemoryUsageSum = afterWarmUpSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        assertTrue("Memory usage should increase after warm up", afterWarmUpSparseMemoryUsageSum > originalSparseMemoryUsageSum);
        assertEquals(originalSparseMemoryUsageStats.size(), afterWarmUpSparseMemoryUsageStats.size());
        for (int i = 0; i < originalSparseMemoryUsageStats.size(); i++) {
            assertTrue(afterWarmUpSparseMemoryUsageStats.get(i) > originalSparseMemoryUsageStats.get(i));
        }
    }

    /**
     * Test clear cache API for sparse index with memory usage verification
     */
    @SneakyThrows
    public void testClearCache() {
        List<Double> originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double originalSparseMemoryUsageSum = originalSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();

        // Execute clear cache request
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response response = client().performRequest(clearCacheRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(
            org.opensearch.common.xcontent.XContentType.JSON.xContent(),
            response.getEntity().getContent()
        ).map();

        assertNotNull(responseMap);
        assertTrue(responseMap.containsKey("_shards"));

        // Verify memory usage decreased after clear cache
        List<Double> afterClearCacheSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double afterClearCacheSparseMemoryUsageSum = afterClearCacheSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        assertTrue("Memory usage should decrease after clear cache", afterClearCacheSparseMemoryUsageSum < originalSparseMemoryUsageSum);
        assertEquals(originalSparseMemoryUsageStats.size(), afterClearCacheSparseMemoryUsageStats.size());
        for (int i = 0; i < originalSparseMemoryUsageStats.size(); i++) {
            assertTrue(afterClearCacheSparseMemoryUsageStats.get(i) < originalSparseMemoryUsageStats.get(i));
        }
    }

    /**
     * Test warm up cache API for sparse index with memory usage verification. Multiple shards and replicas.
     */
    @SneakyThrows
    public void testWarmUpMultiShardReplicasCache() {
        Request request = new Request("DELETE", "/" + TEST_INDEX_NAME);
        client().performRequest(request);
        // Create Sparse Index
        prepareMultiShardReplicasIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);

        // First clear cache before warm up
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response clearCacheResponse = client().performRequest(clearCacheRequest);
        List<Double> originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double originalSparseMemoryUsageSum = originalSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();

        // Execute warm up cache request
        Request warmUpRequest = new Request("POST", "/_plugins/_neural/warmup/" + TEST_INDEX_NAME);
        Response warmUpResponse = client().performRequest(warmUpRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(clearCacheResponse.getStatusLine().getStatusCode()));
        assertEquals(RestStatus.OK, RestStatus.fromCode(warmUpResponse.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(
            org.opensearch.common.xcontent.XContentType.JSON.xContent(),
            warmUpResponse.getEntity().getContent()
        ).map();

        assertNotNull(responseMap);
        assertTrue(responseMap.containsKey("_shards"));

        // Verify memory usage increased after warm up
        List<Double> afterWarmUpSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double afterWarmUpSparseMemoryUsageSum = afterWarmUpSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        assertTrue("Memory usage should increase after warm up", afterWarmUpSparseMemoryUsageSum > originalSparseMemoryUsageSum);
        assertEquals(originalSparseMemoryUsageStats.size(), afterWarmUpSparseMemoryUsageStats.size());
        for (int i = 0; i < originalSparseMemoryUsageStats.size(); i++) {
            assertTrue(afterWarmUpSparseMemoryUsageStats.get(i) > originalSparseMemoryUsageStats.get(i));
        }
    }

    /**
     * Test clear cache API for sparse index with memory usage verification. Multiple shards and replicas.
     */
    @SneakyThrows
    public void testClearMultiShardReplicasCache() {
        Request request = new Request("DELETE", "/" + TEST_INDEX_NAME);
        client().performRequest(request);
        // Create Sparse Index
        prepareMultiShardReplicasIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);
        List<Double> originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double originalSparseMemoryUsageSum = originalSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();

        // Execute clear cache request
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response response = client().performRequest(clearCacheRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(
            org.opensearch.common.xcontent.XContentType.JSON.xContent(),
            response.getEntity().getContent()
        ).map();

        assertNotNull(responseMap);
        assertTrue(responseMap.containsKey("_shards"));

        // Verify memory usage decreased after clear cache
        List<Double> afterClearCacheSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double afterClearCacheSparseMemoryUsageSum = afterClearCacheSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        assertTrue("Memory usage should decrease after clear cache", afterClearCacheSparseMemoryUsageSum < originalSparseMemoryUsageSum);
        assertEquals(originalSparseMemoryUsageStats.size(), afterClearCacheSparseMemoryUsageStats.size());
        for (int i = 0; i < originalSparseMemoryUsageStats.size(); i++) {
            assertTrue(afterClearCacheSparseMemoryUsageStats.get(i) < originalSparseMemoryUsageStats.get(i));
        }
    }

    /**
     * Test warm up cache API for sparse index with memory usage verification. Mix SEISMIC and Rank Feature.
     */
    @SneakyThrows
    public void testWarmUpCache_MixSeismicAndRankFeatures() {
        Request request = new Request("DELETE", "/" + TEST_INDEX_NAME);
        client().performRequest(request);
        // Create Sparse Index
        prepareMixSeismicRankFeaturesIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);

        // First clear cache before warm up
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response clearCacheResponse = client().performRequest(clearCacheRequest);
        List<Double> originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double originalSparseMemoryUsageSum = originalSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();

        // Execute warm up cache request
        Request warmUpRequest = new Request("POST", "/_plugins/_neural/warmup/" + TEST_INDEX_NAME);
        Response warmUpResponse = client().performRequest(warmUpRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(clearCacheResponse.getStatusLine().getStatusCode()));
        assertEquals(RestStatus.OK, RestStatus.fromCode(warmUpResponse.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(
            org.opensearch.common.xcontent.XContentType.JSON.xContent(),
            warmUpResponse.getEntity().getContent()
        ).map();

        assertNotNull(responseMap);
        assertTrue(responseMap.containsKey("_shards"));

        // Verify memory usage increased after warm up
        List<Double> afterWarmUpSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double afterWarmUpSparseMemoryUsageSum = afterWarmUpSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        assertTrue("Memory usage should increase after warm up", afterWarmUpSparseMemoryUsageSum > originalSparseMemoryUsageSum);
        assertEquals(originalSparseMemoryUsageStats.size(), afterWarmUpSparseMemoryUsageStats.size());
        for (int i = 0; i < originalSparseMemoryUsageStats.size(); i++) {
            assertTrue(afterWarmUpSparseMemoryUsageStats.get(i) > originalSparseMemoryUsageStats.get(i));
        }
    }

    /**
     * Test clear cache API for sparse index with memory usage verification. Mix SEISMIC and Rank Feature.
     */
    @SneakyThrows
    public void testClearCache_MixSeismicAndRankFeatures() {
        Request request = new Request("DELETE", "/" + TEST_INDEX_NAME);
        client().performRequest(request);
        // Create Sparse Index
        prepareMixSeismicRankFeaturesIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);
        List<Double> originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double originalSparseMemoryUsageSum = originalSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();

        // Execute clear cache request
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response response = client().performRequest(clearCacheRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(
            org.opensearch.common.xcontent.XContentType.JSON.xContent(),
            response.getEntity().getContent()
        ).map();

        assertNotNull(responseMap);
        assertTrue(responseMap.containsKey("_shards"));

        // Verify memory usage decreased after clear cache
        List<Double> afterClearCacheSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double afterClearCacheSparseMemoryUsageSum = afterClearCacheSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();
        assertTrue("Memory usage should decrease after clear cache", afterClearCacheSparseMemoryUsageSum < originalSparseMemoryUsageSum);
        assertEquals(originalSparseMemoryUsageStats.size(), afterClearCacheSparseMemoryUsageStats.size());
        for (int i = 0; i < originalSparseMemoryUsageStats.size(); i++) {
            assertTrue(afterClearCacheSparseMemoryUsageStats.get(i) < originalSparseMemoryUsageStats.get(i));
        }
    }

    /**
     * Test warm up cache API for sparse index with memory usage verification. Mix SEISMIC and Rank Feature.
     */
    @SneakyThrows
    public void testWarmUpCache_OnlyRankFeatures() {
        Request request = new Request("DELETE", "/" + TEST_INDEX_NAME);
        client().performRequest(request);
        // Create Sparse Index
        prepareOnlyRankFeaturesIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);

        // First clear cache before warm up
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response clearCacheResponse = client().performRequest(clearCacheRequest);
        List<Double> originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double originalSparseMemoryUsageSum = originalSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();

        // Execute warm up cache request
        Request warmUpRequest = new Request("POST", "/_plugins/_neural/warmup/" + TEST_INDEX_NAME);
        Response warmUpResponse = client().performRequest(warmUpRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(clearCacheResponse.getStatusLine().getStatusCode()));
        assertEquals(RestStatus.OK, RestStatus.fromCode(warmUpResponse.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(
            org.opensearch.common.xcontent.XContentType.JSON.xContent(),
            warmUpResponse.getEntity().getContent()
        ).map();

        assertNotNull(responseMap);
        assertTrue(responseMap.containsKey("_shards"));

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
        Request request = new Request("DELETE", "/" + TEST_INDEX_NAME);
        client().performRequest(request);
        // Create Sparse Index
        prepareOnlyRankFeaturesIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);
        List<Double> originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        double originalSparseMemoryUsageSum = originalSparseMemoryUsageStats.stream().mapToDouble(Double::doubleValue).sum();

        // Execute clear cache request
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response response = client().performRequest(clearCacheRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(
            org.opensearch.common.xcontent.XContentType.JSON.xContent(),
            response.getEntity().getContent()
        ).map();

        assertNotNull(responseMap);
        assertTrue(responseMap.containsKey("_shards"));

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

        Request createIndexRequest = new Request("PUT", "/" + nonSparseIndex);
        createIndexRequest.setJsonEntity("{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
        client().performRequest(createIndexRequest);

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

        Request createIndexRequest = new Request("PUT", "/" + nonSparseIndex);
        createIndexRequest.setJsonEntity("{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
        client().performRequest(createIndexRequest);

        // Try to clear cache - should fail
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + nonSparseIndex);
        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(clearCacheRequest));

        assertEquals(RestStatus.BAD_REQUEST, RestStatus.fromCode(exception.getResponse().getStatusLine().getStatusCode()));
        String responseBody = EntityUtils.toString(exception.getResponse().getEntity());
        assertTrue(responseBody.contains("don't support neural_sparse_clear_cache_action operation"));
    }

    /**
     * Test clear cache API for sparse index with memory usage verification
     */
    @SneakyThrows
    public void testClearCacheReturnsSameSearchResults() {
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );

        Map<String, Object> expectedHits = getTotalHits(search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10));

        // Execute clear cache request
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response response = client().performRequest(clearCacheRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(
            org.opensearch.common.xcontent.XContentType.JSON.xContent(),
            response.getEntity().getContent()
        ).map();

        assertNotNull(responseMap);
        assertTrue(responseMap.containsKey("_shards"));

        Map<String, Object> hits = getTotalHits(search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10));

        assertEquals(expectedHits, hits);
    }
}
