/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;

/**
 * Integration tests for warm up and clear cache against a native-engine index. Native only: the
 * Lucene-engine behaviour of the same APIs is covered by {@link NeuralSparseCacheOperationIT}.
 *
 * <p>A native-engine field is searched from its own index file, so it has nothing in the sparse
 * caches these APIs manage. Both are expected to skip it: succeed on every shard and leave the
 * cached memory untouched, rather than fail or load data no query reads.
 */
public class NeuralSparseNativeCacheOperationIT extends SparseBaseIT {

    private static final String TEST_INDEX_NAME = "test-native-sparse-cache-index";
    private static final String TEST_TEXT_FIELD_NAME = "text";
    private static final String TEST_SPARSE_FIELD_NAME = "sparse_field";

    @ParametersFactory(argumentFormatting = "engine=%s")
    public static Collection<Object[]> parameters() {
        return nativeEngineOnly();
    }

    public NeuralSparseNativeCacheOperationIT(SparseEngine engine) {
        super(engine);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        enableStats();
    }

    @After
    @Override
    @SneakyThrows
    public void tearDown() {
        disableStats();
        super.tearDown();
    }

    @SneakyThrows
    public void testWarmUpSkipsNativeEngineIndex() {
        prepareSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);

        List<Double> beforeStats = getSparseMemoryUsageStatsAcrossNodes();

        Request warmUpRequest = new Request("POST", "/_plugins/_neural/warmup/" + TEST_INDEX_NAME);
        Response warmUpResponse = client().performRequest(warmUpRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(warmUpResponse.getStatusLine().getStatusCode()));
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), warmUpResponse.getEntity().getContent()).map();
        assertEquals(Map.of("total", 1, "successful", 1, "failed", 0), responseMap.get("_shards"));

        assertMemoryUsageUnchanged(beforeStats, getSparseMemoryUsageStatsAcrossNodes());
    }

    @SneakyThrows
    public void testClearCacheSkipsNativeEngineIndex() {
        prepareSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);

        List<Double> beforeStats = getSparseMemoryUsageStatsAcrossNodes();

        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response clearCacheResponse = client().performRequest(clearCacheRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(clearCacheResponse.getStatusLine().getStatusCode()));
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), clearCacheResponse.getEntity().getContent()).map();
        assertEquals(Map.of("total", 1, "successful", 1, "failed", 0), responseMap.get("_shards"));

        assertMemoryUsageUnchanged(beforeStats, getSparseMemoryUsageStatsAcrossNodes());
    }

    private void assertMemoryUsageUnchanged(List<Double> before, List<Double> after) {
        assertEquals(before.size(), after.size());
        for (int i = 0; i < before.size(); i++) {
            assertEquals(
                "Cached memory should not change on a native-engine index",
                before.get(i),
                after.get(i),
                DELTA_FOR_SCORE_ASSERTION
            );
        }
    }
}
