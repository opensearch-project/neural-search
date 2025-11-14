/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.SparseTestCommon;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;

public class SparseAnnIT extends AbstractRollingUpgradeTestCase {

    private static final String TEXT_FIELD_NAME = "passage_text";
    private static final String EMBEDDING_FIELD_NAME = "sparse_embedding";

    public void testSparseAnnIndexAndSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER, 90);
        String indexName = getIndexNameForTest();

        switch (getClusterType()) {
            case OLD:
                createIndexWithConfiguration(
                    indexName,
                    Files.readString(Path.of(classLoader.getResource("processor/SparseAnnIndexMappings.json").toURI())),
                    null
                );
                SparseTestCommon.ingestDocumentsAndForceMergeForSingleShard(
                    client(),
                    indexName,
                    TEXT_FIELD_NAME,
                    EMBEDDING_FIELD_NAME,
                    List.of(
                        Map.of("1000", 0.1f, "1001", 0.1f),
                        Map.of("2000", 0.2f, "2001", 0.2f),
                        Map.of("3000", 0.3f, "3001", 0.3f),
                        Map.of("4000", 0.4f, "4001", 0.4f),
                        Map.of("5000", 0.5f, "5001", 0.5f),
                        Map.of("6000", 0.6f, "6001", 0.6f),
                        Map.of("7000", 0.7f, "7001", 0.7f),
                        Map.of("8000", 0.8f, "8001", 0.8f),
                        Map.of("9000", 0.8f, "9001", 0.8f),
                        Map.of("10000", 0.8f, "10001", 0.8f)
                    )
                );
                break;
            case MIXED:
                if (isFirstMixedRound()) {
                    validateAndSearch(indexName, EMBEDDING_FIELD_NAME, 10);
                    SparseTestCommon.ingestDocuments(
                        indexName,
                        TEXT_FIELD_NAME,
                        EMBEDDING_FIELD_NAME,
                        List.of(Map.of("1002", 0.1f, "1003", 0.1f)),
                        null,
                        11
                    );
                } else {
                    validateAndSearch(indexName, EMBEDDING_FIELD_NAME, 11);
                }
                break;
            case UPGRADED:
                try {
                    SparseTestCommon.ingestDocuments(
                        indexName,
                        TEXT_FIELD_NAME,
                        EMBEDDING_FIELD_NAME,
                        List.of(Map.of("1004", 0.1f, "1005", 0.1f)),
                        null,
                        12
                    );
                    validateAndSearch(indexName, EMBEDDING_FIELD_NAME, 12);
                } finally {
                    wipeOfTestResources(getIndexNameForTest(), null, null, null);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }
    }

    public void testSparseAnnCacheOperation_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER, 90);
        String indexName = getIndexNameForTest();

        switch (getClusterType()) {
            case OLD:
                createIndexWithConfiguration(
                    indexName,
                    Files.readString(Path.of(classLoader.getResource("processor/SparseAnnIndexMappings.json").toURI())),
                    null
                );
                SparseTestCommon.ingestDocumentsAndForceMergeForSingleShard(
                    client(),
                    indexName,
                    TEXT_FIELD_NAME,
                    EMBEDDING_FIELD_NAME,
                    List.of(
                        Map.of("1000", 0.1f, "1001", 0.1f),
                        Map.of("2000", 0.2f, "2001", 0.2f),
                        Map.of("3000", 0.3f, "3001", 0.3f),
                        Map.of("4000", 0.4f, "4001", 0.4f),
                        Map.of("5000", 0.5f, "5001", 0.5f),
                        Map.of("6000", 0.6f, "6001", 0.6f),
                        Map.of("7000", 0.7f, "7001", 0.7f),
                        Map.of("8000", 0.8f, "8001", 0.8f),
                        Map.of("9000", 0.8f, "9001", 0.8f),
                        Map.of("10000", 0.8f, "10001", 0.8f)
                    )
                );
                break;
            case MIXED:
                if (isFirstMixedRound()) {
                    validateAndSearch(indexName, EMBEDDING_FIELD_NAME, 11);
                    SparseTestCommon.ingestDocuments(
                        indexName,
                        TEXT_FIELD_NAME,
                        EMBEDDING_FIELD_NAME,
                        List.of(Map.of("1002", 0.1f, "1003", 0.1f)),
                        null,
                        11
                    );
                    // Execute clear cache request
                    Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + indexName);
                    Response response = client().performRequest(clearCacheRequest);
                    assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
                    validateSparseAnn(indexName, EMBEDDING_FIELD_NAME);
                } else {
                    Request warmUpRequest = new Request("POST", "/_plugins/_neural/warmup/" + indexName);
                    Response warmUpResponse = client().performRequest(warmUpRequest);
                    assertEquals(RestStatus.OK, RestStatus.fromCode(warmUpResponse.getStatusLine().getStatusCode()));
                    validateSparseAnn(indexName, EMBEDDING_FIELD_NAME);

                    validateAndSearch(indexName, EMBEDDING_FIELD_NAME, 12);
                }
                break;
            case UPGRADED:
                try {
                    SparseTestCommon.ingestDocuments(
                        indexName,
                        TEXT_FIELD_NAME,
                        EMBEDDING_FIELD_NAME,
                        List.of(Map.of("1004", 0.1f, "1005", 0.1f)),
                        null,
                        12
                    );
                    validateDocCountAndInfo(indexName, 12, () -> getDocById(indexName, "4"), EMBEDDING_FIELD_NAME, Map.class);
                } finally {
                    wipeOfTestResources(getIndexNameForTest(), null, null, null);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }
    }

    private void validateAndSearch(String indexName, String fieldName, int docCount) {
        validateDocCountAndInfo(indexName, docCount, () -> getDocById(indexName, "4"), fieldName, Map.class);
        validateSparseAnn(indexName, fieldName);
    }

    private void validateSparseAnn(String index, String field) {
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = SparseTestCommon.getNeuralSparseQueryBuilder(
            field,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );
        Map<String, Object> searchResults = search(index, neuralSparseQueryBuilder, 10);
        assertEquals(2, getHitCount(searchResults));
        Set<String> actualIds = new HashSet<>(SparseTestCommon.getDocIDs(searchResults));
        assertEquals(Set.of("1", "2"), actualIds);
    }
}
