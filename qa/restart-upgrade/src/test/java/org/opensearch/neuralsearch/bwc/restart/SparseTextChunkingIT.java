/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.restart;

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.neuralsearch.SparseTestCommon;
import org.opensearch.neuralsearch.processor.SparseEncodingProcessor;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.sparse.query.SparseAnnQueryBuilder;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;

/**
 * BWC tests for sparse text chunking search functionality
 */
public class SparseTextChunkingIT extends AbstractRestartUpgradeRestTestCase {
    private static final String NESTED_FIELD_NAME = "passage_chunk_embedding";
    private static final String SPARSE_FIELD_NAME = NESTED_FIELD_NAME + "." + SparseEncodingProcessor.LIST_TYPE_NESTED_MAP_KEY;
    private static final String PIPELINE_NAME = "text-chunking-sparse-pipeline";

    public void testSparseTextChunkingWithRawVectors_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        String indexName = getIndexNameForTest();

        if (isRunningAgainstOldCluster()) {
            // Create nested sparse index for text chunking
            SparseTestCommon.createNestedSparseIndex(
                client(),
                indexName,
                NESTED_FIELD_NAME,
                SparseEncodingProcessor.LIST_TYPE_NESTED_MAP_KEY,
                4,
                0.4f,
                0.5f,
                3,
                1,
                0
            );

            // Ingest documents with chunked sparse vectors
            List<List<Map<String, Float>>> documentsWithChunks = List.of(
                List.of(Map.of("1000", 0.9f, "2000", 0.1f), Map.of("1000", 0.6f, "2000", 0.4f), Map.of("1000", 0.3f, "2000", 0.7f)),
                List.of(Map.of("1000", 0.8f, "2000", 0.2f), Map.of("1000", 0.5f, "2000", 0.5f), Map.of("1000", 0.2f, "2000", 0.8f)),
                List.of(Map.of("1000", 0.7f, "2000", 0.3f), Map.of("1000", 0.4f, "2000", 0.6f), Map.of("1000", 0.1f, "3000", 0.9f))
            );

            SparseTestCommon.ingestNestedDocumentsAndForceMergeForSingleShard(
                client(),
                indexName,
                NESTED_FIELD_NAME,
                documentsWithChunks,
                null
            );

            assertEquals(3, getDocCount(indexName));
            validateDocCountAndInfo(indexName, 3, () -> getDocById(indexName, "1"), NESTED_FIELD_NAME, List.class);
        } else {
            try {
                // Validate search functionality after upgrade
                validateSparseTextChunkingSearch(indexName);

                // Add new document to verify ingestion still works
                List<List<Map<String, Float>>> newDocument = List.of(
                    List.of(Map.of("1000", 0.95f, "2000", 0.05f), Map.of("1000", 0.65f, "2000", 0.35f))
                );

                String payload = SparseTestCommon.prepareNestedSparseBulkIngestPayloadWithMultipleChunks(
                    indexName,
                    NESTED_FIELD_NAME,
                    newDocument,
                    4
                );
                bulkIngest(payload, null);

                assertEquals(4, getDocCount(indexName));
                validateSparseTextChunkingSearch(indexName);
            } finally {
                wipeOfTestResources(indexName, null, null, null);
            }
        }
    }

    public void testSparseTextChunkingWithModelInference_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        String indexName = getIndexNameForTest();
        String modelId = null;

        if (isRunningAgainstOldCluster()) {
            // Upload sparse encoding model
            modelId = uploadSparseEncodingModel();

            // Create pipeline for text chunking and sparse encoding
            URL pipelineURLPath = classLoader.getResource("processor/PipelineForTextChunkingAndSparseEncoding.json");
            Objects.requireNonNull(pipelineURLPath);
            String pipelineConfiguration = Files.readString(Path.of(pipelineURLPath.toURI()));
            pipelineConfiguration = pipelineConfiguration.replace("${MODEL_ID}", modelId);

            createPipelineProcessor(pipelineConfiguration, PIPELINE_NAME, "", null);

            // Create nested sparse index
            SparseTestCommon.createNestedSparseIndex(
                client(),
                indexName,
                NESTED_FIELD_NAME,
                SparseEncodingProcessor.LIST_TYPE_NESTED_MAP_KEY,
                4,
                0.4f,
                0.5f,
                3,
                1,
                0
            );

            updateIndexSettings(indexName, Settings.builder().put("index.default_pipeline", PIPELINE_NAME));

            // Ingest documents with text that will be chunked and encoded
            String doc1 = "{\"passage_text\": \"hello world this is a test document for chunking\"}";
            String doc2 = "{\"passage_text\": \"machine learning models are used for neural search\"}";
            String doc3 = "{\"passage_text\": \"opensearch provides powerful search capabilities for applications\"}";

            ingestDocument(indexName, doc1, "1");
            ingestDocument(indexName, doc2, "2");
            ingestDocument(indexName, doc3, "3");

            SparseTestCommon.forceMerge(client(), indexName);
            SparseTestCommon.waitForSegmentMerge(client(), indexName);

            assertEquals(3, getDocCount(indexName));
            validateDocCountAndInfo(indexName, 3, () -> getDocById(indexName, "1"), NESTED_FIELD_NAME, List.class);
        } else {
            try {
                // Get model ID from cluster state
                modelId = getModelId();

                // Validate search functionality after upgrade
                validateSparseTextChunkingSearchWithModel(indexName, modelId);

                // Add new document to verify pipeline still works
                String newDoc = "{\"passage_text\": \"new document for testing after upgrade\"}";
                ingestDocument(indexName, newDoc, "4");

                assertEquals(4, getDocCount(indexName));
                validateSparseTextChunkingSearchWithModel(indexName, modelId);
            } finally {
                wipeOfTestResources(indexName, PIPELINE_NAME, modelId, null);
            }
        }
    }

    public void testSparseTextChunkingMultipleShard_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        String indexName = getIndexNameForTest();
        int shards = 3;
        int replicas = 0;

        if (isRunningAgainstOldCluster()) {
            // Create nested sparse index with multiple shards
            SparseTestCommon.createNestedSparseIndex(
                client(),
                indexName,
                NESTED_FIELD_NAME,
                SparseEncodingProcessor.LIST_TYPE_NESTED_MAP_KEY,
                4,
                0.4f,
                0.5f,
                3,
                shards,
                replicas
            );

            // Ingest documents across multiple shards
            List<List<Map<String, Float>>> documentsWithChunks = List.of(
                List.of(Map.of("1000", 0.9f, "2000", 0.1f), Map.of("1000", 0.6f, "2000", 0.4f)),
                List.of(Map.of("1000", 0.8f, "2000", 0.2f), Map.of("1000", 0.5f, "2000", 0.5f)),
                List.of(Map.of("1000", 0.7f, "2000", 0.3f), Map.of("1000", 0.4f, "2000", 0.6f))
            );

            List<String> routingIds = SparseTestCommon.generateUniqueRoutingIds(shards);
            for (int i = 0; i < shards; ++i) {
                String payload = SparseTestCommon.prepareNestedSparseBulkIngestPayloadWithMultipleChunks(
                    indexName,
                    NESTED_FIELD_NAME,
                    documentsWithChunks,
                    i * documentsWithChunks.size() + 1
                );
                bulkIngest(payload, null, routingIds.get(i));
            }

            SparseTestCommon.forceMerge(client(), indexName);
            SparseTestCommon.waitForSegmentMerge(client(), indexName, shards, replicas);

            assertEquals(shards * documentsWithChunks.size(), getDocCount(indexName));
        } else {
            try {
                // Validate search functionality across multiple shards after upgrade
                validateSparseTextChunkingSearchMultipleShard(indexName);
            } finally {
                wipeOfTestResources(indexName, null, null, null);
            }
        }
    }

    private void validateSparseTextChunkingSearch(String indexName) {
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = SparseTestCommon.getNeuralSparseQueryBuilder(
            SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 1.5f, "2000", 0.5f)
        );

        QueryBuilder nestedQuery = QueryBuilders.nestedQuery(NESTED_FIELD_NAME, neuralSparseQueryBuilder, ScoreMode.Max);
        Map<String, Object> searchResults = search(indexName, nestedQuery, 10);

        assertNotNull(searchResults);
        assertTrue(getHitCount(searchResults) > 0);
        assertEquals(3, getHitCount(searchResults));
    }

    private void validateSparseTextChunkingSearchWithModel(String indexName, String modelId) {
        SparseAnnQueryBuilder annQueryBuilder = new SparseAnnQueryBuilder().queryCut(2).fieldName(SPARSE_FIELD_NAME).heapFactor(1.0f).k(5);

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder().sparseAnnQueryBuilder(annQueryBuilder)
            .fieldName(SPARSE_FIELD_NAME)
            .modelId(modelId)
            .queryText("hello world");

        QueryBuilder nestedQuery = QueryBuilders.nestedQuery(NESTED_FIELD_NAME, neuralSparseQueryBuilder, ScoreMode.Max);
        Map<String, Object> searchResults = search(indexName, nestedQuery, 10);

        assertNotNull(searchResults);
        assertTrue(getHitCount(searchResults) > 0);
        // Document with "hello world" should be in the results
        assertEquals("1", SparseTestCommon.getDocIDs(searchResults).get(0));
    }

    private void validateSparseTextChunkingSearchMultipleShard(String indexName) {
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = SparseTestCommon.getNeuralSparseQueryBuilder(
            SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 1.5f, "2000", 0.5f)
        );

        QueryBuilder nestedQuery = QueryBuilders.nestedQuery(NESTED_FIELD_NAME, neuralSparseQueryBuilder, ScoreMode.Max);
        Map<String, Object> searchResults = search(indexName, nestedQuery, 20);

        assertNotNull(searchResults);
        assertTrue(getHitCount(searchResults) >= 9); // 3 shards * 3 documents per shard
    }

    private String getModelId() throws Exception {
        // Get the first available sparse encoding model from the cluster
        Request getModelsRequest = new Request("GET", "/_plugins/_ml/models");
        Response response = client().performRequest(getModelsRequest);
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), response.getEntity().getContent()).map();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> hits = (List<Map<String, Object>>) ((Map<String, Object>) responseMap.get("hits")).get("hits");

        for (Map<String, Object> hit : hits) {
            @SuppressWarnings("unchecked")
            Map<String, Object> source = (Map<String, Object>) hit.get("_source");
            if ("SPARSE_ENCODING".equals(source.get("algorithm"))) {
                return (String) source.get("model_id");
            }
        }

        throw new IllegalStateException("No sparse encoding model found in cluster");
    }
}
