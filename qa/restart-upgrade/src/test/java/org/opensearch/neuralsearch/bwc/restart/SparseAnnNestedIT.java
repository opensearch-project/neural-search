/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.restart;

import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.SparseTestCommon;
import org.opensearch.neuralsearch.processor.SparseEncodingProcessor;
import org.opensearch.neuralsearch.util.TestUtils;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.SPARSE_ENCODING_PROCESSOR;

public class SparseAnnNestedIT extends AbstractRestartUpgradeRestTestCase {
    private static final String NESTED_FIELD_NAME = "passage_chunk_embedding";
    private static final String SPARSE_FIELD_NAME = NESTED_FIELD_NAME + "." + SparseEncodingProcessor.LIST_TYPE_NESTED_MAP_KEY;
    private static final String PIPELINE_NAME = "text-chunking-sparse-pipeline";

    public void testSparseANNNestedWithRawVectors_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        String indexName = getIndexNameForTest();

        if (isRunningAgainstOldCluster()) {
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

            validateSparseANNNestedSearch(
                indexName,
                NESTED_FIELD_NAME,
                SPARSE_FIELD_NAME,
                Map.of("1000", 1.5f, "2000", 0.5f),
                3,
                Set.of("1")
            );
        } else {
            try {
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

                validateSparseANNNestedSearch(
                    indexName,
                    NESTED_FIELD_NAME,
                    SPARSE_FIELD_NAME,
                    Map.of("1000", 1.5f, "2000", 0.5f),
                    4,
                    Set.of("4")
                );
            } finally {
                wipeOfTestResources(indexName, null, null, null);
            }
        }
    }

    public void testSparseANNNestedWithModelInference_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        String indexName = getIndexNameForTest();
        String modelId = null;

        if (isRunningAgainstOldCluster()) {
            modelId = uploadSparseEncodingModel();

            URL pipelineURLPath = classLoader.getResource("processor/PipelineForTextChunkingAndSparseEncodingConfiguration.json");
            Objects.requireNonNull(pipelineURLPath);
            String pipelineConfiguration = Files.readString(Path.of(pipelineURLPath.toURI()));
            pipelineConfiguration = pipelineConfiguration.replace("${MODEL_ID}", modelId);

            createPipelineProcessor(pipelineConfiguration, PIPELINE_NAME, modelId, null);

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

            String doc1 = "{\"passage_text\": \"hello world this is a test document for chunking\"}";
            String doc2 = "{\"passage_text\": \"machine learning models are used for neural search\"}";
            String doc3 = "{\"passage_text\": \"opensearch provides powerful search capabilities for applications\"}";

            ingestDocument(indexName, doc1, "1");
            ingestDocument(indexName, doc2, "2");
            ingestDocument(indexName, doc3, "3");

            SparseTestCommon.forceMerge(client(), indexName);
            SparseTestCommon.waitForSegmentMerge(client(), indexName);

            validateSparseANNNestedSearchWithModel(indexName, NESTED_FIELD_NAME, SPARSE_FIELD_NAME, modelId, "hello world", Set.of("1"));
            validateDocCountAndInfo(indexName, 3, () -> getDocById(indexName, "1"), NESTED_FIELD_NAME, List.class);
        } else {
            try {
                modelId = TestUtils.getModelId(getIngestionPipeline(PIPELINE_NAME), SPARSE_ENCODING_PROCESSOR, 1);

                String newDoc = "{\"passage_text\": \"new document for testing after upgrade\"}";
                ingestDocument(indexName, newDoc, "4");

                validateSparseANNNestedSearchWithModel(
                    indexName,
                    NESTED_FIELD_NAME,
                    SPARSE_FIELD_NAME,
                    modelId,
                    "hello world",
                    Set.of("4")
                );
                validateDocCountAndInfo(indexName, 4, () -> getDocById(indexName, "1"), NESTED_FIELD_NAME, List.class);
            } finally {
                wipeOfTestResources(indexName, PIPELINE_NAME, modelId, null);
            }
        }
    }

    public void testSparseANNNestedWithMultipleShard_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        String indexName = getIndexNameForTest();
        int shards = 3;
        int replicas = 0;
        int expectedDocCount = 0;
        List<String> routingIds = SparseTestCommon.generateUniqueRoutingIds(shards);

        if (isRunningAgainstOldCluster()) {
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

            List<List<Map<String, Float>>> documentsWithChunks = List.of(
                List.of(Map.of("1000", 0.9f, "2000", 0.1f), Map.of("1000", 0.6f, "2000", 0.4f)),
                List.of(Map.of("1000", 0.8f, "2000", 0.2f), Map.of("1000", 0.5f, "2000", 0.5f)),
                List.of(Map.of("1000", 0.7f, "2000", 0.3f), Map.of("1000", 0.4f, "2000", 0.6f))
            );

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

            expectedDocCount = shards * documentsWithChunks.size();
            validateSparseANNNestedSearch(
                indexName,
                NESTED_FIELD_NAME,
                SPARSE_FIELD_NAME,
                Map.of("1000", 1.5f, "2000", 0.5f),
                expectedDocCount,
                Set.of("1", "4", "7")
            );
        } else {
            try {
                List<List<Map<String, Float>>> newDocument = List.of(
                    List.of(Map.of("1000", 0.95f, "2000", 0.05f), Map.of("1000", 0.65f, "2000", 0.35f))
                );

                for (int i = 0; i < shards; ++i) {
                    String payload = SparseTestCommon.prepareNestedSparseBulkIngestPayloadWithMultipleChunks(
                        indexName,
                        NESTED_FIELD_NAME,
                        newDocument,
                        i * newDocument.size() + 10
                    );
                    bulkIngest(payload, null, routingIds.get(i));
                }

                expectedDocCount += shards * newDocument.size();
                validateSparseANNNestedSearch(
                    indexName,
                    NESTED_FIELD_NAME,
                    SPARSE_FIELD_NAME,
                    Map.of("1000", 1.5f, "2000", 0.5f),
                    expectedDocCount,
                    Set.of("10", "11", "12")
                );
            } finally {
                wipeOfTestResources(indexName, null, null, null);
            }
        }
    }
}
