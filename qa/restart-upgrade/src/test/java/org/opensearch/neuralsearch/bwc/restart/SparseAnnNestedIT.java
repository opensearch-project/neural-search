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
import java.util.Locale;
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
        int shards = 3;
        int replicas = 0;
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
                1,
                shards,
                replicas
            );

            List<List<Map<String, Float>>> documentsWithChunks = List.of(
                List.of(Map.of("1000", 0.9f, "2000", 0.1f), Map.of("1000", 0.6f, "2000", 0.4f), Map.of("1000", 0.3f, "2000", 0.7f)),
                List.of(Map.of("1000", 0.8f, "2000", 0.2f), Map.of("1000", 0.5f, "2000", 0.5f), Map.of("1000", 0.2f, "2000", 0.8f)),
                List.of(Map.of("1000", 0.7f, "2000", 0.3f), Map.of("1000", 0.4f, "2000", 0.6f), Map.of("1000", 0.1f, "3000", 0.9f))
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
            int expectedDocCount = 9;

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
                SparseTestCommon.forceMerge(client(), indexName);
                SparseTestCommon.waitForSegmentMerge(client(), indexName, shards, replicas);

                int expectedDocCount = 12;
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

    public void testSparseANNNestedWithModelInference_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        String indexName = getIndexNameForTest();
        String modelId = null;
        int shards = 3;
        int replicas = 0;
        List<String> routingIds = SparseTestCommon.generateUniqueRoutingIds(shards);

        if (isRunningAgainstOldCluster()) {
            modelId = uploadSparseEncodingModel();

            URL pipelineURLPath = classLoader.getResource("processor/PipelineForTextChunkingAndSparseEncodingConfiguration.json");
            Objects.requireNonNull(pipelineURLPath);
            String pipelineConfiguration = Files.readString(Path.of(pipelineURLPath.toURI()));
            createPipelineProcessor(pipelineConfiguration, PIPELINE_NAME, modelId, null);

            SparseTestCommon.createNestedSparseIndex(
                client(),
                indexName,
                NESTED_FIELD_NAME,
                SparseEncodingProcessor.LIST_TYPE_NESTED_MAP_KEY,
                4,
                0.4f,
                0.5f,
                1,
                shards,
                replicas
            );

            updateIndexSettings(indexName, Settings.builder().put("index.default_pipeline", PIPELINE_NAME));

            List<String> documents = List.of(
                "{\"passage_text\": \"hello world this is a test document for chunking\"}",
                "{\"passage_text\": \"machine learning models are used for neural search\"}",
                "{\"passage_text\": \"opensearch provides powerful search capabilities for applications\"}"
            );

            int docId = 1;
            for (int i = 0; i < shards; ++i) {
                StringBuilder payloadBuilder = new StringBuilder();
                for (String doc : documents) {
                    payloadBuilder.append(
                        String.format(Locale.ROOT, "{ \"index\": { \"_index\": \"%s\", \"_id\": \"%d\"} }", indexName, docId)
                    );
                    payloadBuilder.append(System.lineSeparator());
                    payloadBuilder.append(doc);
                    payloadBuilder.append(System.lineSeparator());
                    docId++;
                }
                bulkIngest(payloadBuilder.toString(), null, routingIds.get(i));
            }

            SparseTestCommon.forceMerge(client(), indexName);
            SparseTestCommon.waitForSegmentMerge(client(), indexName, shards, replicas);

            validateSparseANNNestedSearchWithModel(
                indexName,
                NESTED_FIELD_NAME,
                SPARSE_FIELD_NAME,
                modelId,
                "hello world",
                Set.of("1", "4", "7")
            );
        } else {
            try {
                modelId = TestUtils.getModelId(getIngestionPipeline(PIPELINE_NAME), SPARSE_ENCODING_PROCESSOR, 1);
                loadAndWaitForModelToBeReady(modelId);

                // Ingest one new document per shard
                List<String> newDocuments = List.of("{\"passage_text\": \"upgraded document for testing after cluster upgrade\"}");

                int newDocId = 10;
                for (int i = 0; i < shards; ++i) {
                    StringBuilder payloadBuilder = new StringBuilder();
                    for (String doc : newDocuments) {
                        payloadBuilder.append(
                            String.format(Locale.ROOT, "{ \"index\": { \"_index\": \"%s\", \"_id\": \"%d\"} }", indexName, newDocId)
                        );
                        payloadBuilder.append(System.lineSeparator());
                        payloadBuilder.append(doc);
                        payloadBuilder.append(System.lineSeparator());
                        newDocId++;
                    }
                    bulkIngest(payloadBuilder.toString(), null, routingIds.get(i));
                }
                SparseTestCommon.forceMerge(client(), indexName);
                SparseTestCommon.waitForSegmentMerge(client(), indexName, shards, replicas);

                validateSparseANNNestedSearchWithModel(
                    indexName,
                    NESTED_FIELD_NAME,
                    SPARSE_FIELD_NAME,
                    modelId,
                    "upgraded document",
                    Set.of("10", "11", "12")
                );
            } finally {
                wipeOfTestResources(indexName, PIPELINE_NAME, modelId, null);
            }
        }
    }
}
