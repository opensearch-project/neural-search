/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling;

import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.info.InfoStatName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.TEXT_EMBEDDING_PROCESSOR;
import static org.opensearch.neuralsearch.util.TestUtils.getModelId;

public class RestNeuralStatsActionIT extends AbstractRollingUpgradeTestCase {
    private static final String PIPELINE_NAME = "nlp-pipeline-stats";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEXT = "Hello world";
    private static final String TEXT_MIXED = "Hello world mixed";
    private static final String TEXT_UPGRADED = "Hello world upgraded";
    private static String modelId = "";

    // Test rolling-upgrade neural stats action
    // Create Text Embedding Processor, Ingestion Pipeline and add document
    // Validate stats are correct during upgrade
    // When new stats are added, we will also want to validate handling fetching stats from previous versions
    // that don't have those stats.
    // TODO: There is a bug in stats api which need to be fixed before enabling following tests
    // https://github.com/opensearch-project/neural-search/issues/1368

//    public void testStats_E2EFlow() throws Exception {
//
//        waitForClusterHealthGreen(NODES_BWC_CLUSTER, 90);
//        updateClusterSettings("plugins.neural_search.stats_enabled", true);
//
//        // Get initial stats
//        String responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
//        logger.info("Initial:" + responseBody);
//        Map<String, Object> infoStats = parseInfoStatsResponse(responseBody);
//        Map<String, Object> aggregatedNodeStats = parseAggregatedNodeStatsResponse(responseBody);
//
//        int numberOfExecution = (int) getNestedValue(aggregatedNodeStats, EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS);
//        int numberOfProcessor = (int) getNestedValue(infoStats, InfoStatName.TEXT_EMBEDDING_PROCESSORS);
//
//        switch (getClusterType()) {
//            case OLD:
//                modelId = uploadTextEmbeddingModel();
//                loadModel(modelId);
//                createPipelineProcessor(modelId, PIPELINE_NAME);
//                createIndexWithConfiguration(
//                    getIndexNameForTest(),
//                    Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
//                    PIPELINE_NAME
//                );
//                addDocument(getIndexNameForTest(), "0", TEST_FIELD, TEXT, null, null);
//                addDocument(getIndexNameForTest(), "1", TEST_FIELD, TEXT, null, null);
//                addDocument(getIndexNameForTest(), "2", TEST_FIELD, TEXT, null, null);
//
//                responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
//                logger.info("Old after insert:" + responseBody);
//                assertEquals(
//                    numberOfExecution + 3,
//                    getNestedValue(parseAggregatedNodeStatsResponse(responseBody), EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS)
//                );
//                assertEquals(
//                    numberOfProcessor + 1,
//                    getNestedValue(parseInfoStatsResponse(responseBody), InfoStatName.TEXT_EMBEDDING_PROCESSORS)
//                );
//                break;
//            case MIXED:
//                modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
//                loadModel(modelId);
//                addDocument(getIndexNameForTest(), "3", TEST_FIELD, TEXT_MIXED, null, null);
//                addDocument(getIndexNameForTest(), "4", TEST_FIELD, TEXT_MIXED, null, null);
//                addDocument(getIndexNameForTest(), "5", TEST_FIELD, TEXT_MIXED, null, null);
//
//                // Get stats
//                responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
//                logger.info("Mixed after insert:" + responseBody);
//
//                assertEquals(
//                    numberOfExecution + 3,
//                    getNestedValue(parseAggregatedNodeStatsResponse(responseBody), EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS)
//                );
//                assertEquals(
//                    numberOfProcessor,
//                    getNestedValue(parseInfoStatsResponse(responseBody), InfoStatName.TEXT_EMBEDDING_PROCESSORS)
//                );
//                break;
//            case UPGRADED:
//                try {
//                    modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
//                    loadModel(modelId);
//                    addDocument(getIndexNameForTest(), "6", TEST_FIELD, TEXT_UPGRADED, null, null);
//                    addDocument(getIndexNameForTest(), "7", TEST_FIELD, TEXT_UPGRADED, null, null);
//                    addDocument(getIndexNameForTest(), "8", TEST_FIELD, TEXT_UPGRADED, null, null);
//
//                    // Get stats
//                    responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
//                    logger.info("Upgraded after insert:" + responseBody);
//
//                    assertEquals(
//                        numberOfExecution + 3,
//                        getNestedValue(parseAggregatedNodeStatsResponse(responseBody), EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS)
//                    );
//                    assertEquals(
//                        numberOfProcessor,
//                        getNestedValue(parseInfoStatsResponse(responseBody), InfoStatName.TEXT_EMBEDDING_PROCESSORS)
//                    );
//                } finally {
//                    wipeOfTestResources(getIndexNameForTest(), PIPELINE_NAME, modelId, null);
//                }
//                break;
//            default:
//                throw new IllegalStateException("Unexpected value: " + getClusterType());
//        }
//    }
}