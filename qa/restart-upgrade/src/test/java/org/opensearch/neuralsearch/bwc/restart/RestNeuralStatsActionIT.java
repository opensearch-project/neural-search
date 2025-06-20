/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.restart;

import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.info.InfoStatName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.TEXT_EMBEDDING_PROCESSOR;
import static org.opensearch.neuralsearch.util.TestUtils.getModelId;

public class RestNeuralStatsActionIT extends AbstractRestartUpgradeRestTestCase {
    private static final String PIPELINE_NAME = "nlp-pipeline-stats";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEXT = "Hello world";
    private static final String TEXT_1 = "Hello world a";

    // Test restart-upgrade with neural stats
    // Enabled/disabled settings should persist between restarts
    // Event stats should be reset on restart
    // Info stats based on persistent constructs should be persisted between restarts
    public void testNeuralStats_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        updateClusterSettings("plugins.neural_search.stats_enabled", true);

        // Get initial stats
        String responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());

        int numberOfExecution = (int) getNestedValue(
            parseAggregatedNodeStatsResponse(responseBody),
            EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS
        );
        int numberOfProcessor = (int) getNestedValue(parseInfoStatsResponse(responseBody), InfoStatName.TEXT_EMBEDDING_PROCESSORS);

        // Currently using text embedding processor executions stat since that's the only one implemented
        // Once other stats are implemented, it may be smarter to use those instead of text embedding processor
        // to avoid having to upload a model and run inference.
        if (isRunningAgainstOldCluster()) {
            String modelId = uploadAndLoadTextEmbeddingModel();
            createPipelineProcessor(modelId, PIPELINE_NAME);
            createIndexWithConfiguration(
                getIndexNameForTest(),
                Files.readString(Path.of(classLoader.getResource("processor/IndexMappingMultipleShard.json").toURI())),
                PIPELINE_NAME
            );
            addDocument(getIndexNameForTest(), "0", TEST_FIELD, TEXT, null, null);
            addDocument(getIndexNameForTest(), "1", TEST_FIELD, TEXT, null, null);
            addDocument(getIndexNameForTest(), "2", TEST_FIELD, TEXT, null, null);

            // Get stats
            responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());

            assertEquals(
                numberOfExecution + 3,
                getNestedValue(parseAggregatedNodeStatsResponse(responseBody), EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS)
            );
            assertEquals(
                numberOfProcessor + 1,
                getNestedValue(parseInfoStatsResponse(responseBody), InfoStatName.TEXT_EMBEDDING_PROCESSORS)
            );
        } else {
            String modelId = null;
            try {
                modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                loadModel(modelId);
                addDocument(getIndexNameForTest(), "3", TEST_FIELD, TEXT_1, null, null);
                addDocument(getIndexNameForTest(), "4", TEST_FIELD, TEXT_1, null, null);
                addDocument(getIndexNameForTest(), "5", TEST_FIELD, TEXT_1, null, null);

                // Get stats
                responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());

                assertEquals(
                    numberOfExecution + 3,
                    getNestedValue(parseAggregatedNodeStatsResponse(responseBody), EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS)
                );
                assertEquals(
                    numberOfProcessor,
                    getNestedValue(parseInfoStatsResponse(responseBody), InfoStatName.TEXT_EMBEDDING_PROCESSORS)
                );
            } finally {
                wipeOfTestResources(getIndexNameForTest(), PIPELINE_NAME, modelId, null);
            }
        }
    }
}
