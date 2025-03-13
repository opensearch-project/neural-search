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
import java.util.Map;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.TEXT_EMBEDDING_PROCESSOR;
import static org.opensearch.neuralsearch.util.TestUtils.getModelId;

public class RestNeuralStatsActionIT extends AbstractRestartUpgradeRestTestCase {
    private static final String PIPELINE_NAME = "nlp-pipeline";
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

        // Currently using text embedding processor executions stat since that's the only one implemented
        // Once other stats are implemented, it may be smarter to use those instead of text embedding processor
        // to avoid having to upload a model and run inference.
        if (isRunningAgainstOldCluster()) {
            String modelId = uploadTextEmbeddingModel();
            loadModel(modelId);
            createPipelineProcessor(modelId, PIPELINE_NAME);
            createIndexWithConfiguration(
                getIndexNameForTest(),
                Files.readString(Path.of(classLoader.getResource("processor/IndexMappingMultipleShard.json").toURI())),
                PIPELINE_NAME
            );
            addDocument(getIndexNameForTest(), "0", TEST_FIELD, TEXT, null, null);

            // Get stats request
            String responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
            Map<String, Object> infoStats = parseInfoStatsResponse(responseBody);
            Map<String, Object> aggregatedNodeStats = parseAggregatedNodeStatsResponse(responseBody);

            assertEquals(1, getNestedValue(aggregatedNodeStats, EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS));
            assertEquals(1, getNestedValue(infoStats, InfoStatName.TEXT_EMBEDDING_PROCESSORS));
        } else {
            String modelId = null;
            try {
                modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                loadModel(modelId);
                addDocument(getIndexNameForTest(), "1", TEST_FIELD, TEXT_1, null, null);
                addDocument(getIndexNameForTest(), "2", TEST_FIELD, TEXT_1, null, null);
                addDocument(getIndexNameForTest(), "3", TEST_FIELD, TEXT_1, null, null);

                // Get stats request
                String responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
                Map<String, Object> infoStats = parseInfoStatsResponse(responseBody);
                Map<String, Object> aggregatedNodeStats = parseAggregatedNodeStatsResponse(responseBody);

                assertEquals(3, getNestedValue(aggregatedNodeStats, EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS));
                assertEquals(1, getNestedValue(infoStats, InfoStatName.TEXT_EMBEDDING_PROCESSORS));
            } finally {
                wipeOfTestResources(getIndexNameForTest(), PIPELINE_NAME, modelId, null);
            }
        }
    }
}
