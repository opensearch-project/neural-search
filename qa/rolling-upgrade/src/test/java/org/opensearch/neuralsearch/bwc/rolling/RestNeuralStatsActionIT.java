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

public class RestNeuralStatsActionIT extends AbstractRollingUpgradeTestCase {
    private static final String PIPELINE_NAME = "nlp-pipeline";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEXT = "Hello world";
    private static final String TEXT_MIXED = "Hello world mixed";
    private static final String TEXT_UPGRADED = "Hello world upgraded";
    private static final int NUM_DOCS_PER_ROUND = 1;
    private static String modelId = "";

    // Test rolling-upgrade neural stats action
    // Create Text Embedding Processor, Ingestion Pipeline and add document
    // Validate stats are correct during upgrade
    // When new stats are added, we will also want to validate handling fetching stats from previous versions
    // that don't have those stats.
    public void testStats_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER, 90);
        updateClusterSettings("plugins.neural_search.stats_enabled", true);

        switch (getClusterType()) {
            case OLD:
                modelId = uploadTextEmbeddingModel();
                loadModel(modelId);
                createPipelineProcessor(modelId, PIPELINE_NAME);
                createIndexWithConfiguration(
                    getIndexNameForTest(),
                    Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
                    PIPELINE_NAME
                );
                addDocument(getIndexNameForTest(), "0", TEST_FIELD, TEXT, null, null);

                // Get stats request
                String responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
                Map<String, Object> infoStats = parseInfoStatsResponse(responseBody);
                Map<String, Object> aggregatedNodeStats = parseAggregatedNodeStatsResponse(responseBody);

                assertEquals(1, getNestedValue(aggregatedNodeStats, EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS));
                assertEquals(1, getNestedValue(infoStats, InfoStatName.TEXT_EMBEDDING_PROCESSORS));
                break;
            case MIXED:
                // Get stats request
                responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
                infoStats = parseInfoStatsResponse(responseBody);

                assertEquals(1, getNestedValue(infoStats, InfoStatName.TEXT_EMBEDDING_PROCESSORS));
                break;
            case UPGRADED:
                try {
                    // Get stats request
                    responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
                    infoStats = parseInfoStatsResponse(responseBody);
                    aggregatedNodeStats = parseAggregatedNodeStatsResponse(responseBody);

                    // After all nodes have be restarted, all event stats should be reset as well
                    assertEquals(0, getNestedValue(aggregatedNodeStats, EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS));
                    assertEquals(1, getNestedValue(infoStats, InfoStatName.TEXT_EMBEDDING_PROCESSORS));
                } finally {
                    wipeOfTestResources(getIndexNameForTest(), PIPELINE_NAME, modelId, null);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }
    }
}
