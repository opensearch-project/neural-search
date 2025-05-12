/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.restart.test;

import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;

public class NeuralSparseTwoPhaseProcessorIT extends AbstractRestartUpgradeRestTestCase {

    private static final String NEURAL_SPARSE_INGEST_PIPELINE_NAME = "nstp-nlp-ingest-pipeline-dense";
    private static final String NEURAL_SPARSE_TWO_PHASE_SEARCH_PIPELINE_NAME = "nstp-nlp-two-phase-search-pipeline-sparse";
    private static final String TEST_ENCODING_FIELD = "passage_embedding";
    private static final String TEST_TEXT_FIELD = "passage_text";
    private static final String TEXT_1 = "Hello world a b";

    public void testNeuralSparseQueryTwoPhaseProcessor_NeuralSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_ENCODING_FIELD).queryText(TEXT_1);
        if (isRunningAgainstOldCluster()) {
            String modelId = getSparseEncodingModelId();
            neuralSparseQueryBuilder.modelId(modelId);
            createPipelineForSparseEncodingProcessor(modelId, NEURAL_SPARSE_INGEST_PIPELINE_NAME);
            createIndexWithConfiguration(
                getIndexNameForTest(),
                Files.readString(Path.of(classLoader.getResource("processor/SparseIndexMappings.json").toURI())),
                NEURAL_SPARSE_INGEST_PIPELINE_NAME
            );
            addSparseEncodingDoc(getIndexNameForTest(), "0", List.of(), List.of(), List.of(TEST_TEXT_FIELD), List.of(TEXT_1));
            createNeuralSparseTwoPhaseSearchProcessor(NEURAL_SPARSE_TWO_PHASE_SEARCH_PIPELINE_NAME);
            updateIndexSettings(
                getIndexNameForTest(),
                Settings.builder().put("index.search.default_pipeline", NEURAL_SPARSE_TWO_PHASE_SEARCH_PIPELINE_NAME)
            );
            Object resultWith2PhasePipeline = search(getIndexNameForTest(), neuralSparseQueryBuilder, 1).get("hits");
            assertNotNull(resultWith2PhasePipeline);
        } else {
            String modelId = null;
            try {
                modelId = getSparseEncodingModelId();
                neuralSparseQueryBuilder.modelId(modelId);
                Object resultWith2PhasePipeline = search(getIndexNameForTest(), neuralSparseQueryBuilder, 1).get("hits");
                assertNotNull(resultWith2PhasePipeline);
            } finally {
                wipeOfTestResources(
                    getIndexNameForTest(),
                    NEURAL_SPARSE_INGEST_PIPELINE_NAME,
                    NEURAL_SPARSE_TWO_PHASE_SEARCH_PIPELINE_NAME
                );
            }
        }
    }
}
