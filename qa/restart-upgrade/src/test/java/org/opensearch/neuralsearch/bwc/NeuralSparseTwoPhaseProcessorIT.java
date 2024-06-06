/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.bwc;

import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.util.TestUtils;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.TEXT_EMBEDDING_PROCESSOR;

public class NeuralSparseTwoPhaseProcessorIT extends AbstractRestartUpgradeRestTestCase {

    private static final String DENSE_INGEST_PIPELINE_NAME = "nstp-nlp-ingest-pipeline-dense";
    private static final String NEURAL_SPARSE_TWO_PHASE_SEARCH_PIPELINE_NAME = "nstp-nlp-two-phase-search-pipeline-sparse";
    private static final String TEST_ENCODING_FIELD = "passage_embedding";
    private static final String TEST_TEXT_FIELD = "passage_text";
    private static final String TEXT_1 = "Hello world a b";

    public void testNeuralQueryTwoPhaseProcessor_NeuralSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_ENCODING_FIELD).queryText(TEXT_1);
        if (isRunningAgainstOldCluster()) {
            String modelId = uploadTextEmbeddingModel();
            loadModel(modelId);
            neuralSparseQueryBuilder.modelId(modelId);
            createPipelineProcessor(modelId, DENSE_INGEST_PIPELINE_NAME);
            createIndexWithConfiguration(
                getIndexNameForTest(),
                Files.readString(Path.of(classLoader.getResource("processor/IndexMappingMultipleShard.json").toURI())),
                DENSE_INGEST_PIPELINE_NAME
            );
            addDocument(getIndexNameForTest(), "0", TEST_TEXT_FIELD, TEXT_1, null, null);

            Object resultWithOut2PhasePipeline= search(getIndexNameForTest(), neuralSparseQueryBuilder, 1).get("hits");
            createNeuralSparseTwoPhaseSearchProcessor(NEURAL_SPARSE_TWO_PHASE_SEARCH_PIPELINE_NAME);
            updateIndexSettings(getIndexNameForTest(), Settings.builder().put("index.search.default_pipeline", NEURAL_SPARSE_TWO_PHASE_SEARCH_PIPELINE_NAME));
            Object resultWith2PhasePipeline = search(getIndexNameForTest(), neuralSparseQueryBuilder, 1).get("hits");
            assertNotNull(resultWith2PhasePipeline);
            assertNotNull(resultWithOut2PhasePipeline);
        } else {
            String modelId = null;
            try {
                modelId = TestUtils.getModelId(getIngestionPipeline(DENSE_INGEST_PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                loadModel(modelId);
                neuralSparseQueryBuilder.modelId(modelId);
                Object resultWith2PhasePipeline = search(getIndexNameForTest(), neuralSparseQueryBuilder, 1).get("hits");
                updateIndexSettings(getIndexNameForTest(), Settings.builder().put("index.search.default_pipeline", "_none"));
                Object resultWithOut2PhasePipeline= search(getIndexNameForTest(), neuralSparseQueryBuilder, 1).get("hits");
                assertNotNull(resultWith2PhasePipeline);
                assertNotNull(resultWithOut2PhasePipeline);
            } finally {
                wipeOfTestResources(getIndexNameForTest(), DENSE_INGEST_PIPELINE_NAME, modelId, NEURAL_SPARSE_TWO_PHASE_SEARCH_PIPELINE_NAME);
            }
        }
    }
}