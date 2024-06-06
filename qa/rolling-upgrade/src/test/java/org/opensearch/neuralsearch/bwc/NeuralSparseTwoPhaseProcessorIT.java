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
import java.util.List;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.SPARSE_ENCODING_PROCESSOR;

public class NeuralSparseTwoPhaseProcessorIT extends AbstractRollingUpgradeTestCase{
    // add prefix to avoid conflicts with other IT class, since don't wipe resources after first round
    private static final String SPARSE_INGEST_PIPELINE_NAME = "nstp-nlp-ingest-pipeline-sparse";
    private static final String SPARSE_SEARCH_TWO_PHASE_PIPELINE_NAME = "nstp-nlp-two-phase-search-pipeline-sparse";
    private static final String TEST_ENCODING_FIELD = "passage_embedding";
    private static final String TEST_TEXT_FIELD = "passage_text";
    private static final String TEXT_1 = "Hello world a b";
    private String sparseModelId = "";

    // test of NeuralSparseTwoPhaseProcessor supports neural_sparse query's two phase speed up
    // the feature is introduced from 2.15
    public void testNeuralSparseTwoPhaseProcessorIT_NeuralSparseSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        // will set the model_id after we obtain the id
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_ENCODING_FIELD)
            .queryText(TEXT_1);

        switch (getClusterType()) {
            case OLD:
                sparseModelId = uploadSparseEncodingModel();
                loadModel(sparseModelId);
                neuralSparseQueryBuilder.modelId(sparseModelId);
                createPipelineForSparseEncodingProcessor(sparseModelId, SPARSE_INGEST_PIPELINE_NAME);
                createIndexWithConfiguration(
                    getIndexNameForTest(),
                    Files.readString(Path.of(classLoader.getResource("processor/SparseIndexMappings.json").toURI())),
                    SPARSE_INGEST_PIPELINE_NAME
                );
                addSparseEncodingDoc(getIndexNameForTest(), "0", List.of(), List.of(), List.of(TEST_TEXT_FIELD), List.of(TEXT_1));
                createNeuralSparseTwoPhaseSearchProcessor(SPARSE_SEARCH_TWO_PHASE_PIPELINE_NAME);
                updateIndexSettings(
                    getIndexNameForTest(),
                    Settings.builder().put("index.search.default_pipeline", SPARSE_SEARCH_TWO_PHASE_PIPELINE_NAME)
                );
                assertNotNull(search(getIndexNameForTest(), neuralSparseQueryBuilder, 1).get("hits"));
                break;
            case MIXED:
                sparseModelId = TestUtils.getModelId(getIngestionPipeline(SPARSE_INGEST_PIPELINE_NAME), SPARSE_ENCODING_PROCESSOR);
                loadModel(sparseModelId);
                neuralSparseQueryBuilder.modelId(sparseModelId);
                assertNotNull(search(getIndexNameForTest(), neuralSparseQueryBuilder, 1).get("hits"));
                break;
            case UPGRADED:
                try {
                    sparseModelId = TestUtils.getModelId(getIngestionPipeline(SPARSE_INGEST_PIPELINE_NAME), SPARSE_ENCODING_PROCESSOR);
                    loadModel(sparseModelId);
                    neuralSparseQueryBuilder.modelId(sparseModelId);
                    assertNotNull(search(getIndexNameForTest(), neuralSparseQueryBuilder, 1).get("hits"));
                } finally {
                    wipeOfTestResources(getIndexNameForTest(), SPARSE_INGEST_PIPELINE_NAME, sparseModelId, SPARSE_SEARCH_TWO_PHASE_PIPELINE_NAME);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }
    }
}
