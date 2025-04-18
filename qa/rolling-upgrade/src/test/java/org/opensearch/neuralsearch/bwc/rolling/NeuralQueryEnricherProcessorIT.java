/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling;

import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.util.TestUtils;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.SPARSE_ENCODING_PROCESSOR;
import static org.opensearch.neuralsearch.util.TestUtils.TEXT_EMBEDDING_PROCESSOR;

public class NeuralQueryEnricherProcessorIT extends AbstractRollingUpgradeTestCase {
    // add prefix to avoid conflicts with other IT class, since we don't wipe resources after first round
    private static final String SPARSE_INGEST_PIPELINE_NAME = "nqep-nlp-ingest-pipeline-sparse";
    private static final String DENSE_INGEST_PIPELINE_NAME = "nqep-nlp-ingest-pipeline-dense";
    private static final String SPARSE_SEARCH_PIPELINE_NAME = "nqep-nlp-search-pipeline-sparse";
    private static final String DENSE_SEARCH_PIPELINE_NAME = "nqep-nlp-search-pipeline-dense";
    private static final String TEST_ENCODING_FIELD = "passage_embedding";
    private static final String TEST_TEXT_FIELD = "passage_text";
    private static final String TEXT_1 = "Hello world a b";
    private String sparseModelId = "";
    private String denseModelId = "";

    // test of NeuralQueryEnricherProcessor supports neural_sparse query default model_id
    // the feature is introduced from 2.13
    public void testNeuralQueryEnricherProcessor_NeuralSparseSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        NeuralSparseQueryBuilder sparseEncodingQueryBuilderWithoutModelId = new NeuralSparseQueryBuilder().fieldName(TEST_ENCODING_FIELD)
            .queryText(TEXT_1);
        // will set the model_id after we obtain the id
        NeuralSparseQueryBuilder sparseEncodingQueryBuilderWithModelId = new NeuralSparseQueryBuilder().fieldName(TEST_ENCODING_FIELD)
            .queryText(TEXT_1);

        switch (getClusterType()) {
            case OLD:
                sparseModelId = uploadSparseEncodingModel();
                loadModel(sparseModelId);
                sparseEncodingQueryBuilderWithModelId.modelId(sparseModelId);
                createPipelineForSparseEncodingProcessor(sparseModelId, SPARSE_INGEST_PIPELINE_NAME);
                createIndexWithConfiguration(
                    getIndexNameForTest(),
                    Files.readString(Path.of(classLoader.getResource("processor/SparseIndexMappings.json").toURI())),
                    SPARSE_INGEST_PIPELINE_NAME
                );

                addSparseEncodingDoc(getIndexNameForTest(), "0", List.of(), List.of(), List.of(TEST_TEXT_FIELD), List.of(TEXT_1));
                createSearchRequestProcessor(sparseModelId, SPARSE_SEARCH_PIPELINE_NAME);
                updateIndexSettings(
                    getIndexNameForTest(),
                    Settings.builder().put("index.search.default_pipeline", SPARSE_SEARCH_PIPELINE_NAME)
                );
                assertEquals(
                    search(getIndexNameForTest(), sparseEncodingQueryBuilderWithoutModelId, 1).get("hits"),
                    search(getIndexNameForTest(), sparseEncodingQueryBuilderWithModelId, 1).get("hits")
                );
                break;
            case MIXED:
                sparseModelId = TestUtils.getModelId(getIngestionPipeline(SPARSE_INGEST_PIPELINE_NAME), SPARSE_ENCODING_PROCESSOR);
                loadModel(sparseModelId);
                sparseEncodingQueryBuilderWithModelId.modelId(sparseModelId);

                waitForClusterHealthGreen(NODES_BWC_CLUSTER);
                assertEquals(
                    search(getIndexNameForTest(), sparseEncodingQueryBuilderWithoutModelId, 1).get("hits"),
                    search(getIndexNameForTest(), sparseEncodingQueryBuilderWithModelId, 1).get("hits")
                );
                break;
            case UPGRADED:
                try {
                    sparseModelId = TestUtils.getModelId(getIngestionPipeline(SPARSE_INGEST_PIPELINE_NAME), SPARSE_ENCODING_PROCESSOR);
                    loadModel(sparseModelId);
                    sparseEncodingQueryBuilderWithModelId.modelId(sparseModelId);
                    assertEquals(
                        search(getIndexNameForTest(), sparseEncodingQueryBuilderWithoutModelId, 1).get("hits"),
                        search(getIndexNameForTest(), sparseEncodingQueryBuilderWithModelId, 1).get("hits")
                    );
                } finally {
                    wipeOfTestResources(getIndexNameForTest(), SPARSE_INGEST_PIPELINE_NAME, sparseModelId, SPARSE_SEARCH_PIPELINE_NAME);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }
    }

    // test of NeuralQueryEnricherProcessor supports neural query default model_id
    // the feature is introduced from 2.11
    public void testNeuralQueryEnricherProcessor_NeuralSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        NeuralQueryBuilder neuralQueryBuilderWithoutModelId = NeuralQueryBuilder.builder()
            .fieldName(TEST_ENCODING_FIELD)
            .queryText(TEXT_1)
            .build();
        NeuralQueryBuilder neuralQueryBuilderWithModelId = NeuralQueryBuilder.builder()
            .fieldName(TEST_ENCODING_FIELD)
            .queryText(TEXT_1)
            .build();

        switch (getClusterType()) {
            case OLD:
                denseModelId = uploadTextEmbeddingModel();
                loadModel(denseModelId);
                neuralQueryBuilderWithModelId.modelId(denseModelId);
                createPipelineProcessor(denseModelId, DENSE_INGEST_PIPELINE_NAME);
                createIndexWithConfiguration(
                    getIndexNameForTest(),
                    Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
                    DENSE_INGEST_PIPELINE_NAME
                );

                addDocument(getIndexNameForTest(), "0", TEST_TEXT_FIELD, TEXT_1, null, null);

                createSearchRequestProcessor(denseModelId, DENSE_SEARCH_PIPELINE_NAME);
                updateIndexSettings(
                    getIndexNameForTest(),
                    Settings.builder().put("index.search.default_pipeline", DENSE_SEARCH_PIPELINE_NAME)
                );
                assertEquals(
                    search(getIndexNameForTest(), neuralQueryBuilderWithoutModelId, 1).get("hits"),
                    search(getIndexNameForTest(), neuralQueryBuilderWithModelId, 1).get("hits")
                );
                break;
            case MIXED:
                denseModelId = TestUtils.getModelId(getIngestionPipeline(DENSE_INGEST_PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                loadModel(denseModelId);
                neuralQueryBuilderWithModelId.modelId(denseModelId);

                waitForClusterHealthGreen(NODES_BWC_CLUSTER);
                assertEquals(
                    search(getIndexNameForTest(), neuralQueryBuilderWithoutModelId, 1).get("hits"),
                    search(getIndexNameForTest(), neuralQueryBuilderWithModelId, 1).get("hits")
                );
                break;
            case UPGRADED:
                try {
                    denseModelId = TestUtils.getModelId(getIngestionPipeline(DENSE_INGEST_PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                    loadModel(denseModelId);
                    neuralQueryBuilderWithModelId.modelId(denseModelId);

                    assertEquals(
                        search(getIndexNameForTest(), neuralQueryBuilderWithoutModelId, 1).get("hits"),
                        search(getIndexNameForTest(), neuralQueryBuilderWithModelId, 1).get("hits")
                    );
                } finally {
                    wipeOfTestResources(getIndexNameForTest(), DENSE_INGEST_PIPELINE_NAME, denseModelId, DENSE_SEARCH_PIPELINE_NAME);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }
    }
}
