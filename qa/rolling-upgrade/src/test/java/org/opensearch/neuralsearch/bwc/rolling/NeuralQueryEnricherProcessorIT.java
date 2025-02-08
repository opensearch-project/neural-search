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

    // test of NeuralQueryEnricherProcessor supports neural_sparse query default model_id
    // the feature is introduced from 2.13
    public void testNeuralQueryEnricherProcessor_NeuralSparseSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        NeuralSparseQueryBuilder sparseEncodingQueryBuilderWithoutModelId = new NeuralSparseQueryBuilder().fieldName(TEST_ENCODING_FIELD)
            .queryText(TEXT_1);
        // will set the model_id after we obtain the id
        NeuralSparseQueryBuilder sparseEncodingQueryBuilderWithModelId = new NeuralSparseQueryBuilder().fieldName(TEST_ENCODING_FIELD)
            .queryText(TEXT_1);
        super.ingestPipelineName = SPARSE_INGEST_PIPELINE_NAME;
        super.searchPipelineName = SPARSE_SEARCH_PIPELINE_NAME;

        switch (getClusterType()) {
            case OLD:
                super.modelId = uploadSparseEncodingModel();
                loadModel(super.modelId);
                sparseEncodingQueryBuilderWithModelId.modelId(super.modelId);
                createPipelineForSparseEncodingProcessor(super.modelId, SPARSE_INGEST_PIPELINE_NAME);
                createIndexWithConfiguration(
                    super.indexName,
                    Files.readString(Path.of(classLoader.getResource("processor/SparseIndexMappings.json").toURI())),
                    SPARSE_INGEST_PIPELINE_NAME
                );

                addSparseEncodingDoc(super.indexName, "0", List.of(), List.of(), List.of(TEST_TEXT_FIELD), List.of(TEXT_1));
                createSearchRequestProcessor(super.modelId, SPARSE_SEARCH_PIPELINE_NAME);
                updateIndexSettings(super.indexName, Settings.builder().put("index.search.default_pipeline", SPARSE_SEARCH_PIPELINE_NAME));
                assertEquals(
                    search(super.indexName, sparseEncodingQueryBuilderWithoutModelId, 1).get("hits"),
                    search(super.indexName, sparseEncodingQueryBuilderWithModelId, 1).get("hits")
                );
                break;
            case MIXED:
                super.modelId = TestUtils.getModelId(getIngestionPipeline(SPARSE_INGEST_PIPELINE_NAME), SPARSE_ENCODING_PROCESSOR);
                loadModel(super.modelId);
                sparseEncodingQueryBuilderWithModelId.modelId(super.modelId);

                assertEquals(
                    search(super.indexName, sparseEncodingQueryBuilderWithoutModelId, 1).get("hits"),
                    search(super.indexName, sparseEncodingQueryBuilderWithModelId, 1).get("hits")
                );
                break;
            case UPGRADED:
                super.modelId = TestUtils.getModelId(getIngestionPipeline(SPARSE_INGEST_PIPELINE_NAME), SPARSE_ENCODING_PROCESSOR);
                loadModel(super.modelId);
                sparseEncodingQueryBuilderWithModelId.modelId(super.modelId);
                assertEquals(
                    search(super.indexName, sparseEncodingQueryBuilderWithoutModelId, 1).get("hits"),
                    search(super.indexName, sparseEncodingQueryBuilderWithModelId, 1).get("hits")
                );
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
        super.ingestPipelineName = DENSE_INGEST_PIPELINE_NAME;
        super.searchPipelineName = DENSE_SEARCH_PIPELINE_NAME;

        switch (getClusterType()) {
            case OLD:
                super.modelId = uploadTextEmbeddingModel();
                loadModel(super.modelId);
                neuralQueryBuilderWithModelId.modelId(super.modelId);
                createPipelineProcessor(super.modelId, DENSE_INGEST_PIPELINE_NAME);
                createIndexWithConfiguration(
                    super.indexName,
                    Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
                    DENSE_INGEST_PIPELINE_NAME
                );

                addDocument(super.indexName, "0", TEST_TEXT_FIELD, TEXT_1, null, null);

                createSearchRequestProcessor(super.modelId, DENSE_SEARCH_PIPELINE_NAME);
                updateIndexSettings(super.indexName, Settings.builder().put("index.search.default_pipeline", DENSE_SEARCH_PIPELINE_NAME));
                assertEquals(
                    search(super.indexName, neuralQueryBuilderWithoutModelId, 1).get("hits"),
                    search(super.indexName, neuralQueryBuilderWithModelId, 1).get("hits")
                );
                break;
            case MIXED:
                super.modelId = TestUtils.getModelId(getIngestionPipeline(DENSE_INGEST_PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                loadModel(super.modelId);
                neuralQueryBuilderWithModelId.modelId(super.modelId);

                assertEquals(
                    search(super.indexName, neuralQueryBuilderWithoutModelId, 1).get("hits"),
                    search(super.indexName, neuralQueryBuilderWithModelId, 1).get("hits")
                );
                break;
            case UPGRADED:
                super.modelId = TestUtils.getModelId(getIngestionPipeline(DENSE_INGEST_PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                loadModel(super.modelId);
                neuralQueryBuilderWithModelId.modelId(super.modelId);

                assertEquals(
                    search(super.indexName, neuralQueryBuilderWithoutModelId, 1).get("hits"),
                    search(super.indexName, neuralQueryBuilderWithModelId, 1).get("hits")
                );
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }
    }
}
