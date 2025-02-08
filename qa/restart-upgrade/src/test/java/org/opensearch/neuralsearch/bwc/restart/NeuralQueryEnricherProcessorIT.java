/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.restart;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.SPARSE_ENCODING_PROCESSOR;
import static org.opensearch.neuralsearch.util.TestUtils.TEXT_EMBEDDING_PROCESSOR;

import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.util.TestUtils;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class NeuralQueryEnricherProcessorIT extends AbstractRestartUpgradeRestTestCase {
    // add prefix to avoid conflicts with other IT class, since we don't wipe resources after first round
    private static final String SPARSE_INGEST_PIPELINE_NAME = "nqep-nlp-ingest-pipeline-sparse";
    private static final String DENSE_INGEST_PIPELINE_NAME = "nqep-nlp-ingest-pipeline-dense";
    private static final String SPARSE_SEARCH_PIPELINE_NAME = "nqep-nlp-search-pipeline-sparse";
    private static final String DENSE_SEARCH_PIPELINE_NAME = "nqep-nlp-search-pipeline-dense";
    private static final String TEST_ENCODING_FIELD = "passage_embedding";
    private static final String TEST_TEXT_FIELD = "passage_text";
    private static final String TEXT_1 = "Hello world a b";

    // Test restart-upgrade neural_query_enricher in restart-upgrade scenario
    public void testNeuralQueryEnricherProcessor_NeuralSparseSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        NeuralSparseQueryBuilder sparseEncodingQueryBuilderWithoutModelId = new NeuralSparseQueryBuilder().fieldName(TEST_ENCODING_FIELD)
            .queryText(TEXT_1);
        // will set the model_id after we obtain the id
        NeuralSparseQueryBuilder sparseEncodingQueryBuilderWithModelId = new NeuralSparseQueryBuilder().fieldName(TEST_ENCODING_FIELD)
            .queryText(TEXT_1);
        super.ingestPipelineName = SPARSE_INGEST_PIPELINE_NAME;
        super.searchPipelineName = SPARSE_SEARCH_PIPELINE_NAME;

        if (isRunningAgainstOldCluster()) {
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
        } else {
            super.modelId = TestUtils.getModelId(getIngestionPipeline(SPARSE_INGEST_PIPELINE_NAME), SPARSE_ENCODING_PROCESSOR);
            loadModel(super.modelId);
            sparseEncodingQueryBuilderWithModelId.modelId(super.modelId);
            assertEquals(
                search(super.indexName, sparseEncodingQueryBuilderWithoutModelId, 1).get("hits"),
                search(super.indexName, sparseEncodingQueryBuilderWithModelId, 1).get("hits")
            );
        }
    }

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
        this.ingestPipelineName = DENSE_INGEST_PIPELINE_NAME;
        this.searchPipelineName = DENSE_SEARCH_PIPELINE_NAME;

        if (isRunningAgainstOldCluster()) {
            super.modelId = uploadTextEmbeddingModel();
            loadModel(super.modelId);
            neuralQueryBuilderWithModelId.modelId(super.modelId);
            createPipelineProcessor(super.modelId, DENSE_INGEST_PIPELINE_NAME);
            createIndexWithConfiguration(
                super.indexName,
                Files.readString(Path.of(classLoader.getResource("processor/IndexMappingMultipleShard.json").toURI())),
                DENSE_INGEST_PIPELINE_NAME
            );

            addDocument(super.indexName, "0", TEST_TEXT_FIELD, TEXT_1, null, null);

            createSearchRequestProcessor(super.modelId, DENSE_SEARCH_PIPELINE_NAME);
            updateIndexSettings(super.indexName, Settings.builder().put("index.search.default_pipeline", DENSE_SEARCH_PIPELINE_NAME));
            assertEquals(
                search(super.indexName, neuralQueryBuilderWithoutModelId, 1).get("hits"),
                search(super.indexName, neuralQueryBuilderWithModelId, 1).get("hits")
            );
        } else {
            super.modelId = TestUtils.getModelId(getIngestionPipeline(DENSE_INGEST_PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
            loadModel(super.modelId);
            neuralQueryBuilderWithModelId.modelId(super.modelId);

            assertEquals(
                search(super.indexName, neuralQueryBuilderWithoutModelId, 1).get("hits"),
                search(super.indexName, neuralQueryBuilderWithModelId, 1).get("hits")
            );
        }
    }
}
