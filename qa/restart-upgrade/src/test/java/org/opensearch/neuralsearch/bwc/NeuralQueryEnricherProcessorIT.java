/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc;

import static org.opensearch.neuralsearch.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.TestUtils.SPARSE_ENCODING_PROCESSOR;
import static org.opensearch.neuralsearch.TestUtils.TEXT_EMBEDDING_PROCESSOR;

import org.opensearch.Version;
import org.opensearch.client.ResponseException;
import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.TestUtils;
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
    private static final String TEXT_2 = "Hello planet";

    // Test restart-upgrade neural_query_enricher in restart-upgrade scenario
    public void testNeuralQueryEnricherProcessor_NeuralSparseSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        Version bwcVersion = parseVersionFromString(getBWCVersion().get());
        logger.info("bwc version: " + bwcVersion.toString());

        if (isRunningAgainstOldCluster()) {
            String modelId = uploadSparseEncodingModel();
            loadModel(modelId);
            createPipelineForSparseEncodingProcessor(modelId, SPARSE_INGEST_PIPELINE_NAME);
            createIndexWithConfiguration(
                getIndexNameForTest(),
                Files.readString(Path.of(classLoader.getResource("processor/SparseIndexMappings.json").toURI())),
                SPARSE_INGEST_PIPELINE_NAME
            );

            addSparseEncodingDoc(getIndexNameForTest(), "0", List.of(), List.of(), List.of(TEST_TEXT_FIELD), List.of(TEXT_1));

            createSearchRequestProcessor(modelId, SPARSE_SEARCH_PIPELINE_NAME);
            updateIndexSettings(
                getIndexNameForTest(),
                Settings.builder().put("index.search.default_pipeline", SPARSE_SEARCH_PIPELINE_NAME)
            );
            if (bwcVersion.onOrAfter(Version.V_2_13_0)) {
                // after we support default model id in neural_sparse query
                // do nothing here. need to add test codes after finishing backport
                ;
            } else {
                // before we support default model id in neural_sparse query
                NeuralSparseQueryBuilder sparseEncodingQueryBuilderWithoutModelId = new NeuralSparseQueryBuilder().fieldName(
                    TEST_ENCODING_FIELD
                ).queryText(TEXT_1);

                expectThrows(ResponseException.class, () -> search(getIndexNameForTest(), sparseEncodingQueryBuilderWithoutModelId, 1));
            }
        } else {
            String modelId = null;
            try {
                modelId = TestUtils.getModelId(getIngestionPipeline(SPARSE_INGEST_PIPELINE_NAME), SPARSE_ENCODING_PROCESSOR);
                loadModel(modelId);
                NeuralSparseQueryBuilder sparseEncodingQueryBuilderWithoutModelId = new NeuralSparseQueryBuilder().fieldName(
                    TEST_ENCODING_FIELD
                ).queryText(TEXT_1);
                NeuralSparseQueryBuilder sparseEncodingQueryBuilderWithModelId = new NeuralSparseQueryBuilder().fieldName(
                    TEST_ENCODING_FIELD
                ).queryText(TEXT_1).modelId(modelId);
                assertEquals(
                    search(getIndexNameForTest(), sparseEncodingQueryBuilderWithoutModelId, 1).get("hits"),
                    search(getIndexNameForTest(), sparseEncodingQueryBuilderWithModelId, 1).get("hits")
                );
            } finally {
                wipeOfTestResources(getIndexNameForTest(), SPARSE_INGEST_PIPELINE_NAME, modelId, SPARSE_SEARCH_PIPELINE_NAME);
            }
        }
    }

    public void testNeuralQueryEnricherProcessor_NeuralSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        Version bwcVersion = parseVersionFromString(getBWCVersion().get());
        logger.info("bwc version: " + bwcVersion.toString());

        if (isRunningAgainstOldCluster()) {
            String modelId = uploadTextEmbeddingModel();
            loadModel(modelId);
            createPipelineProcessor(modelId, DENSE_INGEST_PIPELINE_NAME);
            createIndexWithConfiguration(
                getIndexNameForTest(),
                Files.readString(Path.of(classLoader.getResource("processor/IndexMappingMultipleShard.json").toURI())),
                DENSE_INGEST_PIPELINE_NAME
            );

            addDocument(getIndexNameForTest(), "0", TEST_TEXT_FIELD, TEXT_1, null, null);

            createSearchRequestProcessor(modelId, DENSE_SEARCH_PIPELINE_NAME);
            updateIndexSettings(getIndexNameForTest(), Settings.builder().put("index.search.default_pipeline", DENSE_SEARCH_PIPELINE_NAME));

            NeuralQueryBuilder neuralQueryBuilderWithoutModelId = new NeuralQueryBuilder().fieldName(TEST_ENCODING_FIELD).queryText(TEXT_1);
            NeuralQueryBuilder neuralQueryBuilderWithModelId = new NeuralQueryBuilder().fieldName(TEST_ENCODING_FIELD)
                .queryText(TEXT_1)
                .modelId(modelId);
            assertEquals(
                search(getIndexNameForTest(), neuralQueryBuilderWithoutModelId, 1).get("hits"),
                search(getIndexNameForTest(), neuralQueryBuilderWithModelId, 1).get("hits")
            );
        } else {
            String modelId = null;
            try {
                modelId = TestUtils.getModelId(getIngestionPipeline(DENSE_INGEST_PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                loadModel(modelId);

                NeuralQueryBuilder neuralQueryBuilderWithoutModelId = new NeuralQueryBuilder().fieldName(TEST_ENCODING_FIELD)
                    .queryText(TEXT_1);
                NeuralQueryBuilder neuralQueryBuilderWithModelId = new NeuralQueryBuilder().fieldName(TEST_ENCODING_FIELD)
                    .queryText(TEXT_1)
                    .modelId(modelId);
                assertEquals(
                    search(getIndexNameForTest(), neuralQueryBuilderWithoutModelId, 1).get("hits"),
                    search(getIndexNameForTest(), neuralQueryBuilderWithModelId, 1).get("hits")
                );
            } finally {
                wipeOfTestResources(getIndexNameForTest(), SPARSE_INGEST_PIPELINE_NAME, modelId, SPARSE_SEARCH_PIPELINE_NAME);
            }
        }
    }
}