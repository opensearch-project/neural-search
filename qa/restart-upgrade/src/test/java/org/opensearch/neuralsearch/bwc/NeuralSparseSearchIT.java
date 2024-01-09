/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.opensearch.neuralsearch.TestUtils;
import static org.opensearch.neuralsearch.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.TestUtils.SPARSE_ENCODING_PROCESSOR;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;

public class NeuralSparseSearchIT extends AbstractRestartUpgradeRestTestCase {
    private static final String PIPELINE_NAME = "nlp-ingest-pipeline-sparse";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEXT_1 = "Hello world";
    private static final String TEXT_2 = "Hi planet";
    private static final String QUERY = "Hi world";

    // Test restart-upgrade test sparse embedding processor
    // Create Sparse Encoding Processor, Ingestion Pipeline and add document
    // Validate process , pipeline and document count in restart-upgrade scenario
    public void testSparseEncodingProcessor_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        if (isRunningAgainstOldCluster()) {
            String modelId = uploadTextEmbeddingModel();
            loadModel(modelId);
            createPipelineProcessor(modelId, PIPELINE_NAME);
            createIndexWithConfiguration(
                getIndexNameForTest(),
                Files.readString(Path.of(classLoader.getResource("processor/SparseIndexMappings.json").toURI())),
                PIPELINE_NAME
            );
            addDocument(getIndexNameForTest(), "0", TEST_FIELD, TEXT_1, null, null);
        } else {
            Map<String, Object> pipeline = getIngestionPipeline(PIPELINE_NAME);
            assertNotNull(pipeline);
            String modelId = TestUtils.getModelId(pipeline, SPARSE_ENCODING_PROCESSOR);
            loadModel(modelId);
            addDocument(getIndexNameForTest(), "1", TEST_FIELD, TEXT_2, null, null);
            validateTestIndex(modelId);
            deletePipeline(PIPELINE_NAME);
            deleteModel(modelId);
            deleteIndex(getIndexNameForTest());
        }

    }

    private void validateTestIndex(String modelId) throws Exception {
        int docCount = getDocCount(getIndexNameForTest());
        assertEquals(2, docCount);
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder();
        neuralSparseQueryBuilder.fieldName("passage_embedding");
        neuralSparseQueryBuilder.queryText(QUERY);
        neuralSparseQueryBuilder.modelId(modelId);
        Map<String, Object> response = search(getIndexNameForTest(), neuralSparseQueryBuilder, 1);
        assertNotNull(response);
        int hits = getHitCount(response);
        assertEquals(2, hits);
    }

    private String uploadTextEmbeddingModel() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        return registerModelGroupAndGetModelId(requestBody);
    }

    private String registerModelGroupAndGetModelId(String requestBody) throws Exception {
        String modelGroupRegisterRequestBody = Files.readString(
            Path.of(classLoader.getResource("processor/CreateModelGroupRequestBody.json").toURI())
        );
        String modelGroupId = registerModelGroup(
            String.format(LOCALE, modelGroupRegisterRequestBody, "public_model_" + RandomizedTest.randomAsciiAlphanumOfLength(8))
        );
        return uploadModel(String.format(LOCALE, requestBody, modelGroupId));
    }

    private void createPipelineProcessor(String modelId, String pipelineName) throws Exception {
        String requestBody = Files.readString(
            Path.of(classLoader.getResource("processor/PipelineForSparseEncodingProcessorConfiguration.json").toURI())
        );
        createPipelineProcessor(requestBody, pipelineName, modelId);
    }
}
