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

public class NeuralSparseSearchIT extends AbstractRollingUpgradeTestCase {
    private static final String PIPELINE_NAME = "nlp-ingest-pipeline-sparse";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEXT = "Hello world";
    private static final String TEXT_MIXED = "Hi planet";
    private static final String TEXT_UPGRADED = "Hi earth";
    private static final int NUM_DOCS_PER_ROUND = 1;
    private static final String query = "Hi world";

    public void testNeuralSparseSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        switch (getClusterType()) {
            case OLD:
                String modelId = uploadTextEmbeddingModel();
                loadModel(modelId);
                createPipelineProcessor(modelId, PIPELINE_NAME);
                createIndexWithConfiguration(
                    getIndexNameForTest(),
                    Files.readString(Path.of(classLoader.getResource("processor/SparseIndexMappings.json").toURI())),
                    PIPELINE_NAME
                );
                addDocument(getIndexNameForTest(), "0", TEST_FIELD, TEXT, null, null);
                break;
            case MIXED:
                modelId = getModelId(PIPELINE_NAME);
                int totalDocsCountMixed;
                if (isFirstMixedRound()) {
                    totalDocsCountMixed = NUM_DOCS_PER_ROUND;
                    validateTestIndexOnUpgrade(totalDocsCountMixed, modelId);
                    addDocument(getIndexNameForTest(), "1", TEST_FIELD, TEXT_MIXED, null, null);
                } else {
                    totalDocsCountMixed = 2 * NUM_DOCS_PER_ROUND;
                    validateTestIndexOnUpgrade(totalDocsCountMixed, modelId);
                }
                break;
            case UPGRADED:
                modelId = getModelId(PIPELINE_NAME);
                int totalDocsCountUpgraded = 3 * NUM_DOCS_PER_ROUND;
                loadModel(modelId);
                addDocument(getIndexNameForTest(), "2", TEST_FIELD, TEXT_UPGRADED, null, null);
                validateTestIndexOnUpgrade(totalDocsCountUpgraded, modelId);
                deletePipeline(PIPELINE_NAME);
                deleteModel(modelId);
                deleteIndex(getIndexNameForTest());
                break;
        }
    }

    private void validateTestIndexOnUpgrade(int numberOfDocs, String modelId) throws Exception {
        int docCount = getDocCount(getIndexNameForTest());
        assertEquals(numberOfDocs, docCount);
        loadModel(modelId);
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder();
        neuralSparseQueryBuilder.fieldName("passage_embedding");
        neuralSparseQueryBuilder.queryText(query);
        neuralSparseQueryBuilder.modelId(modelId);
        Map<String, Object> response = search(getIndexNameForTest(), neuralSparseQueryBuilder, 1);
        assertNotNull(response);
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

    private String getModelId(String pipelineName) {
        Map<String, Object> pipeline = getIngestionPipeline(pipelineName);
        assertNotNull(pipeline);
        return TestUtils.getModelId(pipeline, SPARSE_ENCODING_PROCESSOR);
    }
}
