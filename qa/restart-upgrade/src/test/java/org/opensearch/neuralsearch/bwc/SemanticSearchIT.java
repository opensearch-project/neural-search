/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.opensearch.neuralsearch.TestUtils.NODES_BWC_CLUSTER;

import static org.opensearch.neuralsearch.TestUtils.getModelId;
import static org.opensearch.neuralsearch.TestUtils.TEXT_EMBEDDING_PROCESSOR;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

public class SemanticSearchIT extends AbstractRestartUpgradeRestTestCase {

    private static final String PIPELINE_NAME = "nlp-pipeline";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEXT = "Hello world";
    private static final String TEXT_1 = "Hello world a";

    // Test restart-upgrade Semantic Search
    // Create Text Embedding Processor, Ingestion Pipeline and add document
    // Validate process , pipeline and document count in restart-upgrade scenario
    public void testSemanticSearch() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);

        if (isRunningAgainstOldCluster()) {
            String modelId = uploadTextEmbeddingModel();
            loadModel(modelId);
            createPipelineProcessor(modelId, PIPELINE_NAME);
            createIndexWithConfiguration(
                getIndexNameForTest(),
                Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
                PIPELINE_NAME
            );
            addDocument(getIndexNameForTest(), "0", TEST_FIELD, TEXT);
        } else {
            Map<String, Object> pipeline = getIngestionPipeline(PIPELINE_NAME);
            assertNotNull(pipeline);
            String modelId = getModelId(pipeline, TEXT_EMBEDDING_PROCESSOR);
            addDocument(getIndexNameForTest(), "1", TEST_FIELD, TEXT_1);
            validateTestIndex(modelId);
            deletePipeline(PIPELINE_NAME);
            deleteModel(modelId);
            deleteIndex(getIndexNameForTest());
        }
    }

    private void validateTestIndex(String modelId) throws Exception {
        int docCount = getDocCount(getIndexNameForTest());
        assertEquals(2, docCount);
        loadModel(modelId);
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder();
        neuralQueryBuilder.fieldName("passage_embedding");
        neuralQueryBuilder.modelId(modelId);
        neuralQueryBuilder.queryText(TEXT);
        neuralQueryBuilder.k(1);
        Map<String, Object> response = search(getIndexNameForTest(), neuralQueryBuilder, 1);
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
        return uploadModelId(String.format(LOCALE, requestBody, modelGroupId));
    }

    protected void createPipelineProcessor(String modelId, String pipelineName, ProcessorType processorType) throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/PipelineConfiguration.json").toURI()));
        createPipelineProcessor(requestBody, pipelineName, modelId);
    }

}
