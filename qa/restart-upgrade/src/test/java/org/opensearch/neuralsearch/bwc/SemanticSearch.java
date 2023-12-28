/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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

public class SemanticSearch extends AbstractRestartUpgradeRestTestCase {

    private static final String PIPELINE_NAME = "nlp-pipeline";
    private static String DOC_ID = "0";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEXT = "Hello world";

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
                testIndex,
                Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
                PIPELINE_NAME
            );
            addDocument(testIndex, DOC_ID, TEST_FIELD, TEXT);
        } else {
            Map<String, Object> pipeline = getIngestionPipeline(PIPELINE_NAME);
            assertNotNull(pipeline);
            String modelId = getModelId(pipeline, TEXT_EMBEDDING_PROCESSOR);
            validateTestIndex(modelId);
            deletePipeline(PIPELINE_NAME);
            deleteModel(modelId);
            deleteIndex(testIndex);
        }
    }

    private void validateTestIndex(String modelId) throws Exception {
        int docCount = getDocCount(testIndex);
        assertEquals(1, docCount);
        loadModel(modelId);
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder();
        neuralQueryBuilder.fieldName("passage_embedding");
        neuralQueryBuilder.modelId(modelId);
        neuralQueryBuilder.queryText(TEXT);
        neuralQueryBuilder.k(1);
        Map<String, Object> response = search(testIndex, neuralQueryBuilder, 1);
        assertNotNull(response);
    }

    private String uploadTextEmbeddingModel() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        return registerModelGroupAndGetModelId(requestBody);
    }

    private String registerModelGroupAndGetModelId(String requestBody) throws Exception {
        String modelGroupRegisterRequestBody = Files.readString(
            Path.of(classLoader.getResource("processor/CreateModelGroupRequestBody.json").toURI())
        ).replace("<MODEL_GROUP_NAME>", "public_model_" + RandomizedTest.randomAsciiAlphanumOfLength(8));

        String modelGroupId = registerModelGroup(modelGroupRegisterRequestBody);

        requestBody = requestBody.replace("<MODEL_GROUP_ID>", modelGroupId);

        return uploadModelId(requestBody);
    }

    protected void createPipelineProcessor(String modelId, String pipelineName, ProcessorType processorType) throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/PipelineConfiguration.json").toURI()));
        createPipelineProcessor(requestBody, pipelineName, modelId);
    }

}
