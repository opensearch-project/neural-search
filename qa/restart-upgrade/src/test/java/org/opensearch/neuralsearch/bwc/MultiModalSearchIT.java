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
import static org.opensearch.neuralsearch.TestUtils.TEXT_IMAGE_EMBEDDING_PROCESSOR;
import static org.opensearch.neuralsearch.TestUtils.getModelId;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

public class MultiModalSearchIT extends AbstractRestartUpgradeRestTestCase{
    private static final String PIPELINE_NAME = "nlp-ingest-pipeline";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEST_IMAGE_FIELD = "passage_image";
    private static final String TEXT = "Hello world";
    private static final String TEXT_1 = "Hello world a";
    private static final String TEST_IMAGE_TEXT = "/9j/4AAQSkZJRgABAQAASABIAAD";
    private static final String TEST_IMAGE_TEXT_1 = "/9j/4AAQSkZJRgbdwoeicfhoid";

    // Test restart-upgrade MultiModal Search
    // Create Text Image Embedding Processor, Ingestion Pipeline and add document
    // Validate process , pipeline and document count in restart-upgrade scenario
    public void testMultiModalSearch_E2EFlow() throws Exception{
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);

        if (isRunningAgainstOldCluster()){
           String modelId= uploadTextImageEmbeddingModel();
           loadModel(modelId);
           createPipelineProcessor(modelId, PIPELINE_NAME);
           createIndexWithConfiguration(
                    getIndexNameForTest(),
                    Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
                    PIPELINE_NAME
           );
           addDocument(getIndexNameForTest(),"0",TEST_FIELD,TEXT,TEST_IMAGE_FIELD,TEST_IMAGE_TEXT);
        }else {
            Map<String, Object> pipeline = getIngestionPipeline(PIPELINE_NAME);
            assertNotNull(pipeline);
            String modelId = getModelId(pipeline, TEXT_IMAGE_EMBEDDING_PROCESSOR);
            loadModel(modelId);
            addDocument(getIndexNameForTest(), "1", TEST_FIELD, TEXT_1,TEST_IMAGE_FIELD,TEST_IMAGE_TEXT_1);
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
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
                "passage_embedding",
                TEXT,
                TEST_IMAGE_TEXT,
                modelId,
                1,
                null,
                null
        );
        Map<String, Object> response = search(getIndexNameForTest(), neuralQueryBuilder, 1);
        assertNotNull(response);
    }

    private String uploadTextImageEmbeddingModel() throws Exception {
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

    protected void createPipelineProcessor(String modelId, String pipelineName) throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/PipelineForTextImagingProcessorConfiguration.json").toURI()));
        createPipelineProcessor(requestBody, pipelineName, modelId);
    }

}
