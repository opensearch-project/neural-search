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
import static org.opensearch.neuralsearch.TestUtils.TEXT_IMAGE_EMBEDDING_PROCESSOR;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

public class MultiModalSearchIT extends AbstractRollingUpgradeTestCase {
    private static final String PIPELINE_NAME = "nlp-ingest-pipeline";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEST_IMAGE_FIELD = "passage_image";
    private static final String TEXT = "Hello world";
    private static final String TEXT_MIXED = "Hello world mixed";
    private static final String TEXT_UPGRADED = "Hello world upgraded";
    private static final String TEST_IMAGE_TEXT = "/9j/4AAQSkZJRgABAQAASABIAAD";
    private static final String TEST_IMAGE_TEXT_MIXED = "/9j/4AAQSkZJRgbdwoeicfhoid";
    private static final String TEST_IMAGE_TEXT_UPGRADED = "/9j/4AAQSkZJR8eydhgfwceocvlk";

    private static final int NUM_DOCS_PER_ROUND = 1;

    public void testMultiModalSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        switch (getClusterType()) {
            case OLD:
                String modelId = uploadTextImageEmbeddingModel();
                loadModel(modelId);
                createPipelineProcessor(modelId, PIPELINE_NAME);
                createIndexWithConfiguration(
                    getIndexNameForTest(),
                    Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
                    PIPELINE_NAME
                );
                addDocument(getIndexNameForTest(), "0", TEST_FIELD, TEXT, TEST_IMAGE_FIELD, TEST_IMAGE_TEXT);
                break;
            case MIXED:
                modelId = getModelId(PIPELINE_NAME);
                int totalDocsCountMixed;
                if (isFirstMixedRound()) {
                    totalDocsCountMixed = NUM_DOCS_PER_ROUND;
                    validateTestIndexOnUpgrade(totalDocsCountMixed, modelId, TEXT, TEST_IMAGE_TEXT);
                    addDocument(getIndexNameForTest(), "1", TEST_FIELD, TEXT_MIXED, TEST_IMAGE_FIELD, TEST_IMAGE_TEXT_MIXED);
                } else {
                    totalDocsCountMixed = 2 * NUM_DOCS_PER_ROUND;
                    validateTestIndexOnUpgrade(totalDocsCountMixed, modelId, TEXT_MIXED, TEST_IMAGE_TEXT_MIXED);
                }
                break;
            case UPGRADED:
                modelId = getModelId(PIPELINE_NAME);
                int totalDocsCountUpgraded = 3 * NUM_DOCS_PER_ROUND;
                loadModel(modelId);
                addDocument(getIndexNameForTest(), "2", TEST_FIELD, TEXT_UPGRADED, TEST_IMAGE_FIELD, TEST_IMAGE_TEXT_UPGRADED);
                validateTestIndexOnUpgrade(totalDocsCountUpgraded, modelId, TEXT_UPGRADED, TEST_IMAGE_TEXT_UPGRADED);
                deletePipeline(PIPELINE_NAME);
                deleteModel(modelId);
                deleteIndex(getIndexNameForTest());
                break;
        }
    }

    private void validateTestIndexOnUpgrade(int numberOfDocs, String modelId, String text, String imageText) throws Exception {
        int docCount = getDocCount(getIndexNameForTest());
        assertEquals(numberOfDocs, docCount);
        loadModel(modelId);
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder("passage_embedding", text, imageText, modelId, 1, null, null);
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
        String requestBody = Files.readString(
            Path.of(classLoader.getResource("processor/PipelineForTextImagingProcessorConfiguration.json").toURI())
        );
        createPipelineProcessor(requestBody, pipelineName, modelId);
    }

    private String getModelId(String pipelineName) {
        Map<String, Object> pipeline = getIngestionPipeline(pipelineName);
        assertNotNull(pipeline);
        return TestUtils.getModelId(pipeline, TEXT_IMAGE_EMBEDDING_PROCESSOR);
    }
}
