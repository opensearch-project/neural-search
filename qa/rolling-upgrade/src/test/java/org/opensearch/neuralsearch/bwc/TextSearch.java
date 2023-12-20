/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.bwc;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.opensearch.neuralsearch.TestUtils.NODES_BWC_CLUSTER;

public class TextSearch extends AbstractRollingUpgradeTestCase{
    private static final String PIPELINE_NAME = "nlp-pipeline";
    private static final String TEST_FIELD = "test-field";
    private static final String TEXT= "Hello world";
    private static final String TEXT_MIXED= "Hello world mixed";
    private static final String TEXT_UPGRADED= "Hello world upgraded";
    private static final int NUM_DOCS = 1;

    public void testIndex() throws Exception{
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        switch (getClusterType()){
            case OLD:
                String modelId= uploadTextEmbeddingModel();
                loadModel(modelId);
                createPipelineProcessor(modelId,PIPELINE_NAME);
                createIndexWithConfiguration(
                        testIndex,
                        Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
                        PIPELINE_NAME
                );
                addDocument(testIndex, "0",TEST_FIELD,TEXT);
                break;
            case MIXED:
                int totalDocsCountMixed;
                if (isFirstMixedRound()){
                    totalDocsCountMixed=NUM_DOCS;
                    validateTestIndexOnUpgrade(totalDocsCountMixed);
                    addDocument(testIndex, "1",TEST_FIELD,TEXT_MIXED);
                }else{
                    totalDocsCountMixed=2*NUM_DOCS;
                    validateTestIndexOnUpgrade(totalDocsCountMixed);
                }
                break;
            case UPGRADED:
                int totalDocsCountUpgraded=3*NUM_DOCS;
                addDocument(testIndex, "2",TEST_FIELD,TEXT_UPGRADED);
                validateTestIndexOnUpgrade(totalDocsCountUpgraded);
                deleteIndex(testIndex);
                break;
        }

    }

    private void validateTestIndexOnUpgrade(int numberOfDocs) throws Exception {
        int docCount=getDocCount(testIndex);
        assertEquals(numberOfDocs,docCount);
    }

    private String uploadTextEmbeddingModel() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        return registerModelGroupAndGetModelId(requestBody);
    }

    private String registerModelGroupAndGetModelId(String requestBody) throws Exception {
        String modelGroupRegisterRequestBody = Files.readString(
                Path.of(classLoader.getResource("processor/CreateModelGroupRequestBody.json").toURI())
        ).replace("<MODEL_GROUP_NAME>", "public_model_" + RandomizedTest.randomAsciiAlphanumOfLength(8));

        String modelGroupId=registerModelGroup(modelGroupRegisterRequestBody);

        requestBody = requestBody.replace("<MODEL_GROUP_ID>", modelGroupId);

        return uploadModelId(requestBody);
    }

    protected void createPipelineProcessor(String modelId, String pipelineName, ProcessorType processorType) throws Exception {
        String requestBody=Files.readString(Path.of(classLoader.getResource("processor/PipelineConfiguration.json").toURI()));
        createPipelineProcessor(requestBody,pipelineName,modelId);
    }
}
