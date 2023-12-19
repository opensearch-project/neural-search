/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.bwc;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.opensearch.neuralsearch.TestUtils.NODES_BWC_CLUSTER;

public class TextSearch extends AbstractRestartUpgradeRestTestCase{

    private static final String PIPELINE_NAME = "nlp-pipeline";
    private static String DOC_ID = "0";
    private static final String TEST_FIELD = "test-field";
    private static final String TEXT= "Hello world";

    public void testIndex() throws Exception{
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);

        if (isRunningAgainstOldCluster()){
            String modelId= uploadTextEmbeddingModel();
            loadModel(modelId);
            createPipelineProcessor(modelId,PIPELINE_NAME);
            createIndexWithConfiguration(
                    testIndex,
                    Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
                    PIPELINE_NAME
            );
            logger.info("=================================================================================Document Added");
            addDocument(testIndex, DOC_ID,TEST_FIELD,TEXT);
        }else {
            System.out.println("===========================================================================================Cluster Upgraded");
            validateTestIndex();
        }
    }


    private void validateTestIndex() throws Exception {
        int docCount=getDocCount(testIndex);
        assertEquals(1,docCount);
        deleteIndex(testIndex);
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
