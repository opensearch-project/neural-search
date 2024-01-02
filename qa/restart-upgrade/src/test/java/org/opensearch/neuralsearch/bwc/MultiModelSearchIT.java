/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.bwc;


import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.opensearch.neuralsearch.TestUtils.NODES_BWC_CLUSTER;

public class MultiModelSearchIT extends AbstractRestartUpgradeRestTestCase{
    private static final String PIPELINE_NAME = "nlp-ingest-pipeline";

    public void testMultiModalSearch_E2EFlow() throws Exception{
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);

        if (isRunningAgainstOldCluster()){
           String modelId= getModelId();
           loadModel(modelId);
           createPipelineProcessor(modelId, PIPELINE_NAME,ProcessorType.TEXT_IMAGE_EMBEDDING );

        }
    }

    private String getModelId() throws Exception {
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

    protected void createPipelineProcessor(String modelId, String pipelineName, ProcessorType processorType) throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/PipelineForTextImagingProcessorConfiguration.json").toURI()));
        createPipelineProcessor(requestBody, pipelineName, modelId);
    }

}
