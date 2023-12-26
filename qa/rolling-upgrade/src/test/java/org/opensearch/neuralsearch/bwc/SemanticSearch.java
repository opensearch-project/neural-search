/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.bwc;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.TestUtils;
import static org.opensearch.neuralsearch.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.TestUtils.TEXT_EMBEDDING_PROCESSOR;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

@Log4j2
public class SemanticSearch extends AbstractRollingUpgradeTestCase{
    private static final String PIPELINE_NAME = "nlp-pipeline";
    private static final String TEST_FIELD = "test-field";
    private static final String TEXT= "Hello world";
    private static final String TEXT_MIXED= "Hello world mixed";
    private static final String TEXT_UPGRADED= "Hello world upgraded";
    private static final int NUM_DOCS = 1;

    public void testSemanticSearch() throws Exception{
        log.info("Get Cluster Type=========================="+getClusterType());
        getIndices();
        getShards();
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
                modelId=getModelId(PIPELINE_NAME);
                int totalDocsCountMixed;
                if (isFirstMixedRound()){
                    totalDocsCountMixed=NUM_DOCS;
                    validateTestIndexOnUpgrade(totalDocsCountMixed, modelId, TEXT);
                    addDocument(testIndex, "1",TEST_FIELD,TEXT_MIXED);
                    
                }else{
                    totalDocsCountMixed=2*NUM_DOCS;
                    validateTestIndexOnUpgrade(totalDocsCountMixed, modelId, TEXT_MIXED);
                }
                break;
            case UPGRADED:
                modelId=getModelId(PIPELINE_NAME);
                int totalDocsCountUpgraded=3*NUM_DOCS;
                addDocument(testIndex, "2",TEST_FIELD,TEXT_UPGRADED);
                validateTestIndexOnUpgrade(totalDocsCountUpgraded, modelId, TEXT_UPGRADED);
                deleteIndex(testIndex);
                break;
        }

    }

    private void validateTestIndexOnUpgrade(int numberOfDocs, String modelId, String text) throws Exception {
        int docCount=getDocCount(testIndex);
        assertEquals(numberOfDocs,docCount);
        loadModel(modelId);
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder();
        neuralQueryBuilder.fieldName("passage_embedding");
        neuralQueryBuilder.modelId(modelId);
        neuralQueryBuilder.queryText(text);
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

        String modelGroupId=registerModelGroup(modelGroupRegisterRequestBody);

        requestBody = requestBody.replace("<MODEL_GROUP_ID>", modelGroupId);

        return uploadModelId(requestBody);
    }

    protected void createPipelineProcessor(String modelId, String pipelineName, ProcessorType processorType) throws Exception {
        String requestBody=Files.readString(Path.of(classLoader.getResource("processor/PipelineConfiguration.json").toURI()));
        createPipelineProcessor(requestBody,pipelineName,modelId);
    }

    private String getModelId(String pipelineName){
        Map<String,Object> pipeline = getIngestionPipeline(pipelineName);
        assertNotNull(pipeline);
        return TestUtils.getModelId(pipeline,TEXT_EMBEDDING_PROCESSOR);
    }
}
