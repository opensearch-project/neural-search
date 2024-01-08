/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.TestUtils;
import static org.opensearch.neuralsearch.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.TestUtils.PARAM_NAME_WEIGHTS;
import static org.opensearch.neuralsearch.TestUtils.TEXT_EMBEDDING_PROCESSOR;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

public class HybridSearchIT extends AbstractRollingUpgradeTestCase {

    private static final String PIPELINE_NAME = "nlp-pipeline";
    private static final String SEARCH_PIPELINE_NAME = "nlp-search-pipeline";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEXT = "Hello world";
    private static final String TEXT_MIXED = "Hi planet";
    private static final String TEXT_UPGRADED = "Hi earth";
    private static final String query = "Hi world";
    private static final int NUM_DOCS_PER_ROUND = 1;

    public void testHybridSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        switch (getClusterType()) {
            case OLD:
                String modelId = uploadTextEmbeddingModel();
                loadModel(modelId);
                createPipelineProcessor(modelId, PIPELINE_NAME);
                createIndexWithConfiguration(
                    getIndexNameForTest(),
                    Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
                    PIPELINE_NAME
                );
                addDocument(getIndexNameForTest(), "0", TEST_FIELD, TEXT, null, null);
                createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE_NAME, Map.of(PARAM_NAME_WEIGHTS, List.of(0.3, 0.7)));
                break;
            case MIXED:
                modelId = getModelId(PIPELINE_NAME);
                int totalDocsCountMixed;
                if (isFirstMixedRound()) {
                    totalDocsCountMixed = NUM_DOCS_PER_ROUND;
                    validateTestIndexOnUpgrade(totalDocsCountMixed, modelId);
                    addDocument(getIndexNameForTest(), "0", TEST_FIELD, TEXT_MIXED, null, null);
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
                deleteSearchPipeline(SEARCH_PIPELINE_NAME);
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
        HybridQueryBuilder hybridQueryBuilder = getQueryBuilder(modelId);
        Map<String, Object> searchResponseAsMap = search(
            getIndexNameForTest(),
            hybridQueryBuilder,
            null,
            1,
            Map.of("search_pipeline", SEARCH_PIPELINE_NAME)
        );
        assertNotNull(searchResponseAsMap);
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
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/PipelineConfiguration.json").toURI()));
        createPipelineProcessor(requestBody, pipelineName, modelId);
    }

    public HybridQueryBuilder getQueryBuilder(String modelId) {
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder();
        neuralQueryBuilder.fieldName("passage_embedding");
        neuralQueryBuilder.modelId(modelId);
        neuralQueryBuilder.queryText(query);
        neuralQueryBuilder.k(5);

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("text", query);

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(matchQueryBuilder);
        hybridQueryBuilder.add(neuralQueryBuilder);

        return hybridQueryBuilder;
    }

    private String getModelId(String pipelineName) {
        Map<String, Object> pipeline = getIngestionPipeline(pipelineName);
        assertNotNull(pipeline);
        return TestUtils.getModelId(pipeline, TEXT_EMBEDDING_PROCESSOR);
    }
}
