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
import static org.opensearch.neuralsearch.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.TestUtils.PARAM_NAME_WEIGHTS;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

public class HybridSearchIT extends AbstractRestartUpgradeRestTestCase {
    private static final String PIPELINE_NAME = "nlp-pipeline";
    private static final String SEARCH_PIPELINE_NAME = "nlp-search-pipeline";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEXT = "Hello world";
    private static final String TEXT_1 = "Hello world a";
    private static final String query = "Hi world";

    public void testHybridSearch_E2EFlow() throws Exception {
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
            addDocument(getIndexNameForTest(), "0", TEST_FIELD, TEXT, null, null);
            createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE_NAME, Map.of(PARAM_NAME_WEIGHTS, List.of(0.3, 0.7)));
        } else {

        }
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

    private void validateTestIndex(String modelId) throws Exception {
        int docCount = getDocCount(getIndexNameForTest());
        assertEquals(2, docCount);
        loadModel(modelId);
        HybridQueryBuilder hybridQueryBuilder = getQuery(modelId);
        Map<String, Object> searchResponseAsMap = search(
            getIndexNameForTest(),
            hybridQueryBuilder,
            null,
            1,
            Map.of("search_pipeline", SEARCH_PIPELINE_NAME)
        );
        assertNotNull(searchResponseAsMap);
    }

    public HybridQueryBuilder getQuery(String modelId) {
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

}
