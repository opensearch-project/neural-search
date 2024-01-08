/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import org.opensearch.index.query.MatchQueryBuilder;
import static org.opensearch.neuralsearch.TestUtils.getModelId;
import static org.opensearch.neuralsearch.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.TestUtils.PARAM_NAME_WEIGHTS;
import static org.opensearch.neuralsearch.TestUtils.TEXT_EMBEDDING_PROCESSOR;
import static org.opensearch.neuralsearch.TestUtils.DEFAULT_NORMALIZATION_METHOD;
import static org.opensearch.neuralsearch.TestUtils.DEFAULT_COMBINATION_METHOD;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

public class HybridSearchIT extends AbstractRestartUpgradeRestTestCase {
    private static final String PIPELINE_NAME = "nlp-hybrid-pipeline";
    private static final String SEARCH_PIPELINE_NAME = "nlp-search-pipeline";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEXT = "Hello world";
    private static final String TEXT_1 = "Hi planet";
    private static final String query = "Hi world";

    // Test restart-upgrade Hybrid Search
    // Create Text Embedding Processor, Ingestion Pipeline, add document and search pipeline with noramlization processor
    // Validate process , pipeline and document count in restart-upgrade scenario
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
            createSearchPipeline(
                SEARCH_PIPELINE_NAME,
                DEFAULT_NORMALIZATION_METHOD,
                DEFAULT_COMBINATION_METHOD,
                Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.3f, 0.7f }))
            );
        } else {
            Map<String, Object> pipeline = getIngestionPipeline(PIPELINE_NAME);
            assertNotNull(pipeline);
            String modelId = getModelId(pipeline, TEXT_EMBEDDING_PROCESSOR);
            loadModel(modelId);
            addDocument(getIndexNameForTest(), "1", TEST_FIELD, TEXT_1, null, null);
            validateTestIndex(modelId);
            deleteSearchPipeline(SEARCH_PIPELINE_NAME);
            deletePipeline(PIPELINE_NAME);
            deleteModel(modelId);
            deleteIndex(getIndexNameForTest());
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
        HybridQueryBuilder hybridQueryBuilder = getQueryBuilder(modelId);
        Map<String, Object> searchResponseAsMap = search(
            getIndexNameForTest(),
            hybridQueryBuilder,
            null,
            1,
            Map.of("search_pipeline", SEARCH_PIPELINE_NAME)
        );
        assertNotNull(searchResponseAsMap);
        int hits = getHitCount(searchResponseAsMap);
        assertEquals(1, hits);
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

}
