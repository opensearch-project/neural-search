/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
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
    private static final String PIPELINE1_NAME = "nlp-hybrid-1-pipeline";
    private static final String SEARCH_PIPELINE_NAME = "nlp-search-pipeline";
    private static final String SEARCH_PIPELINE1_NAME = "nlp-search-1-pipeline";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEXT_1 = "Hello world";
    private static final String TEXT_2 = "Hi planet";
    private static final String TEXT_3 = "Hi earth";
    private static final String TEXT_4 = "Hi amazon";
    private static final String TEXT_5 = "Hi mars";
    private static final String TEXT_6 = "Hi opensearch";
    private static final String QUERY = "Hi world";

    // Test restart-upgrade normalization processor when index with multiple shards
    // Create Text Embedding Processor, Ingestion Pipeline, add document and search pipeline with normalization processor
    // Validate process , pipeline and document count in restart-upgrade scenario
    public void testNormalizationProcessor_whenIndexWithMultipleShards_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        if (isRunningAgainstOldCluster()) {
            String index = getIndexNameForTest();
            String modelId = uploadTextEmbeddingModel();
            loadModel(modelId);
            createPipelineProcessor(modelId, PIPELINE_NAME);
            createIndexWithConfiguration(
                index,
                Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
                PIPELINE_NAME
            );
            addDocument(index, "0", TEST_FIELD, TEXT_1, null, null);
            addDocument(index, "1", TEST_FIELD, TEXT_2, null, null);
            addDocument(index, "2", TEST_FIELD, TEXT_3, null, null);
            addDocument(index, "3", TEST_FIELD, TEXT_4, null, null);
            addDocument(index, "4", TEST_FIELD, TEXT_5, null, null);
            createSearchPipeline(
                SEARCH_PIPELINE_NAME,
                DEFAULT_NORMALIZATION_METHOD,
                DEFAULT_COMBINATION_METHOD,
                Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.3f, 0.7f }))
            );
        } else {
            String index = getIndexNameForTest();
            Map<String, Object> pipeline = getIngestionPipeline(PIPELINE_NAME);
            assertNotNull(pipeline);
            String modelId = getModelId(pipeline, TEXT_EMBEDDING_PROCESSOR);
            loadModel(modelId);
            addDocument(index, "5", TEST_FIELD, TEXT_6, null, null);
            validateTestIndex(modelId, index, SEARCH_PIPELINE_NAME);
            deleteSearchPipeline(SEARCH_PIPELINE_NAME);
            deletePipeline(PIPELINE_NAME);
            deleteModel(modelId);
            deleteIndex(index);
        }
    }

    // Test restart-upgrade normalization processor when index with single shard
    // Create Text Embedding Processor, Ingestion Pipeline, add document and search pipeline with normalization processor
    // Validate process , pipeline and document count in restart-upgrade scenario
    public void testNormalizationProcessor_whenIndexWithSingleShard_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        if (isRunningAgainstOldCluster()) {
            String index = getIndexNameForTest() + "1";
            String modelId = uploadTextEmbeddingModel();
            loadModel(modelId);
            createPipelineProcessor(modelId, PIPELINE1_NAME);
            createIndexWithConfiguration(
                index,
                Files.readString(Path.of(classLoader.getResource("processor/Index1Mappings.json").toURI())),
                PIPELINE1_NAME
            );
            addDocument(index, "0", TEST_FIELD, TEXT_1, null, null);
            addDocument(index, "1", TEST_FIELD, TEXT_2, null, null);
            addDocument(index, "2", TEST_FIELD, TEXT_3, null, null);
            addDocument(index, "3", TEST_FIELD, TEXT_4, null, null);
            addDocument(index, "4", TEST_FIELD, TEXT_5, null, null);
            createSearchPipeline(
                SEARCH_PIPELINE1_NAME,
                DEFAULT_NORMALIZATION_METHOD,
                DEFAULT_COMBINATION_METHOD,
                Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.3f, 0.7f }))
            );
        } else {
            String index = getIndexNameForTest() + "1";
            Map<String, Object> pipeline = getIngestionPipeline(PIPELINE1_NAME);
            assertNotNull(pipeline);
            String modelId = getModelId(pipeline, TEXT_EMBEDDING_PROCESSOR);
            loadModel(modelId);
            addDocument(index, "5", TEST_FIELD, TEXT_6, null, null);
            validateTestIndex(modelId, index, SEARCH_PIPELINE1_NAME);
            deleteSearchPipeline(SEARCH_PIPELINE1_NAME);
            deletePipeline(PIPELINE1_NAME);
            deleteModel(modelId);
            deleteIndex(index);
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

    private void validateTestIndex(String modelId, String index, String searchPipeline) throws Exception {
        int docCount = getDocCount(index);
        assertEquals(6, docCount);
        HybridQueryBuilder hybridQueryBuilder = getQueryBuilder(modelId);
        Map<String, Object> searchResponseAsMap = search(index, hybridQueryBuilder, null, 1, Map.of("search_pipeline", searchPipeline));
        assertNotNull(searchResponseAsMap);
        int hits = getHitCount(searchResponseAsMap);
        assertEquals(1, hits);
        List<Double> scoresList = getNormalizationScoreList(searchResponseAsMap);
        for (Double score : scoresList) {
            assertTrue(0 < score && score < 1);
        }
    }

    public HybridQueryBuilder getQueryBuilder(String modelId) {
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder();
        neuralQueryBuilder.fieldName("passage_embedding");
        neuralQueryBuilder.modelId(modelId);
        neuralQueryBuilder.queryText(QUERY);
        neuralQueryBuilder.k(5);

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("text", QUERY);

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(matchQueryBuilder);
        hybridQueryBuilder.add(neuralQueryBuilder);

        return hybridQueryBuilder;
    }

}
