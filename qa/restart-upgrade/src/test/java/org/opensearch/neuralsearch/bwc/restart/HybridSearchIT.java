/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.restart;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.opensearch.index.query.MatchQueryBuilder;

import static org.opensearch.neuralsearch.util.TestUtils.getModelId;
import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.PARAM_NAME_WEIGHTS;
import static org.opensearch.neuralsearch.util.TestUtils.TEXT_EMBEDDING_PROCESSOR;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_NORMALIZATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_COMBINATION_METHOD;

import org.opensearch.knn.index.query.rescore.RescoreContext;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

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
        validateNormalizationProcessor("processor/IndexMappingMultipleShard.json", PIPELINE_NAME, SEARCH_PIPELINE_NAME);
    }

    // Test restart-upgrade normalization processor when index with single shard
    // Create Text Embedding Processor, Ingestion Pipeline, add document and search pipeline with normalization processor
    // Validate process , pipeline and document count in restart-upgrade scenario
    public void testNormalizationProcessor_whenIndexWithSingleShard_E2EFlow() throws Exception {
        validateNormalizationProcessor("processor/IndexMappingSingleShard.json", PIPELINE1_NAME, SEARCH_PIPELINE1_NAME);
    }

    private void validateNormalizationProcessor(final String fileName, final String pipelineName, final String searchPipelineName)
        throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        if (isRunningAgainstOldCluster()) {
            String modelId = uploadTextEmbeddingModel();
            createPipelineProcessor(modelId, pipelineName);
            createIndexWithConfiguration(
                getIndexNameForTest(),
                Files.readString(Path.of(classLoader.getResource(fileName).toURI())),
                pipelineName
            );
            addDocuments(getIndexNameForTest(), true);
            createSearchPipeline(searchPipelineName);
        } else {
            String modelId = null;
            try {
                modelId = getModelId(getIngestionPipeline(pipelineName), TEXT_EMBEDDING_PROCESSOR);
                loadAndWaitForModelToBeReady(modelId);
                addDocuments(getIndexNameForTest(), false);
                HybridQueryBuilder hybridQueryBuilder = getQueryBuilder(modelId, null, null, null, null);
                validateTestIndex(getIndexNameForTest(), searchPipelineName, hybridQueryBuilder);
                hybridQueryBuilder = getQueryBuilder(
                    modelId,
                    Boolean.FALSE,
                    Map.of("ef_search", 100),
                    RescoreContext.getDefault(),
                    new MatchQueryBuilder("_id", "5")
                );
                validateTestIndex(getIndexNameForTest(), searchPipelineName, hybridQueryBuilder);
            } finally {
                wipeOfTestResources(getIndexNameForTest(), pipelineName, modelId, searchPipelineName);
            }
        }
    }

    private void addDocuments(final String indexName, boolean isRunningAgainstOldCluster) throws IOException {
        if (isRunningAgainstOldCluster) {
            addDocument(indexName, "0", TEST_FIELD, TEXT_1, null, null);
            addDocument(indexName, "1", TEST_FIELD, TEXT_2, null, null);
            addDocument(indexName, "2", TEST_FIELD, TEXT_3, null, null);
            addDocument(indexName, "3", TEST_FIELD, TEXT_4, null, null);
            addDocument(indexName, "4", TEST_FIELD, TEXT_5, null, null);
        } else {
            addDocument(indexName, "5", TEST_FIELD, TEXT_6, null, null);
        }
    }

    private void createSearchPipeline(final String pipelineName) {
        createSearchPipeline(
            pipelineName,
            DEFAULT_NORMALIZATION_METHOD,
            DEFAULT_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.3f, 0.7f })),
            false
        );
    }

    private void validateTestIndex(final String index, final String searchPipeline, HybridQueryBuilder queryBuilder) {
        int docCount = getDocCount(index);
        assertEquals(6, docCount);
        Map<String, Object> searchResponseAsMap = search(index, queryBuilder, null, 1, Map.of("search_pipeline", searchPipeline));
        assertNotNull(searchResponseAsMap);
        int hits = getHitCount(searchResponseAsMap);
        assertEquals(1, hits);
        List<Double> scoresList = getNormalizationScoreList(searchResponseAsMap);
        for (Double score : scoresList) {
            assertTrue(0 <= score && score <= 2);
        }
    }

    private HybridQueryBuilder getQueryBuilder(
        final String modelId,
        final Boolean expandNestedDocs,
        final Map<String, ?> methodParameters,
        final RescoreContext rescoreContext,
        final QueryBuilder filter
    ) {
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName("passage_embedding")
            .modelId(modelId)
            .queryText(QUERY)
            .k(5)
            .build();
        if (expandNestedDocs != null) {
            neuralQueryBuilder.expandNested(expandNestedDocs);
        }
        if (methodParameters != null) {
            neuralQueryBuilder.methodParameters(methodParameters);
        }
        if (rescoreContext != null) {
            neuralQueryBuilder.rescoreContext(rescoreContext);
        }

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("text", QUERY);

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(matchQueryBuilder);
        hybridQueryBuilder.add(neuralQueryBuilder);

        if (filter != null) {
            hybridQueryBuilder.filter(filter);
        }

        return hybridQueryBuilder;
    }

}
