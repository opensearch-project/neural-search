/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.restart;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_COMBINATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_NORMALIZATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.PARAM_NAME_WEIGHTS;
import static org.opensearch.neuralsearch.util.TestUtils.TEXT_EMBEDDING_PROCESSOR;
import static org.opensearch.neuralsearch.util.TestUtils.getModelId;

import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.knn.index.query.rescore.RescoreContext;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

public class HybridSearchWithRescoreIT extends AbstractRestartUpgradeRestTestCase {
    private static final String PIPELINE_NAME = "nlp-hybrid-with-rescore-pipeline";
    private static final String SEARCH_PIPELINE_NAME = "nlp-search-with_rescore-pipeline";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEXT = "Hello world";
    private static final String TEXT_UPGRADED = "Hi earth";
    private static final String QUERY = "Hi world";
    private static final int NUM_DOCS_PER_ROUND = 1;
    private static final String VECTOR_EMBEDDING_FIELD = "passage_embedding";
    protected static final String RESCORE_QUERY = "hi";

    /**
     * Test normalization with hybrid query and rescore. This test is required as rescore will not be compatible with version lower than 2.15
     */
    public void testHybridQueryWithRescore_whenIndexWithMultipleShards_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);

        if (isRunningAgainstOldCluster()) {
            String modelId = uploadTextEmbeddingModel();
            createPipelineProcessor(modelId, PIPELINE_NAME);
            createIndexWithConfiguration(
                getIndexNameForTest(),
                Files.readString(Path.of(classLoader.getResource("processor/IndexMappingMultipleShard.json").toURI())),
                PIPELINE_NAME
            );
            addDocument(getIndexNameForTest(), "0", TEST_FIELD, TEXT, null, null);
            createSearchPipeline(
                SEARCH_PIPELINE_NAME,
                DEFAULT_NORMALIZATION_METHOD,
                DEFAULT_COMBINATION_METHOD,
                Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.3f, 0.7f })),
                false
            );
        } else {
            String modelId = null;
            try {
                modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                loadAndWaitForModelToBeReady(modelId);
                addDocument(getIndexNameForTest(), "1", TEST_FIELD, TEXT_UPGRADED, null, null);
                HybridQueryBuilder hybridQueryBuilder = getQueryBuilder(modelId, null, null);
                QueryBuilder rescorer = QueryBuilders.matchQuery(TEST_FIELD, RESCORE_QUERY).boost(0.3f);
                validateTestIndex(getIndexNameForTest(), hybridQueryBuilder, rescorer);
                hybridQueryBuilder = getQueryBuilder(modelId, Map.of("ef_search", 100), RescoreContext.getDefault());
                validateTestIndex(getIndexNameForTest(), hybridQueryBuilder, rescorer);
            } finally {
                wipeOfTestResources(getIndexNameForTest(), PIPELINE_NAME, modelId, null);
            }
        }
    }

    private void validateTestIndex(final String index, HybridQueryBuilder queryBuilder, QueryBuilder rescorer) {
        int docCount = getDocCount(index);
        assertEquals(2, docCount);
        Map<String, Object> searchResponseAsMap = search(index, queryBuilder, rescorer, 1, Map.of("search_pipeline", SEARCH_PIPELINE_NAME));
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
        final Map<String, ?> methodParameters,
        final RescoreContext rescoreContextForNeuralQuery
    ) {
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(VECTOR_EMBEDDING_FIELD)
            .modelId(modelId)
            .queryText(QUERY)
            .k(5)
            .build();
        if (methodParameters != null) {
            neuralQueryBuilder.methodParameters(methodParameters);
        }
        if (Objects.nonNull(rescoreContextForNeuralQuery)) {
            neuralQueryBuilder.rescoreContext(rescoreContextForNeuralQuery);
        }

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("text", QUERY);

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(matchQueryBuilder);
        hybridQueryBuilder.add(neuralQueryBuilder);

        return hybridQueryBuilder;
    }
}
