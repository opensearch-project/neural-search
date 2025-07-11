/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.opensearch.index.query.MatchQueryBuilder;
import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.PARAM_NAME_WEIGHTS;
import static org.opensearch.neuralsearch.util.TestUtils.TEXT_EMBEDDING_PROCESSOR;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_NORMALIZATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_COMBINATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.getModelId;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.knn.index.query.rescore.RescoreContext;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

public class HybridSearchIT extends AbstractRollingUpgradeTestCase {

    private static final String PIPELINE_NAME = "nlp-hybrid-pipeline";
    private static final String SEARCH_PIPELINE_NAME = "nlp-search-pipeline";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEXT = "Hello world";
    private static final String TEXT_MIXED = "Hi planet";
    private static final String TEXT_UPGRADED = "Hi earth";
    private static final String QUERY = "Hi world";
    private static final int NUM_DOCS_PER_ROUND = 1;
    private static final String VECTOR_EMBEDDING_FIELD = "passage_embedding";
    protected static final String RESCORE_QUERY = "hi";
    private static String modelId = "";

    // Test rolling-upgrade normalization processor when index with multiple shards
    // Create Text Embedding Processor, Ingestion Pipeline, add document and search pipeline with noramlization processor
    // Validate process , pipeline and document count in rolling-upgrade scenario
    public void testNormalizationProcessor_whenIndexWithMultipleShards_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        switch (getClusterType()) {
            case OLD:
                modelId = uploadTextEmbeddingModel();
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
                    Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.3f, 0.7f })),
                    false
                );
                break;
            case MIXED:
                modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                loadAndWaitForModelToBeReady(modelId);
                int totalDocsCountMixed;
                if (isFirstMixedRound()) {
                    totalDocsCountMixed = NUM_DOCS_PER_ROUND;
                    HybridQueryBuilder hybridQueryBuilder = getQueryBuilder(modelId, null, null, null, null);
                    validateTestIndexOnUpgrade(totalDocsCountMixed, modelId, hybridQueryBuilder, null);
                    addDocument(getIndexNameForTest(), "1", TEST_FIELD, TEXT_MIXED, null, null);
                } else {
                    totalDocsCountMixed = 2 * NUM_DOCS_PER_ROUND;
                    HybridQueryBuilder hybridQueryBuilder = getQueryBuilder(modelId, null, null, null, null);
                    validateTestIndexOnUpgrade(totalDocsCountMixed, modelId, hybridQueryBuilder, null);
                }
                break;
            case UPGRADED:
                try {
                    modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                    loadAndWaitForModelToBeReady(modelId);
                    int totalDocsCountUpgraded = 3 * NUM_DOCS_PER_ROUND;
                    addDocument(getIndexNameForTest(), "2", TEST_FIELD, TEXT_UPGRADED, null, null);
                    HybridQueryBuilder hybridQueryBuilder = getQueryBuilder(modelId, null, null, null, null);
                    validateTestIndexOnUpgrade(totalDocsCountUpgraded, modelId, hybridQueryBuilder, null);
                    hybridQueryBuilder = getQueryBuilder(
                        modelId,
                        Boolean.FALSE,
                        Map.of("ef_search", 100),
                        RescoreContext.getDefault(),
                        new MatchQueryBuilder("_id", "2")
                    );
                    validateTestIndexOnUpgrade(totalDocsCountUpgraded, modelId, hybridQueryBuilder, null);
                } finally {
                    wipeOfTestResources(getIndexNameForTest(), PIPELINE_NAME, modelId, SEARCH_PIPELINE_NAME);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }
    }

    private void validateTestIndexOnUpgrade(
        final int numberOfDocs,
        final String modelId,
        HybridQueryBuilder hybridQueryBuilder,
        QueryBuilder rescorer
    ) throws Exception {
        int docCount = getDocCount(getIndexNameForTest());
        assertEquals(numberOfDocs, docCount);
        Map<String, Object> searchResponseAsMap = search(
            getIndexNameForTest(),
            hybridQueryBuilder,
            rescorer,
            1,
            Map.of("search_pipeline", SEARCH_PIPELINE_NAME)
        );
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
        final RescoreContext rescoreContextForNeuralQuery,
        final QueryBuilder filter
    ) {
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(VECTOR_EMBEDDING_FIELD)
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
        if (Objects.nonNull(rescoreContextForNeuralQuery)) {
            neuralQueryBuilder.rescoreContext(rescoreContextForNeuralQuery);
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
