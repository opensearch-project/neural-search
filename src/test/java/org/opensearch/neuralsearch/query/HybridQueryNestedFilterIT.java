/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.SneakyThrows;
import org.apache.lucene.search.join.ScoreMode;
import org.junit.BeforeClass;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import java.util.Locale;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;
import static org.opensearch.neuralsearch.util.TestUtils.getMaxScore;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_DIMENSION;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_SPACE_TYPE;
import static org.opensearch.neuralsearch.util.TestUtils.createRandomVector;

/**
 * Integration tests for {@link HybridQueryBuilder#filter(org.opensearch.index.query.QueryBuilder)} when
 * sub-queries are wrapped in nested queries. See https://github.com/opensearch-project/neural-search/issues/1759.
 */
public class HybridQueryNestedFilterIT extends BaseNeuralSearchIT {

    private static final String TEST_INDEX = "test-hybrid-nested-filter-index";
    private static final String SEARCH_PIPELINE = "phase-results-hybrid-nested-filter-pipeline";
    private static final String NESTED_PATH = "embeddings";
    private static final String NESTED_TEXT_FIELD = NESTED_PATH + ".text";
    private static final String NESTED_KNN_FIELD = NESTED_PATH + ".embedding";
    private static final String TARGET_DOC_ID = "target";
    private static final String DECOY_DOC_ID = "decoy";
    private static final String QUERY_TEXT = "search terms";
    private static final String DECOY_TEXT = "unrelated content";
    private static final int K = 1;

    @BeforeClass
    @SneakyThrows
    public static void setUpCluster() {
        HybridQueryNestedFilterIT instance = new HybridQueryNestedFilterIT();
        instance.initClient();
        instance.updateClusterSettings();
    }

    /**
     * When hybrid.filter targets a document whose nested embedding is outside the global top-k, the filter must be
     * pushed into the nested neural query as a kNN pre-filter so both hybrid sub-queries contribute to the score.
     */
    @SneakyThrows
    public void testHybridFilter_whenNestedNeuralQuery_thenKnnPreFilterFindsTargetDocument() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        prepareNestedIndexWithDocuments();
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);

        String modelId = prepareModel();
        float[] queryVector = runInference(modelId, QUERY_TEXT);
        float[] decoyVector = queryVector;
        float[] targetVector = createRandomVector(TEST_DIMENSION);

        indexNestedDocument(TARGET_DOC_ID, QUERY_TEXT, targetVector);
        indexNestedDocument(DECOY_DOC_ID, DECOY_TEXT, decoyVector);

        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(NESTED_TEXT_FIELD, QUERY_TEXT);
        NestedQueryBuilder matchNestedQueryBuilder = QueryBuilders.nestedQuery(NESTED_PATH, matchQueryBuilder, ScoreMode.Max);

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(NESTED_KNN_FIELD)
            .queryText(QUERY_TEXT)
            .modelId(modelId)
            .k(K)
            .build();
        NestedQueryBuilder neuralNestedQueryBuilder = QueryBuilders.nestedQuery(NESTED_PATH, neuralQueryBuilder, ScoreMode.Max);

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(matchNestedQueryBuilder);
        hybridQueryBuilder.add(neuralNestedQueryBuilder);
        hybridQueryBuilder.filter(new TermQueryBuilder("_id", TARGET_DOC_ID));

        Map<String, Object> searchResponse = search(
            TEST_INDEX,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null
        );

        assertEquals(1, getHitCount(searchResponse));
        assertEquals(TARGET_DOC_ID, getFirstInnerHit(searchResponse).get("_id"));
        assertTrue(getMaxScore(searchResponse).isPresent());
        assertEquals(1.0f, getMaxScore(searchResponse).get(), DELTA_FOR_SCORE_ASSERTION);
    }

    @SneakyThrows
    private void prepareNestedIndexWithDocuments() {
        if (indexExists(TEST_INDEX)) {
            return;
        }
        String indexConfiguration = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("settings")
            .field("number_of_shards", 1)
            .field("number_of_replicas", 0)
            .field("index.knn", true)
            .endObject()
            .startObject("mappings")
            .startObject("properties")
            .startObject(NESTED_PATH)
            .field("type", "nested")
            .startObject("properties")
            .startObject("text")
            .field("type", "text")
            .endObject()
            .startObject("embedding")
            .field("type", "knn_vector")
            .field("dimension", TEST_DIMENSION)
            .startObject("method")
            .field("engine", "lucene")
            .field("space_type", TEST_SPACE_TYPE.getValue())
            .field("name", "hnsw")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        createIndexWithConfiguration(TEST_INDEX, indexConfiguration, "");
    }

    @SneakyThrows
    private void indexNestedDocument(final String docId, final String text, final float[] embedding) {
        String source = XContentFactory.jsonBuilder()
            .startObject()
            .startArray(NESTED_PATH)
            .startObject()
            .field("text", text)
            .field("embedding", embedding)
            .endObject()
            .endArray()
            .endObject()
            .toString();
        makeRequest(
            client(),
            "PUT",
            String.format(Locale.ROOT, "/%s/_doc/%s?refresh=true", TEST_INDEX, docId),
            null,
            toHttpEntity(source),
            null
        );
    }
}
