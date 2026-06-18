/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.knn.index.query.KNNQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.sparse.query.SparseAnnQueryBuilder;

import static org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils.setUpClusterService;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_DIMENSION;
import static org.opensearch.neuralsearch.util.TestUtils.createRandomVector;

public class HybridQueryFilterUtilTests extends OpenSearchQueryTestCase {

    private static final TermQueryBuilder FILTER = new TermQueryBuilder("_id", "target-doc-id");

    public void testApplyFilterToSubQuery_whenNestedNeuralQuery_thenFilterOnInnerNeuralQuery() {
        setUpClusterService();
        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName("embeddings.embedding")
            .queryText("search terms")
            .modelId("model-id")
            .k(100)
            .build();
        NestedQueryBuilder nestedQuery = new NestedQueryBuilder("embeddings", neuralQuery, ScoreMode.Max);

        QueryBuilder result = HybridQueryFilterUtil.applyFilterToSubQuery(nestedQuery, FILTER);

        assertTrue(result instanceof NestedQueryBuilder);
        QueryBuilder innerQuery = ((NestedQueryBuilder) result).query();
        assertTrue(innerQuery instanceof NeuralQueryBuilder);
        assertEquals(FILTER, ((NeuralQueryBuilder) innerQuery).queryfilter());
    }

    public void testApplyFilterToSubQuery_whenNestedKnnQuery_thenFilterOnInnerKnnQuery() {
        KNNQueryBuilder knnQuery = KNNQueryBuilder.builder()
            .fieldName("embeddings.embedding")
            .vector(createRandomVector(TEST_DIMENSION))
            .k(100)
            .build();
        NestedQueryBuilder nestedQuery = new NestedQueryBuilder("embeddings", knnQuery, ScoreMode.Max);

        QueryBuilder result = HybridQueryFilterUtil.applyFilterToSubQuery(nestedQuery, FILTER);

        assertTrue(result instanceof NestedQueryBuilder);
        QueryBuilder innerQuery = ((NestedQueryBuilder) result).query();
        assertTrue(innerQuery instanceof KNNQueryBuilder);
        assertEquals(FILTER, ((KNNQueryBuilder) innerQuery).getFilter());
    }

    public void testApplyFilterToSubQuery_whenNestedMatchQuery_thenFilterWrappedInBoolQuery() {
        NestedQueryBuilder nestedQuery = new NestedQueryBuilder(
            "embeddings",
            new MatchQueryBuilder("embeddings.text", "search terms"),
            ScoreMode.Max
        );

        QueryBuilder result = HybridQueryFilterUtil.applyFilterToSubQuery(nestedQuery, FILTER);

        assertTrue(result instanceof BoolQueryBuilder);
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) result;
        assertTrue(boolQuery.must().get(0) instanceof NestedQueryBuilder);
        assertEquals(FILTER, boolQuery.filter().get(0));
    }

    public void testApplyFilterToSubQuery_whenNestedBoolQuery_thenFilterWrappedInBoolQuery() {
        BoolQueryBuilder innerBool = QueryBuilders.boolQuery()
            .must(new MatchQueryBuilder("embeddings.text", "search terms"))
            .filter(new TermQueryBuilder("embeddings.type", "chunk"));
        NestedQueryBuilder nestedQuery = new NestedQueryBuilder("embeddings", innerBool, ScoreMode.Max);

        QueryBuilder result = HybridQueryFilterUtil.applyFilterToSubQuery(nestedQuery, FILTER);

        assertTrue(result instanceof BoolQueryBuilder);
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) result;
        assertTrue(boolQuery.must().get(0) instanceof NestedQueryBuilder);
        assertEquals(FILTER, boolQuery.filter().get(0));
    }

    public void testApplyFilterToSubQuery_whenNestedNeuralSparseQuery_thenFilterWrappedInBoolQuery() {
        SparseAnnQueryBuilder sparseAnnQuery = new SparseAnnQueryBuilder().fieldName("embeddings.sparse")
            .k(10)
            .queryTokens(java.util.Map.of("1000", 1.0f));
        NeuralSparseQueryBuilder neuralSparseQuery = new NeuralSparseQueryBuilder().fieldName("embeddings.sparse")
            .sparseAnnQueryBuilder(sparseAnnQuery);
        NestedQueryBuilder nestedQuery = new NestedQueryBuilder("embeddings", neuralSparseQuery, ScoreMode.Max);

        QueryBuilder result = HybridQueryFilterUtil.applyFilterToSubQuery(nestedQuery, FILTER);

        assertTrue(result instanceof BoolQueryBuilder);
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) result;
        assertTrue(boolQuery.must().get(0) instanceof NestedQueryBuilder);
        assertEquals(FILTER, boolQuery.filter().get(0));
    }

    public void testApplyFilterToSubQuery_whenNestedHybridQuery_thenFilterWrappedInBoolQuery() {
        HybridQueryBuilder hybridQuery = new HybridQueryBuilder().add(new MatchQueryBuilder("embeddings.text", "search terms"));
        NestedQueryBuilder nestedQuery = new NestedQueryBuilder("embeddings", hybridQuery, ScoreMode.Max);

        QueryBuilder result = HybridQueryFilterUtil.applyFilterToSubQuery(nestedQuery, FILTER);

        assertTrue(result instanceof BoolQueryBuilder);
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) result;
        assertTrue(boolQuery.must().get(0) instanceof NestedQueryBuilder);
        assertEquals(FILTER, boolQuery.filter().get(0));
    }
}
