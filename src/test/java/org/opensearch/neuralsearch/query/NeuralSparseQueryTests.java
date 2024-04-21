/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.SneakyThrows;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.junit.Before;

import static org.mockito.Mockito.mock;

public class NeuralSparseQueryTests extends OpenSearchQueryTestCase {

    private Query currentQuery, highScoreTokenQuery, lowScoreTokenQuery;

    @Before
    public void setup() {
        Query query1 = FeatureField.newLinearQuery("testFiled", "apple", 5.0f);
        Query query2 = FeatureField.newLinearQuery("testFiled", "banana", 1.0f);
        Query query3 = FeatureField.newLinearQuery("testFiled", "orange", 1.2f);
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        currentQuery = builder.add(query1, BooleanClause.Occur.SHOULD)
            .add(query2, BooleanClause.Occur.SHOULD)
            .add(query3, BooleanClause.Occur.SHOULD)
            .build();
        builder = new BooleanQuery.Builder();
        highScoreTokenQuery = builder.add(query1, BooleanClause.Occur.SHOULD).build();
        builder = new BooleanQuery.Builder();
        lowScoreTokenQuery = builder.add(query3, BooleanClause.Occur.SHOULD).add(query2, BooleanClause.Occur.SHOULD).build();
    }

    @SneakyThrows
    public void testToStringMethod() {
        NeuralSparseQuery neuralSparseQuery = new NeuralSparseQuery(currentQuery, highScoreTokenQuery, lowScoreTokenQuery, 5.0f);
        String expectedString = "NeuralSparseQuery("
            + currentQuery.toString()
            + ','
            + highScoreTokenQuery.toString()
            + ", "
            + lowScoreTokenQuery.toString()
            + ")";
        assertEquals(expectedString, neuralSparseQuery.toString());
    }

    @SneakyThrows
    public void testRewrite_whenDifferent_thenNotSame() {
        IndexSearcher mockIndexSearcher = mock(IndexSearcher.class);
        NeuralSparseQuery neuralSparseQuery = new NeuralSparseQuery(currentQuery, highScoreTokenQuery, lowScoreTokenQuery, 5.0f);
        Query rewrittenQuery = neuralSparseQuery.rewrite(mockIndexSearcher);
        assertNotSame(neuralSparseQuery, rewrittenQuery);
        assertTrue(rewrittenQuery instanceof NeuralSparseQuery);
    }

    @SneakyThrows
    public void testRewrite_whenDifferent_thenSame() {
        IndexSearcher mockIndexSearcher = mock(IndexSearcher.class);
        NeuralSparseQuery neuralSparseQuery = new NeuralSparseQuery(
            new MatchAllDocsQuery(),
            new MatchAllDocsQuery(),
            new MatchAllDocsQuery(),
            5.0f
        );
        Query rewrittenQuery = neuralSparseQuery.rewrite(mockIndexSearcher);
        assertSame(neuralSparseQuery, rewrittenQuery);
        assertTrue(rewrittenQuery instanceof NeuralSparseQuery);
    }

    @SneakyThrows
    public void testEqualsAndHashCode() {
        NeuralSparseQuery query1 = new NeuralSparseQuery(currentQuery, highScoreTokenQuery, lowScoreTokenQuery, 5.0f);
        NeuralSparseQuery query2 = new NeuralSparseQuery(currentQuery, highScoreTokenQuery, lowScoreTokenQuery, 5.0f);
        NeuralSparseQuery query3 = new NeuralSparseQuery(currentQuery, highScoreTokenQuery, new MatchAllDocsQuery(), 5.0f);
        NeuralSparseQuery query4 = new NeuralSparseQuery(currentQuery, highScoreTokenQuery, lowScoreTokenQuery, 6.0f);
        assertEquals(query1, query2);
        assertNotEquals(query1, query3);
        assertNotEquals(query1, query4);
        assertEquals(query1.hashCode(), query2.hashCode());
        assertNotEquals(query1.hashCode(), query3.hashCode());
        assertNotEquals(query1.hashCode(), query4.hashCode());

    }

    @SneakyThrows
    public void testExtractLowScoreToken_thenCurrentChanged() {
        NeuralSparseQuery neuralSparseQuery = new NeuralSparseQuery(currentQuery, highScoreTokenQuery, lowScoreTokenQuery, 5.0f);
        assertSame(neuralSparseQuery.getCurrentQuery(), currentQuery);
        neuralSparseQuery.extractLowScoreToken();
        assertSame(neuralSparseQuery.getCurrentQuery(), highScoreTokenQuery);
    }

    @SneakyThrows
    public void testVisit_thenSuccess() {
        NeuralSparseQuery neuralSparseQuery = new NeuralSparseQuery(currentQuery, highScoreTokenQuery, lowScoreTokenQuery, 5.0f);
        neuralSparseQuery.visit(QueryVisitor.EMPTY_VISITOR);
    }

}
