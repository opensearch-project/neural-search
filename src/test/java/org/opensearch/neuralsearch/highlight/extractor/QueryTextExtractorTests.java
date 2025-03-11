/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.extractor;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.opensearch.neuralsearch.query.NeuralKNNQuery;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for the query text extractor framework
 */
public class QueryTextExtractorTests extends OpenSearchTestCase {

    private QueryTextExtractorRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = new QueryTextExtractorRegistry();
    }

    public void testTermQueryExtraction() {
        // Create a term query
        TermQuery query = new TermQuery(new Term("content", "artificial"));

        // Extract the query text
        String queryText = registry.extractQueryText(query, "content");

        // Verify the result
        assertEquals("artificial", queryText);
    }

    public void testBooleanQueryExtraction() {
        // Create a boolean query
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("content", "artificial")), BooleanClause.Occur.MUST);
        builder.add(new TermQuery(new Term("content", "intelligence")), BooleanClause.Occur.MUST);
        BooleanQuery query = builder.build();

        // Extract the query text
        String queryText = registry.extractQueryText(query, "content");

        // Verify the result
        assertEquals("artificial intelligence", queryText);
    }

    public void testNeuralQueryExtraction() {
        // Mock a neural query
        NeuralKNNQuery query = mock(NeuralKNNQuery.class);
        when(query.getOriginalQueryText()).thenReturn("How do neural networks work?");

        // Extract the query text
        String queryText = registry.extractQueryText(query, "content");

        // Verify the result
        assertEquals("How do neural networks work?", queryText);
    }

    public void testGenericFallback() {
        // Create a query that doesn't have a specific extractor
        TermQuery query = new TermQuery(new Term("title", "OpenSearch"));

        // Extract the query text using the same field name as the query
        String queryText = registry.extractQueryText(query, "title");

        // Verify the result - should use the generic extractor
        assertEquals("OpenSearch", queryText);
    }
}
