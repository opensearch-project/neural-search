/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.opensearch.neuralsearch.highlight.extractor.BooleanQueryTextExtractor;
import org.opensearch.neuralsearch.highlight.extractor.NeuralQueryTextExtractor;
import org.opensearch.neuralsearch.highlight.extractor.QueryTextExtractorRegistry;
import org.opensearch.neuralsearch.highlight.extractor.TermQueryTextExtractor;
import org.opensearch.neuralsearch.query.NeuralKNNQuery;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the query text extractor framework
 */
public class QueryTextExtractorTests extends OpenSearchTestCase {

    private QueryTextExtractorRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = new QueryTextExtractorRegistry();
    }

    /**
     * Tests the TermQueryTextExtractor
     */
    public void testTermQueryExtractor() {
        TermQueryTextExtractor extractor = new TermQueryTextExtractor();

        // Test with matching field
        TermQuery query = new TermQuery(new Term("content", "artificial"));
        String result = extractor.extractQueryText(query, "content");
        assertEquals("Should extract term text for matching field", "artificial", result);

        // Test with non-matching field
        result = extractor.extractQueryText(query, "title");
        assertEquals("Should return empty string for non-matching field", "", result);

        // Test with non-TermQuery
        BooleanQuery booleanQuery = new BooleanQuery.Builder().build();
        result = extractor.extractQueryText(booleanQuery, "content");
        assertEquals("Should return empty string for non-TermQuery", "", result);
    }

    /**
     * Tests the NeuralQueryTextExtractor
     */
    public void testNeuralQueryExtractor() {
        NeuralQueryTextExtractor extractor = new NeuralQueryTextExtractor();

        // Test with NeuralKNNQuery
        NeuralKNNQuery neuralQuery = mock(NeuralKNNQuery.class);
        when(neuralQuery.getOriginalQueryText()).thenReturn("neural search query");

        String result = extractor.extractQueryText(neuralQuery, "content");
        assertEquals("Should extract original query text", "neural search query", result);

        // Test with non-NeuralKNNQuery
        TermQuery termQuery = new TermQuery(new Term("content", "term"));
        result = extractor.extractQueryText(termQuery, "content");
        assertEquals("Should return empty string for non-NeuralKNNQuery", "", result);
    }

    /**
     * Tests the BooleanQueryTextExtractor
     */
    public void testBooleanQueryExtractor() {
        // Create a registry for the BooleanQueryTextExtractor to use
        QueryTextExtractorRegistry localRegistry = new QueryTextExtractorRegistry();
        BooleanQueryTextExtractor extractor = new BooleanQueryTextExtractor(localRegistry);

        // Create a boolean query with multiple clauses
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("content", "artificial")), BooleanClause.Occur.MUST);
        builder.add(new TermQuery(new Term("content", "intelligence")), BooleanClause.Occur.MUST);
        builder.add(new TermQuery(new Term("content", "not-included")), BooleanClause.Occur.MUST_NOT);
        BooleanQuery query = builder.build();

        String result = extractor.extractQueryText(query, "content");
        assertEquals("Should combine terms with space separator", "artificial intelligence", result);

        // Test with non-BooleanQuery
        TermQuery termQuery = new TermQuery(new Term("content", "term"));
        result = extractor.extractQueryText(termQuery, "content");
        assertEquals("Should return empty string for non-BooleanQuery", "", result);

        // Test with empty clauses
        BooleanQuery emptyQuery = new BooleanQuery.Builder().build();
        result = extractor.extractQueryText(emptyQuery, "content");
        assertEquals("Should return empty string for empty boolean query", "", result);
    }

    /**
     * Tests the QueryTextExtractorRegistry
     */
    public void testQueryTextExtractorRegistry() {
        // Test with registered extractors
        TermQuery termQuery = new TermQuery(new Term("content", "term"));
        String result = registry.extractQueryText(termQuery, "content");
        assertEquals("Should use TermQueryTextExtractor", "term", result);

        NeuralKNNQuery neuralQuery = mock(NeuralKNNQuery.class);
        when(neuralQuery.getOriginalQueryText()).thenReturn("neural query");
        result = registry.extractQueryText(neuralQuery, "content");
        assertEquals("Should use NeuralQueryTextExtractor", "neural query", result);

        // Test with boolean query
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("content", "term1")), BooleanClause.Occur.MUST);
        builder.add(new TermQuery(new Term("content", "term2")), BooleanClause.Occur.MUST);
        BooleanQuery booleanQuery = builder.build();

        result = registry.extractQueryText(booleanQuery, "content");
        assertEquals("Should use BooleanQueryTextExtractor", "term1 term2", result);

        // Test with unregistered query type
        // Create a custom query type that doesn't have a registered extractor
        class CustomQuery extends org.apache.lucene.search.Query {
            @Override
            public String toString(String field) {
                return "custom";
            }

            @Override
            public boolean equals(Object obj) {
                return false;
            }

            @Override
            public int hashCode() {
                return 0;
            }

            @Override
            public void visit(org.apache.lucene.search.QueryVisitor visitor) {
                // Simple implementation - just accept this query
                visitor.visitLeaf(this);
            }
        }

        CustomQuery customQuery = new CustomQuery();

        // This should return null since there's no extractor for CustomQuery
        result = registry.extractQueryText(customQuery, "content");
        assertNull("Should return null for unregistered query type", result);

        // Register a custom extractor
        registry.register(CustomQuery.class, (q, f) -> "custom-extracted");

        result = registry.extractQueryText(customQuery, "content");
        assertEquals("Should use custom extractor", "custom-extracted", result);
    }
}
