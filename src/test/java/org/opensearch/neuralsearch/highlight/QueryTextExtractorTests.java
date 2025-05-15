/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.index.search.OpenSearchToParentBlockJoinQuery;
import org.opensearch.neuralsearch.highlight.extractor.BooleanQueryTextExtractor;
import org.opensearch.neuralsearch.highlight.extractor.NestedQueryTextExtractor;
import org.opensearch.neuralsearch.highlight.extractor.NeuralQueryTextExtractor;
import org.opensearch.neuralsearch.highlight.extractor.QueryTextExtractorRegistry;
import org.opensearch.neuralsearch.highlight.extractor.TermQueryTextExtractor;
import org.opensearch.neuralsearch.highlight.extractor.HybridQueryTextExtractor;
import org.opensearch.neuralsearch.query.NeuralKNNQuery;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.neuralsearch.query.HybridQueryContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

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
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> extractor.extractQueryText(booleanQuery, "content")
        );
        assertTrue(
            "Should throw IllegalArgumentException with correct message",
            exception.getMessage().contains("Expected TermQuery but got BooleanQuery")
        );
    }

    /**
     * Test the NestedQueryTextExtractor
     */
    public void testNestedQueryTextExtractor_whenNeuralKnnQueryNested() {
        // Create a registry for the testNestedQueryTextExtractor to use
        QueryTextExtractorRegistry localRegistry = new QueryTextExtractorRegistry();
        NestedQueryTextExtractor extractor = new NestedQueryTextExtractor(localRegistry);

        NeuralKNNQuery neuralQuery = new NeuralKNNQuery(new TermQuery(new Term("content", "dummy")), "semantic search query");
        OpenSearchToParentBlockJoinQuery openSearchToParentBlockJoinQuery = new OpenSearchToParentBlockJoinQuery(
            neuralQuery,
            null,
            ScoreMode.Max,
            "nestedpath"
        );
        String result = extractor.extractQueryText(openSearchToParentBlockJoinQuery, "content");
        assertEquals("Should extract original query text", "semantic search query", result);
    }

    /**
     * Tests the NeuralQueryTextExtractor
     */
    public void testNeuralQueryExtractor() {
        NeuralQueryTextExtractor extractor = new NeuralQueryTextExtractor();

        // Test with NeuralKNNQuery
        NeuralKNNQuery neuralQuery = new NeuralKNNQuery(new TermQuery(new Term("content", "dummy")), "semantic search query");
        String result = extractor.extractQueryText(neuralQuery, "content");
        assertEquals("Should extract original query text", "semantic search query", result);

        // Test with non-NeuralKNNQuery
        TermQuery termQuery = new TermQuery(new Term("content", "term"));
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> extractor.extractQueryText(termQuery, "content")
        );
        assertTrue(
            "Should throw IllegalArgumentException with correct message",
            exception.getMessage().contains("Expected NeuralKNNQuery but got TermQuery")
        );
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
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> extractor.extractQueryText(termQuery, "content")
        );
        assertTrue(
            "Should throw IllegalArgumentException with correct message",
            exception.getMessage().contains("Expected BooleanQuery but got TermQuery")
        );

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

        NeuralKNNQuery neuralQuery = new NeuralKNNQuery(new TermQuery(new Term("content", "dummy")), "semantic query");
        result = registry.extractQueryText(neuralQuery, "content");
        assertEquals("Should use NeuralQueryTextExtractor", "semantic query", result);

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

    /**
     * Tests the HybridQueryTextExtractor
     */
    public void testHybridQueryExtractor() {
        // Create a hybrid query with multiple sub-queries
        List<Query> subQueries = new ArrayList<>();

        // Add a term query
        TermQuery termQuery = new TermQuery(new Term("content", "machine"));
        subQueries.add(termQuery);

        // Add a boolean query (match query)
        BooleanQuery.Builder boolBuilder = new BooleanQuery.Builder();
        boolBuilder.add(new TermQuery(new Term("content", "learning")), BooleanClause.Occur.MUST);
        subQueries.add(boolBuilder.build());

        // Add a neural query
        NeuralKNNQuery neuralQuery = new NeuralKNNQuery(new TermQuery(new Term("content", "dummy")), "AI systems that can learn");
        subQueries.add(neuralQuery);

        // Create the hybrid query
        HybridQuery hybridQuery = new HybridQuery(subQueries, HybridQueryContext.builder().build());

        // Test extraction
        String result = registry.extractQueryText(hybridQuery, "content");
        assertEquals("Should combine all query texts correctly", "machine learning AI systems that can learn", result);

        // Test with non-HybridQuery
        TermQuery nonHybridQuery = new TermQuery(new Term("content", "term"));
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            HybridQueryTextExtractor extractor = new HybridQueryTextExtractor(registry);
            extractor.extractQueryText(nonHybridQuery, "content");
        });
        assertTrue(
            "Should throw IllegalArgumentException with correct message",
            exception.getMessage().contains("Expected HybridQuery but got TermQuery")
        );
    }

    /**
     * Tests the HybridQueryTextExtractor with null sub-queries
     */
    public void testHybridQueryExtractorWithNullQueries() {
        // Create a hybrid query with null sub-queries
        List<Query> subQueries = new ArrayList<>();
        subQueries.add(new TermQuery(new Term("content", "valid"))); // Need at least one valid query
        subQueries.add(null);

        // Create the hybrid query
        HybridQuery hybridQuery = new HybridQuery(subQueries, HybridQueryContext.builder().build());

        // Test extraction
        String result = registry.extractQueryText(hybridQuery, "content");
        assertEquals("Should handle null queries gracefully", "valid", result);
    }

    /**
     * Tests the HybridQueryTextExtractor with invalid field names
     */
    public void testHybridQueryExtractorWithInvalidField() {
        List<Query> subQueries = new ArrayList<>();

        // Add a term query with non-matching field
        TermQuery termQuery = new TermQuery(new Term("title", "machine"));
        subQueries.add(termQuery);

        // Add a neural query
        NeuralKNNQuery neuralQuery = new NeuralKNNQuery(new TermQuery(new Term("content", "dummy")), "AI systems");
        subQueries.add(neuralQuery);

        // Create the hybrid query
        HybridQuery hybridQuery = new HybridQuery(subQueries, HybridQueryContext.builder().build());

        // Test extraction
        String result = registry.extractQueryText(hybridQuery, "content");
        assertEquals("Should only include matching field and neural query", "AI systems", result);
    }

    /**
     * Tests the HybridQueryTextExtractor with mixed valid and invalid queries
     */
    public void testHybridQueryExtractorWithMixedQueries() {
        List<Query> subQueries = new ArrayList<>();

        // Add valid term query first (required)
        TermQuery termQuery = new TermQuery(new Term("content", "machine"));
        subQueries.add(termQuery);

        // Add null query
        subQueries.add(null);

        // Add query with non-matching field
        TermQuery wrongFieldQuery = new TermQuery(new Term("title", "learning"));
        subQueries.add(wrongFieldQuery);

        // Add neural query
        NeuralKNNQuery neuralQuery = new NeuralKNNQuery(new TermQuery(new Term("content", "dummy")), "AI");
        subQueries.add(neuralQuery);

        // Create the hybrid query
        HybridQuery hybridQuery = new HybridQuery(subQueries, HybridQueryContext.builder().build());

        // Test extraction
        String result = registry.extractQueryText(hybridQuery, "content");
        assertEquals("Should handle mixed queries correctly", "machine AI", result);
    }
}
