/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.util;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.knn.index.query.KNNQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralKNNQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.apache.lucene.search.join.ScoreMode;

import java.util.Locale;

public class ProcessorUtilsQueryTextTests extends OpenSearchTestCase {

    public void testExtractQueryTextFromBuilder_NullQuery() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ProcessorUtils.extractQueryTextFromBuilder(null)
        );
        assertEquals(String.format(Locale.ROOT, "query builder cannot be null"), exception.getMessage());
    }

    public void testExtractQueryTextFromBuilder_MatchQuery() {
        MatchQueryBuilder matchQuery = new MatchQueryBuilder("field", "test value");
        String result = ProcessorUtils.extractQueryTextFromBuilder(matchQuery);
        assertEquals("test value", result);
    }

    public void testExtractQueryTextFromBuilder_TermQuery() {
        TermQueryBuilder termQuery = new TermQueryBuilder("field", "test");
        String result = ProcessorUtils.extractQueryTextFromBuilder(termQuery);
        assertEquals("test", result);
    }

    public void testExtractQueryTextFromBuilder_BoolQuery() {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        boolQuery.must(new MatchQueryBuilder("field1", "must text"));
        boolQuery.should(new MatchQueryBuilder("field2", "should text"));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ProcessorUtils.extractQueryTextFromBuilder(boolQuery)
        );

        assertEquals(
            String.format(Locale.ROOT, "Query type %s not supported for semantic highlighting.", "BoolQueryBuilder"),
            exception.getMessage()
        );
    }

    public void testExtractQueryTextFromBuilder_EmptyBoolQuery() {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ProcessorUtils.extractQueryTextFromBuilder(boolQuery)
        );

        assertEquals(
            String.format(Locale.ROOT, "Query type %s not supported for semantic highlighting.", "BoolQueryBuilder"),
            exception.getMessage()
        );
    }

    public void testExtractQueryTextFromBuilder_HybridQuery() {
        HybridQueryBuilder hybridQuery = new HybridQueryBuilder();
        hybridQuery.add(new MatchQueryBuilder("field1", "hybrid1"));
        hybridQuery.add(new MatchQueryBuilder("field2", "hybrid2"));

        String result = ProcessorUtils.extractQueryTextFromBuilder(hybridQuery);
        assertEquals("hybrid1 hybrid2", result);
    }

    public void testExtractQueryTextFromBuilder_NestedQuery() {
        NestedQueryBuilder nestedQuery = new NestedQueryBuilder(
            "nested_field",
            new MatchQueryBuilder("nested_field.text", "nested text"),
            ScoreMode.Avg
        );
        String result = ProcessorUtils.extractQueryTextFromBuilder(nestedQuery);
        assertEquals("nested text", result);
    }

    public void testExtractQueryTextFromBuilder_KNNQuery() {
        // KNN query is not supported for semantic highlighting
        KNNQueryBuilder knnQuery = new KNNQueryBuilder("vector_field", new float[] { 1.0f, 2.0f }, 5);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ProcessorUtils.extractQueryTextFromBuilder(knnQuery)
        );

        assertEquals(
            String.format(Locale.ROOT, "Query type %s not supported for semantic highlighting.", "KNNQueryBuilder"),
            exception.getMessage()
        );
    }

    public void testExtractQueryTextFromBuilder_NeuralKNNQuery() {
        // NeuralKNN query with original query text
        NeuralKNNQueryBuilder neuralKnnQuery = NeuralKNNQueryBuilder.builder()
            .fieldName("vector_field")
            .vector(new float[] { 1.0f, 2.0f })
            .k(5)
            .originalQueryText("original neural query")
            .build();

        String result = ProcessorUtils.extractQueryTextFromBuilder(neuralKnnQuery);
        assertEquals("original neural query", result);
    }

    public void testExtractQueryTextFromBuilder_NeuralKNNQueryWithoutText() {
        // NeuralKNN query without original query text
        NeuralKNNQueryBuilder neuralKnnQuery = NeuralKNNQueryBuilder.builder()
            .fieldName("vector_field")
            .vector(new float[] { 1.0f, 2.0f })
            .k(5)
            .build();

        String result = ProcessorUtils.extractQueryTextFromBuilder(neuralKnnQuery);
        assertNull(result);
    }
}
