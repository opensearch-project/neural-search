/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.util;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.BoostingQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.ScriptScoreQueryBuilder;
import org.opensearch.knn.index.query.KNNQueryBuilder;
import org.opensearch.neuralsearch.query.HybridFusionQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralKNNQueryBuilder;
import org.opensearch.script.Script;
import org.opensearch.test.OpenSearchTestCase;
import org.apache.lucene.search.join.ScoreMode;

import java.util.List;
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

        String result = ProcessorUtils.extractQueryTextFromBuilder(boolQuery);
        assertEquals("must text should text", result);
    }

    public void testExtractQueryTextFromBuilder_EmptyBoolQuery() {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ProcessorUtils.extractQueryTextFromBuilder(boolQuery)
        );

        assertTrue(exception.getMessage().contains("bool query has no extractable clause"));
    }

    public void testExtractQueryTextFromBuilder_HybridQuery() {
        HybridQueryBuilder hybridQuery = new HybridQueryBuilder();
        hybridQuery.add(new MatchQueryBuilder("field1", "hybrid1"));
        hybridQuery.add(new MatchQueryBuilder("field2", "hybrid2"));

        String result = ProcessorUtils.extractQueryTextFromBuilder(hybridQuery);
        assertEquals("hybrid1 hybrid2", result);
    }

    /**
     * Fused mode replaces the hybrid at the coordinator before any response processor runs, so a processor extracting the
     * query text is handed the substitute. It must read the same text a classic hybrid gives — the substitute itself has
     * none.
     */
    public void testExtractQueryTextFromBuilder_FusedHybridReadsTheCarriedOriginal() {
        HybridQueryBuilder original = new HybridQueryBuilder();
        original.add(new MatchQueryBuilder("field1", "hybrid1"));
        original.add(new MatchQueryBuilder("field2", "hybrid2"));

        assertEquals(
            "the substitute must yield exactly what the hybrid it replaced yields",
            ProcessorUtils.extractQueryTextFromBuilder(original),
            ProcessorUtils.extractQueryTextFromBuilder(fusedSubstitute(original))
        );
    }

    /**
     * The Tail carries each leg as a match-set form, and a materialized one is {@code bool{filter: ids, filter: term(_index)}}
     * — which the bool case above would read as the index name. So extracting from the substitute's own clauses is worse
     * than failing, and with no original carried this has to fail.
     */
    public void testExtractQueryTextFromBuilder_FusedHybridWithoutOriginalIsUnsupported() {
        HybridFusionQueryBuilder substitute = fusedSubstitute(null);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ProcessorUtils.extractQueryTextFromBuilder(substitute)
        );
        assertTrue(exception.getMessage().contains("HybridFusionQueryBuilder"));
        assertTrue(exception.getMessage().contains("not supported for semantic highlighting"));
    }

    private static HybridFusionQueryBuilder fusedSubstitute(HybridQueryBuilder original) {
        // A one-document window whose Tail holds the shape a materialized kNN/neural leg takes, so the negative case is a
        // query whose clauses really would produce text if they were read.
        return new HybridFusionQueryBuilder(
            new String[] { "d1" },
            new String[] { "idx" },
            new float[] { 1.0f },
            List.of(new BoolQueryBuilder().filter(new IdsQueryBuilder().addIds("d1")).filter(new TermQueryBuilder("_index", "idx"))),
            List.of(),
            List.of(),
            original
        );
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

    public void testExtractQueryTextFromBuilder_ConstantScoreUnwrapsInner() {
        ConstantScoreQueryBuilder query = new ConstantScoreQueryBuilder(new MatchQueryBuilder("body", "constant text"));
        assertEquals("constant text", ProcessorUtils.extractQueryTextFromBuilder(query));
    }

    public void testExtractQueryTextFromBuilder_FunctionScoreUnwrapsInner() {
        FunctionScoreQueryBuilder query = new FunctionScoreQueryBuilder(new MatchQueryBuilder("body", "boosted text"));
        assertEquals("boosted text", ProcessorUtils.extractQueryTextFromBuilder(query));
    }

    public void testExtractQueryTextFromBuilder_ScriptScoreUnwrapsInner() {
        ScriptScoreQueryBuilder query = new ScriptScoreQueryBuilder(
            new MatchQueryBuilder("body", "scored text"),
            new Script("doc['x'].value")
        );
        assertEquals("scored text", ProcessorUtils.extractQueryTextFromBuilder(query));
    }

    public void testExtractQueryTextFromBuilder_BoostingUsesPositiveOnly() {
        BoostingQueryBuilder query = new BoostingQueryBuilder(
            new MatchQueryBuilder("body", "positive text"),
            new MatchQueryBuilder("body", "negative text")
        );
        assertEquals("positive text", ProcessorUtils.extractQueryTextFromBuilder(query));
    }

    public void testExtractQueryTextFromBuilder_DisMax() {
        DisMaxQueryBuilder query = new DisMaxQueryBuilder();
        query.add(new MatchQueryBuilder("a", "alpha"));
        query.add(new MatchQueryBuilder("b", "beta"));
        assertEquals("alpha beta", ProcessorUtils.extractQueryTextFromBuilder(query));
    }
}
