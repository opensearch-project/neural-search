/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.index.query.AbstractQueryBuilder.BOOST_FIELD;
import static org.opensearch.index.query.AbstractQueryBuilder.NAME_FIELD;
import static org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder.ANALYZER_FIELD;
import static org.opensearch.neuralsearch.util.TestUtils.xContentBuilderToMap;
import static org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder.MAX_TOKEN_SCORE_FIELD;
import static org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder.MODEL_ID_FIELD;
import static org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder.NAME;
import static org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder.QUERY_TEXT_FIELD;
import static org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder.QUERY_TOKENS_FIELD;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.neuralsearch.util.TestUtils;
import org.opensearch.transport.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SetOnce;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.FilterStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.neuralsearch.util.prune.PruneType;
import org.opensearch.test.OpenSearchTestCase;

import lombok.SneakyThrows;

public class NeuralSparseQueryBuilderTests extends OpenSearchTestCase {

    private static final String FIELD_NAME = "testField";
    private static final String QUERY_TEXT = "Hello world!";
    private static final String MODEL_ID = "mfgfgdsfgfdgsde";
    private static final String ANALYZER_NAME = "standard";
    private static final float BOOST = 1.8f;
    private static final String QUERY_NAME = "queryName";
    private static final Float MAX_TOKEN_SCORE = 123f;
    private static final Supplier<Map<String, Float>> QUERY_TOKENS_SUPPLIER = () -> Map.of("hello", 1.f, "world", 2.f);

    @Before
    public void setup() {
        setUpClusterService(Version.CURRENT);

        // Initialize EventStatsManager for tests
        TestUtils.initializeEventStatsManager();
    }

    @SneakyThrows
    public void testFromXContent_whenBuiltWithQueryText_thenBuildSuccessfully() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "model_id": "string"
              }
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = NeuralSparseQueryBuilder.fromXContent(contentParser);

        assertEquals(FIELD_NAME, sparseEncodingQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, sparseEncodingQueryBuilder.queryText());
        assertEquals(MODEL_ID, sparseEncodingQueryBuilder.modelId());
    }

    @SneakyThrows
    public void testFromXContent_whenBuiltWithQueryTokens_thenBuildSuccessfully() {
        /*
          {
              "VECTOR_FIELD": {
                "query_tokens": {
                    "token_a": float_score_a,
                    "token_b": float_score_b
              }
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TOKENS_FIELD.getPreferredName(), QUERY_TOKENS_SUPPLIER.get())
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = NeuralSparseQueryBuilder.fromXContent(contentParser);

        assertEquals(FIELD_NAME, sparseEncodingQueryBuilder.fieldName());
        assertEquals(QUERY_TOKENS_SUPPLIER.get(), sparseEncodingQueryBuilder.queryTokensSupplier().get());
    }

    @SneakyThrows
    public void testFromXContent_whenBuiltWithQueryTextAndAnalyzer_thenBuildSuccessfully() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "analyzer": "string"
              }
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(ANALYZER_FIELD.getPreferredName(), ANALYZER_NAME)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = NeuralSparseQueryBuilder.fromXContent(contentParser);

        assertEquals(FIELD_NAME, sparseEncodingQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, sparseEncodingQueryBuilder.queryText());
        assertEquals(ANALYZER_NAME, sparseEncodingQueryBuilder.analyzer());
    }

    @SneakyThrows
    public void testFromXContent_whenBuiltWithOptionals_thenBuildSuccessfully() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "model_id": "string",
                "boost": 10.0,
                "_name": "something",
              }
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(BOOST_FIELD.getPreferredName(), BOOST)
            .field(NAME_FIELD.getPreferredName(), QUERY_NAME)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = NeuralSparseQueryBuilder.fromXContent(contentParser);

        assertEquals(FIELD_NAME, sparseEncodingQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, sparseEncodingQueryBuilder.queryText());
        assertEquals(MODEL_ID, sparseEncodingQueryBuilder.modelId());
        assertEquals(BOOST, sparseEncodingQueryBuilder.boost(), 0.0);
        assertEquals(QUERY_NAME, sparseEncodingQueryBuilder.queryName());
    }

    @SneakyThrows
    public void testFromXContent_whenBuiltWithMaxTokenScore_thenThrowWarning() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "model_id": "string",
                "max_token_score": 123.0
              }
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(MAX_TOKEN_SCORE_FIELD.getPreferredName(), MAX_TOKEN_SCORE)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = NeuralSparseQueryBuilder.fromXContent(contentParser);
        assertWarnings("Deprecated field [max_token_score] used, this field is unused and will be removed entirely");
    }

    @SneakyThrows
    public void testFromXContent_whenBuildWithMultipleRootFields_thenFail() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "model_id": "string",
                "boost": 10.0,
                "_name": "something",
              },
              "invalid": 10
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(BOOST_FIELD.getPreferredName(), BOOST)
            .field(NAME_FIELD.getPreferredName(), QUERY_NAME)
            .endObject()
            .field("invalid", 10)
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        expectThrows(ParsingException.class, () -> NeuralSparseQueryBuilder.fromXContent(contentParser));
    }

    @SneakyThrows
    public void testFromXContent_whenBuildWithMissingQuery_thenFail() {
        /*
          {
              "VECTOR_FIELD": {
                "model_id": "string"
              }
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        expectThrows(IllegalArgumentException.class, () -> NeuralSparseQueryBuilder.fromXContent(contentParser));
    }

    @SneakyThrows
    public void testFromXContent_whenBuildWithMissingModelIdInCurrentVersion_thenSuccess() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string"
              }
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = NeuralSparseQueryBuilder.fromXContent(contentParser);

        assertNull(sparseEncodingQueryBuilder.modelId());
    }

    @SneakyThrows
    public void testFromXContent_whenBuildWithMissingModelIdInOldVersion_thenFail() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string"
              }
          }
        */
        setUpClusterService(Version.V_2_12_0);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        expectThrows(IllegalArgumentException.class, () -> NeuralSparseQueryBuilder.fromXContent(contentParser));
    }

    @SneakyThrows
    public void testFromXContent_whenBuildWithDuplicateParameters_thenFail() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "query_text": "string",
                "model_id": "string",
                "model_id": "string"
              }
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        expectThrows(IOException.class, () -> NeuralSparseQueryBuilder.fromXContent(contentParser));
    }

    @SneakyThrows
    public void testFromXContent_whenBuildWithEmptyQuery_thenFail() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": ""
              }
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), StringUtils.EMPTY)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        expectThrows(IllegalArgumentException.class, () -> NeuralSparseQueryBuilder.fromXContent(contentParser));
    }

    @SneakyThrows
    public void testFromXContent_whenBuildWithEmptyModelId_thenFail() {
        /*
          {
              "VECTOR_FIELD": {
                "model_id": ""
              }
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(MODEL_ID_FIELD.getPreferredName(), StringUtils.EMPTY)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        expectThrows(IllegalArgumentException.class, () -> NeuralSparseQueryBuilder.fromXContent(contentParser));
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    public void testToXContent() {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(FIELD_NAME)
            .modelId(MODEL_ID)
            .queryText(QUERY_TEXT)
            .maxTokenScore(MAX_TOKEN_SCORE)
            .queryTokensSupplier(QUERY_TOKENS_SUPPLIER)
            .analyzer(ANALYZER_NAME);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder = sparseEncodingQueryBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        Map<String, Object> out = xContentBuilderToMap(builder);

        Object outer = out.get(NAME);
        if (!(outer instanceof Map)) {
            fail("sparse encoding does not map to nested object");
        }

        Map<String, Object> outerMap = (Map<String, Object>) outer;

        assertEquals(1, outerMap.size());
        assertTrue(outerMap.containsKey(FIELD_NAME));

        Object secondInner = outerMap.get(FIELD_NAME);
        if (!(secondInner instanceof Map)) {
            fail("field name does not map to nested object");
        }

        Map<String, Object> secondInnerMap = (Map<String, Object>) secondInner;

        assertEquals(MODEL_ID, secondInnerMap.get(MODEL_ID_FIELD.getPreferredName()));
        assertEquals(QUERY_TEXT, secondInnerMap.get(QUERY_TEXT_FIELD.getPreferredName()));
        assertEquals(MAX_TOKEN_SCORE, (Double) secondInnerMap.get(MAX_TOKEN_SCORE_FIELD.getPreferredName()), 0.0);
        assertEquals(ANALYZER_NAME, secondInnerMap.get(ANALYZER_FIELD.getPreferredName()));
        Map<String, Double> parsedQueryTokens = (Map<String, Double>) secondInnerMap.get(QUERY_TOKENS_FIELD.getPreferredName());
        assertEquals(QUERY_TOKENS_SUPPLIER.get().keySet(), parsedQueryTokens.keySet());
        for (Map.Entry<String, Float> entry : QUERY_TOKENS_SUPPLIER.get().entrySet()) {
            assertEquals(entry.getValue(), parsedQueryTokens.get(entry.getKey()).floatValue(), 0);
        }
    }

    public void testStreams_whenCurrentVersion_thenSuccess() {
        setUpClusterService(Version.CURRENT);
        testStreams(true);
        testStreamsWithQueryTokensOnly();
    }

    public void testStreams_whenMinVersionIsBeforeDefaultModelId_thenSuccess() {
        setUpClusterService(Version.V_2_12_0);
        testStreams(false);
        testStreamsWithQueryTokensOnly();
    }

    @SneakyThrows
    private void testStreams(boolean verifyAnalyzer) {
        NeuralSparseQueryBuilder original = new NeuralSparseQueryBuilder();
        original.fieldName(FIELD_NAME);
        original.queryText(QUERY_TEXT);
        original.maxTokenScore(MAX_TOKEN_SCORE);
        original.modelId(MODEL_ID);
        original.boost(BOOST);
        original.queryName(QUERY_NAME);

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        original.writeTo(streamOutput);

        FilterStreamInput filterStreamInput = new NamedWriteableAwareStreamInput(
            streamOutput.bytes().streamInput(),
            new NamedWriteableRegistry(
                List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new))
            )
        );

        NeuralSparseQueryBuilder copy = new NeuralSparseQueryBuilder(filterStreamInput);
        assertEquals(original, copy);

        SetOnce<Map<String, Float>> queryTokensSetOnce = new SetOnce<>();
        queryTokensSetOnce.set(Map.of("hello", 1.0f, "world", 2.0f));
        original.queryTokensSupplier(queryTokensSetOnce::get);

        streamOutput = new BytesStreamOutput();
        original.writeTo(streamOutput);

        filterStreamInput = new NamedWriteableAwareStreamInput(
            streamOutput.bytes().streamInput(),
            new NamedWriteableRegistry(
                List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new))
            )
        );

        copy = new NeuralSparseQueryBuilder(filterStreamInput);
        assertEquals(original, copy);

        if (verifyAnalyzer) {
            original.analyzer(ANALYZER_NAME);

            streamOutput = new BytesStreamOutput();
            original.writeTo(streamOutput);

            filterStreamInput = new NamedWriteableAwareStreamInput(
                streamOutput.bytes().streamInput(),
                new NamedWriteableRegistry(
                    List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new))
                )
            );

            copy = new NeuralSparseQueryBuilder(filterStreamInput);
            assertEquals(original, copy);
        }
    }

    @SneakyThrows
    private void testStreamsWithQueryTokensOnly() {
        NeuralSparseQueryBuilder original = new NeuralSparseQueryBuilder();
        original.fieldName(FIELD_NAME);
        original.queryTokensSupplier(QUERY_TOKENS_SUPPLIER);

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        original.writeTo(streamOutput);

        FilterStreamInput filterStreamInput = new NamedWriteableAwareStreamInput(
            streamOutput.bytes().streamInput(),
            new NamedWriteableRegistry(
                List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new))
            )
        );

        NeuralSparseQueryBuilder copy = new NeuralSparseQueryBuilder(filterStreamInput);
        assertEquals(original, copy);
    }

    public void testHashAndEquals() {
        String fieldName1 = "field 1";
        String fieldName2 = "field 2";
        String queryText1 = "query text 1";
        String queryText2 = "query text 2";
        String modelId1 = "model-1";
        String modelId2 = "model-2";
        float maxTokenScore1 = 1.1f;
        float maxTokenScore2 = 2.2f;
        float boost1 = 1.8f;
        float boost2 = 3.8f;
        String queryName1 = "query-1";
        String queryName2 = "query-2";
        Map<String, Float> queryTokens1 = Map.of("hello", 1.0f, "world", 2.0f);
        Map<String, Float> queryTokens2 = Map.of("hello", 1.0f, "world", 2.2f);

        NeuralSparseQueryBuilder sparseEncodingQueryBuilder_baseline = new NeuralSparseQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .maxTokenScore(maxTokenScore1)
            .boost(boost1)
            .queryName(queryName1);

        // Identical to sparseEncodingQueryBuilder_baseline
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder_baselineCopy = new NeuralSparseQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .maxTokenScore(maxTokenScore1)
            .boost(boost1)
            .queryName(queryName1);

        // Identical to sparseEncodingQueryBuilder_baseline except default boost and query name
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder_defaultBoostAndQueryName = new NeuralSparseQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .maxTokenScore(maxTokenScore1);

        // Identical to sparseEncodingQueryBuilder_baseline except diff field name
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder_diffFieldName = new NeuralSparseQueryBuilder().fieldName(fieldName2)
            .queryText(queryText1)
            .modelId(modelId1)
            .maxTokenScore(maxTokenScore1)
            .boost(boost1)
            .queryName(queryName1);

        // Identical to sparseEncodingQueryBuilder_baseline except diff query text
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder_diffQueryText = new NeuralSparseQueryBuilder().fieldName(fieldName1)
            .queryText(queryText2)
            .modelId(modelId1)
            .maxTokenScore(maxTokenScore1)
            .boost(boost1)
            .queryName(queryName1);

        // Identical to sparseEncodingQueryBuilder_baseline except diff model ID
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder_diffModelId = new NeuralSparseQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId2)
            .maxTokenScore(maxTokenScore1)
            .boost(boost1)
            .queryName(queryName1);

        // Identical to sparseEncodingQueryBuilder_baseline except diff boost
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder_diffBoost = new NeuralSparseQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .maxTokenScore(maxTokenScore1)
            .boost(boost2)
            .queryName(queryName1);

        // Identical to sparseEncodingQueryBuilder_baseline except diff query name
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder_diffQueryName = new NeuralSparseQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .maxTokenScore(maxTokenScore1)
            .boost(boost1)
            .queryName(queryName2);

        // Identical to sparseEncodingQueryBuilder_baseline except diff max token score
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder_diffMaxTokenScore = new NeuralSparseQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .maxTokenScore(maxTokenScore2)
            .boost(boost1)
            .queryName(queryName1);

        // Identical to sparseEncodingQueryBuilder_baseline except non-null query tokens supplier
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder_nonNullQueryTokens = new NeuralSparseQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .maxTokenScore(maxTokenScore1)
            .boost(boost1)
            .queryName(queryName1)
            .queryTokensSupplier(() -> queryTokens1);

        // Identical to sparseEncodingQueryBuilder_baseline except non-null query tokens supplier
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder_diffQueryTokens = new NeuralSparseQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .maxTokenScore(maxTokenScore1)
            .boost(boost1)
            .queryName(queryName1)
            .queryTokensSupplier(() -> queryTokens2);

        // Identical to sparseEncodingQueryBuilder_baseline except null query text
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder_nullQueryText = new NeuralSparseQueryBuilder().fieldName(fieldName1)
            .modelId(modelId1)
            .boost(boost1)
            .queryName(queryName1);

        // Identical to sparseEncodingQueryBuilder_baseline except null model id
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder_nullModelId = new NeuralSparseQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .boost(boost1)
            .queryName(queryName1);

        // Identical to sparseEncodingQueryBuilder_baseline except different two phase info with default one
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder_diffTwoPhaseInfo = new NeuralSparseQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .maxTokenScore(maxTokenScore1)
            .boost(boost1)
            .queryName(queryName1)
            .neuralSparseQueryTwoPhaseInfo(
                new NeuralSparseQueryTwoPhaseInfo(NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PHASE_ONE, 0.5f, PruneType.MAX_RATIO)
            );

        // Identical to sparseEncodingQueryBuilder_baseline except non-null analyzer
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder_nonNullAnalyzer = new NeuralSparseQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .maxTokenScore(maxTokenScore1)
            .boost(boost1)
            .queryName(queryName1)
            .analyzer("standard");

        assertEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_baseline);
        assertEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_baseline.hashCode());

        assertEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_baselineCopy);
        assertEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_baselineCopy.hashCode());

        assertNotEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_defaultBoostAndQueryName);
        assertNotEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_defaultBoostAndQueryName.hashCode());

        assertNotEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_diffFieldName);
        assertNotEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_diffFieldName.hashCode());

        assertNotEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_diffQueryText);
        assertNotEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_diffQueryText.hashCode());

        assertNotEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_diffModelId);
        assertNotEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_diffModelId.hashCode());

        assertNotEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_diffBoost);
        assertNotEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_diffBoost.hashCode());

        assertNotEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_diffQueryName);
        assertNotEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_diffQueryName.hashCode());

        assertNotEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_diffMaxTokenScore);
        assertNotEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_diffMaxTokenScore.hashCode());

        assertNotEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_nonNullQueryTokens);
        assertNotEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_nonNullQueryTokens.hashCode());

        assertNotEquals(sparseEncodingQueryBuilder_nonNullQueryTokens, sparseEncodingQueryBuilder_diffQueryTokens);
        assertNotEquals(sparseEncodingQueryBuilder_nonNullQueryTokens.hashCode(), sparseEncodingQueryBuilder_diffQueryTokens.hashCode());

        assertNotEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_nullQueryText);
        assertNotEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_nullQueryText.hashCode());

        assertNotEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_nullModelId);
        assertNotEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_nullModelId.hashCode());

        assertNotEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_diffTwoPhaseInfo);
        assertNotEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_diffTwoPhaseInfo.hashCode());

        assertNotEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_nonNullAnalyzer);
        assertNotEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_nonNullAnalyzer.hashCode());
    }

    @SneakyThrows
    public void testRewrite_whenQueryTokensSupplierNull_thenSetQueryTokensSupplier() {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID);
        Map<String, Float> expectedMap = Map.of("1", 1f, "2", 2f);
        MLCommonsClientAccessor mlCommonsClientAccessor = mock(MLCommonsClientAccessor.class);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(1);
            listener.onResponse(List.of(Map.of("response", List.of(expectedMap))));
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), isA(ActionListener.class));
        NeuralSparseQueryBuilder.initialize(mlCommonsClientAccessor);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        QueryRewriteContext queryRewriteContext = mock(QueryRewriteContext.class);
        doAnswer(invocation -> {
            BiConsumer<Client, ActionListener<?>> biConsumer = invocation.getArgument(0);
            biConsumer.accept(
                null,
                ActionListener.wrap(
                    response -> inProgressLatch.countDown(),
                    err -> fail("Failed to set query tokens supplier: " + err.getMessage())
                )
            );
            return null;
        }).when(queryRewriteContext).registerAsyncAction(any());

        NeuralSparseQueryBuilder queryBuilder = (NeuralSparseQueryBuilder) sparseEncodingQueryBuilder.doRewrite(queryRewriteContext);
        assertNotNull(queryBuilder.queryTokensSupplier());
        assertTrue(inProgressLatch.await(5, TimeUnit.SECONDS));
        assertEquals(expectedMap, queryBuilder.queryTokensSupplier().get());
    }

    @SneakyThrows
    public void testRewrite_whenQueryTokensSupplierNull_andPruneSet_thenSuceessPrune() {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .twoPhaseSharedQueryToken(Map.of())
            .neuralSparseQueryTwoPhaseInfo(
                new NeuralSparseQueryTwoPhaseInfo(NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PHASE_ONE, 3f, PruneType.ABS_VALUE)
            );
        Map<String, Float> expectedMap = Map.of("1", 1f, "2", 5f);
        MLCommonsClientAccessor mlCommonsClientAccessor = mock(MLCommonsClientAccessor.class);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(1);
            listener.onResponse(List.of(Map.of("response", List.of(expectedMap))));
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), isA(ActionListener.class));
        NeuralSparseQueryBuilder.initialize(mlCommonsClientAccessor);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        QueryRewriteContext queryRewriteContext = mock(QueryRewriteContext.class);
        doAnswer(invocation -> {
            BiConsumer<Client, ActionListener<?>> biConsumer = invocation.getArgument(0);
            biConsumer.accept(
                null,
                ActionListener.wrap(
                    response -> inProgressLatch.countDown(),
                    err -> fail("Failed to set query tokens supplier: " + err.getMessage())
                )
            );
            return null;
        }).when(queryRewriteContext).registerAsyncAction(any());

        NeuralSparseQueryBuilder queryBuilder = (NeuralSparseQueryBuilder) sparseEncodingQueryBuilder.doRewrite(queryRewriteContext);
        assertNotNull(queryBuilder.queryTokensSupplier());
        assertTrue(inProgressLatch.await(5, TimeUnit.SECONDS));
        assertEquals(Map.of("2", 5f), queryBuilder.queryTokensSupplier().get());
        assertEquals(Map.of("1", 1f), queryBuilder.twoPhaseSharedQueryToken());
    }

    @SneakyThrows
    public void testRewrite_whenQueryTokensSupplierSet_thenReturnSelf() {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .queryTokensSupplier(QUERY_TOKENS_SUPPLIER);
        QueryBuilder queryBuilder = sparseEncodingQueryBuilder.doRewrite(null);
        assertSame(queryBuilder, sparseEncodingQueryBuilder);

        sparseEncodingQueryBuilder.queryTokensSupplier(() -> null);
        queryBuilder = sparseEncodingQueryBuilder.doRewrite(null);
        assertSame(queryBuilder, sparseEncodingQueryBuilder);
    }

    private void setUpClusterService(Version version) {
        ClusterService clusterService = NeuralSearchClusterTestUtils.mockClusterService(version);
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        NeuralSearchClusterUtil.instance().initialize(clusterService, indexNameExpressionResolver);
    }

    @SneakyThrows
    public void testDoToQuery_successfulDoToQuery() {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(FIELD_NAME)
            .maxTokenScore(MAX_TOKEN_SCORE)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .queryTokensSupplier(QUERY_TOKENS_SUPPLIER);
        QueryShardContext mockedQueryShardContext = mock(QueryShardContext.class);
        MappedFieldType mockedMappedFieldType = mock(MappedFieldType.class);
        doAnswer(invocation -> "rank_features").when(mockedMappedFieldType).typeName();
        doAnswer(invocation -> mockedMappedFieldType).when(mockedQueryShardContext).fieldMapper(any());

        BooleanQuery.Builder targetQueryBuilder = new BooleanQuery.Builder();
        targetQueryBuilder.add(FeatureField.newLinearQuery(FIELD_NAME, "hello", 1.f), BooleanClause.Occur.SHOULD);
        targetQueryBuilder.add(FeatureField.newLinearQuery(FIELD_NAME, "world", 2.f), BooleanClause.Occur.SHOULD);

        assertEquals(sparseEncodingQueryBuilder.doToQuery(mockedQueryShardContext), targetQueryBuilder.build());
    }

    @SneakyThrows
    public void testDoToQuery_whenEmptyQueryToken_thenThrowException() {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(FIELD_NAME)
            .maxTokenScore(MAX_TOKEN_SCORE)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .queryTokensSupplier(() -> Collections.emptyMap());
        QueryShardContext mockedQueryShardContext = mock(QueryShardContext.class);
        MappedFieldType mockedMappedFieldType = mock(MappedFieldType.class);
        doAnswer(invocation -> "rank_features").when(mockedMappedFieldType).typeName();
        doAnswer(invocation -> mockedMappedFieldType).when(mockedQueryShardContext).fieldMapper(any());
        expectThrows(IllegalArgumentException.class, () -> sparseEncodingQueryBuilder.doToQuery(mock(QueryShardContext.class)));
    }

    @SneakyThrows
    public void testRewrite_whenQueryTokensSupplierNull_andAnalyzerNotNull_thenReturnSelf() {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .analyzer(ANALYZER_NAME);
        QueryBuilder queryBuilder = sparseEncodingQueryBuilder.doRewrite(null);
        assertSame(queryBuilder, sparseEncodingQueryBuilder);
    }

    public void testGetQueryTokens_queryTokensSupplierNonNull() {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(FIELD_NAME)
            .queryTokensSupplier(QUERY_TOKENS_SUPPLIER);
        QueryShardContext mockedQueryShardContext = mock(QueryShardContext.class);

        assertEquals(QUERY_TOKENS_SUPPLIER.get(), sparseEncodingQueryBuilder.getQueryTokens(mockedQueryShardContext));
    }

    @SneakyThrows
    public void testGetQueryTokens_useAnalyzerWithoutTokenWeights() {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(FIELD_NAME)
            .queryText("hello world")
            .analyzer("default");

        QueryShardContext mockedQueryShardContext = mock(QueryShardContext.class);
        IndexAnalyzers mockIndexAnalyzers = new IndexAnalyzers(
            Map.of("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        when(mockedQueryShardContext.getIndexAnalyzers()).thenReturn(mockIndexAnalyzers);

        Map<String, Float> queryTokens = sparseEncodingQueryBuilder.getQueryTokens(mockedQueryShardContext);
        assertEquals(2, queryTokens.size());
        assertEquals(1f, queryTokens.get("hello"), 0f);
        assertEquals(1f, queryTokens.get("world"), 0f);
    }

    @SneakyThrows
    public void testGetQueryTokens_whenAnalyzerNotFound_thenThrowException() {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(FIELD_NAME)
            .queryText("hello world")
            .analyzer("test");

        QueryShardContext mockedQueryShardContext = mock(QueryShardContext.class);
        IndexAnalyzers mockIndexAnalyzers = new IndexAnalyzers(
            Map.of("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        when(mockedQueryShardContext.getIndexAnalyzers()).thenReturn(mockIndexAnalyzers);

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> sparseEncodingQueryBuilder.getQueryTokens(mockedQueryShardContext)
        );
        assertEquals("Analyzer [test] not found in shard context. ", exception.getMessage());
    }

    @SneakyThrows
    public void testGetQueryTokens_useAnalyzerWithTokenWeights() {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(FIELD_NAME)
            .queryText("hello world")
            .analyzer("default");

        QueryShardContext mockedQueryShardContext = mock(QueryShardContext.class);
        Analyzer mockedAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String s) {
                Tokenizer tokenizer = new Tokenizer() {
                    private int numToCall = 0;
                    CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
                    PayloadAttribute payloadAtt = addAttribute(PayloadAttribute.class);

                    @Override
                    public boolean incrementToken() throws IOException {
                        clearAttributes();
                        switch (numToCall) {
                            case 0:
                                termAtt.append("hello");
                                payloadAtt.setPayload(new BytesRef(ByteBuffer.allocate(4).putFloat(1.23f).array()));
                                numToCall++;
                                return true;
                            case 1:
                                termAtt.append("world");
                                payloadAtt.setPayload(new BytesRef(ByteBuffer.allocate(4).putFloat(2.34f).array()));
                                numToCall++;
                                return true;
                            default:
                                return false;
                        }
                    }
                };
                return new TokenStreamComponents(tokenizer, tokenizer);
            }
        };

        IndexAnalyzers mockIndexAnalyzers = new IndexAnalyzers(
            Map.of("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, mockedAnalyzer)),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        when(mockedQueryShardContext.getIndexAnalyzers()).thenReturn(mockIndexAnalyzers);

        Map<String, Float> queryTokens = sparseEncodingQueryBuilder.getQueryTokens(mockedQueryShardContext);
        assertEquals(2, queryTokens.size());
        assertEquals(1.23f, queryTokens.get("hello"), 0f);
        assertEquals(2.34f, queryTokens.get("world"), 0f);
    }

    @SneakyThrows
    public void testGetQueryTokens_useAnalyzerWithMalformedTokenWeights_thenFail() {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(FIELD_NAME)
            .queryText("hello world")
            .analyzer("default");

        QueryShardContext mockedQueryShardContext = mock(QueryShardContext.class);
        Analyzer mockedAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String s) {
                Tokenizer tokenizer = new Tokenizer() {
                    private int numToCall = 0;
                    CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
                    PayloadAttribute payloadAtt = addAttribute(PayloadAttribute.class);

                    @Override
                    public boolean incrementToken() throws IOException {
                        clearAttributes();
                        switch (numToCall) {
                            case 0:
                                termAtt.append("hello");
                                payloadAtt.setPayload(new BytesRef(ByteBuffer.allocate(2).putShort((short) 1).array()));
                                numToCall++;
                                return true;
                            case 1:
                                termAtt.append("world");
                                payloadAtt.setPayload(new BytesRef(ByteBuffer.allocate(4).putFloat(2.34f).array()));
                                numToCall++;
                                return true;
                            default:
                                return false;
                        }
                    }
                };
                return new TokenStreamComponents(tokenizer, tokenizer);
            }
        };

        IndexAnalyzers mockIndexAnalyzers = new IndexAnalyzers(
            Map.of("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, mockedAnalyzer)),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        when(mockedQueryShardContext.getIndexAnalyzers()).thenReturn(mockIndexAnalyzers);

        OpenSearchException exception = assertThrows(
            OpenSearchException.class,
            () -> sparseEncodingQueryBuilder.getQueryTokens(mockedQueryShardContext)
        );
        assertEquals("failed to parse query token weight from analyzer. ", exception.getMessage());
    }
}
