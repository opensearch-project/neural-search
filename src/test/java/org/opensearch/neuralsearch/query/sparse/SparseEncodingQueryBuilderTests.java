/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query.sparse;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.opensearch.index.query.AbstractQueryBuilder.BOOST_FIELD;
import static org.opensearch.index.query.AbstractQueryBuilder.NAME_FIELD;
import static org.opensearch.neuralsearch.TestUtils.xContentBuilderToMap;
import static org.opensearch.neuralsearch.query.sparse.SparseEncodingQueryBuilder.MODEL_ID_FIELD;
import static org.opensearch.neuralsearch.query.sparse.SparseEncodingQueryBuilder.NAME;
import static org.opensearch.neuralsearch.query.sparse.SparseEncodingQueryBuilder.QUERY_TEXT_FIELD;
import static org.opensearch.neuralsearch.query.sparse.SparseEncodingQueryBuilder.QUERY_TOKENS_FIELD;
import static org.opensearch.neuralsearch.query.sparse.SparseEncodingQueryBuilder.TOKEN_SCORE_UPPER_BOUND_FIELD;

import lombok.SneakyThrows;
import org.opensearch.client.Client;
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
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class SparseEncodingQueryBuilderTests extends OpenSearchTestCase {

    private static final String FIELD_NAME = "testField";
    private static final String QUERY_TEXT = "Hello world!";
    private static final Map<String,Float> QUERY_TOKENS = Map.of("hello", 1.f, "world", 2.f);
    private static final String MODEL_ID = "mfgfgdsfgfdgsde";
    private static final Float TOKEN_SCORE_UPPER_BOUND = 123f;
    private static final float BOOST = 1.8f;
    private static final String QUERY_NAME = "queryName";
    private static final Supplier<Map<String, Float>> QUERY_TOKENS_SUPPLIER = () -> Map.of("hello", 1.f, "world", 2.f);

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
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder = SparseEncodingQueryBuilder.fromXContent(contentParser);

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
                    "string":float,
                }
              }
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(FIELD_NAME)
                .field(QUERY_TOKENS_FIELD.getPreferredName(), QUERY_TOKENS)
                .endObject()
                .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder = SparseEncodingQueryBuilder.fromXContent(contentParser);

        assertEquals(FIELD_NAME, sparseEncodingQueryBuilder.fieldName());
        assertEquals(QUERY_TOKENS, sparseEncodingQueryBuilder.queryTokens());
    }

    @SneakyThrows
    public void testFromXContent_whenBuiltWithOptionals_thenBuildSuccessfully() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "model_id": "string",
                "token_score_upper_bound":123.0,
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
                .field(TOKEN_SCORE_UPPER_BOUND_FIELD.getPreferredName(), TOKEN_SCORE_UPPER_BOUND)
                .field(BOOST_FIELD.getPreferredName(), BOOST)
                .field(NAME_FIELD.getPreferredName(), QUERY_NAME)
                .endObject()
                .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder = SparseEncodingQueryBuilder.fromXContent(contentParser);

        assertEquals(FIELD_NAME, sparseEncodingQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, sparseEncodingQueryBuilder.queryText());
        assertEquals(MODEL_ID, sparseEncodingQueryBuilder.modelId());
        assertEquals(TOKEN_SCORE_UPPER_BOUND, sparseEncodingQueryBuilder.tokenScoreUpperBound());
        assertEquals(BOOST, sparseEncodingQueryBuilder.boost(), 0.0);
        assertEquals(QUERY_NAME, sparseEncodingQueryBuilder.queryName());
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
        expectThrows(ParsingException.class, () -> SparseEncodingQueryBuilder.fromXContent(contentParser));
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
        expectThrows(IllegalArgumentException.class, () -> SparseEncodingQueryBuilder.fromXContent(contentParser));
    }

    @SneakyThrows
    public void testFromXContent_whenBuildWithMissingModelId_thenFail() {
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
        expectThrows(IllegalArgumentException.class, () -> SparseEncodingQueryBuilder.fromXContent(contentParser));
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
        expectThrows(IOException.class, () -> SparseEncodingQueryBuilder.fromXContent(contentParser));
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    public void testToXContent() {
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder = new SparseEncodingQueryBuilder().fieldName(FIELD_NAME)
                .modelId(MODEL_ID)
                .queryText(QUERY_TEXT)
                .queryTokens(QUERY_TOKENS)
                .tokenScoreUpperBound(TOKEN_SCORE_UPPER_BOUND);

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
        // QUERY_TOKENS is <String,Float> map, the converted one use <String, Double>
        Map<String, Double> convertedQueryTokensMap = (Map<String, Double>) secondInnerMap.get(QUERY_TOKENS_FIELD.getPreferredName());
        assertEquals(QUERY_TOKENS.size(), convertedQueryTokensMap.size());
        for (Map.Entry<String, Float> entry: QUERY_TOKENS.entrySet()) {
            assertEquals(entry.getValue(), convertedQueryTokensMap.get(entry.getKey()).floatValue(), 0);
        }
        assertEquals(
                TOKEN_SCORE_UPPER_BOUND,
                ((Double) secondInnerMap.get(TOKEN_SCORE_UPPER_BOUND_FIELD.getPreferredName())).floatValue(),
                0
        );
    }

    @SneakyThrows
    public void testStreams() {
        SparseEncodingQueryBuilder original = new SparseEncodingQueryBuilder();
        original.fieldName(FIELD_NAME);
        original.queryText(QUERY_TEXT);
        original.modelId(MODEL_ID);
        original.queryTokens(QUERY_TOKENS);
        original.tokenScoreUpperBound(TOKEN_SCORE_UPPER_BOUND);
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

        SparseEncodingQueryBuilder copy = new SparseEncodingQueryBuilder(filterStreamInput);
        assertEquals(original, copy);
    }

    public void testHashAndEquals() {
        String fieldName1 = "field 1";
        String fieldName2 = "field 2";
        String queryText1 = "query text 1";
        String queryText2 = "query text 2";
        String modelId1 = "model-1";
        String modelId2 = "model-2";
        float boost1 = 1.8f;
        float boost2 = 3.8f;
        String queryName1 = "query-1";
        String queryName2 = "query-2";
        Map<String, Float> queryTokens1 = Map.of("hello", 1f);
        Map<String, Float> queryTokens2 = Map.of("hello", 2f);
        float tokenScoreUpperBound1 = 1f;
        float tokenScoreUpperBound2 = 2f;

        SparseEncodingQueryBuilder sparseEncodingQueryBuilder_baseline = new SparseEncodingQueryBuilder()
                .fieldName(fieldName1)
                .queryText(queryText1)
                .queryTokens(queryTokens1)
                .modelId(modelId1)
                .boost(boost1)
                .queryName(queryName1)
                .tokenScoreUpperBound(tokenScoreUpperBound1);

        // Identical to sparseEncodingQueryBuilder_baseline
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder_baselineCopy = new SparseEncodingQueryBuilder()
                .fieldName(fieldName1)
                .queryText(queryText1)
                .queryTokens(queryTokens1)
                .modelId(modelId1)
                .boost(boost1)
                .queryName(queryName1)
                .tokenScoreUpperBound(tokenScoreUpperBound1);

        // Identical to sparseEncodingQueryBuilder_baseline except default boost and query name
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder_defaultBoostAndQueryName = new SparseEncodingQueryBuilder()
                .fieldName(fieldName1)
                .queryText(queryText1)
                .queryTokens(queryTokens1)
                .modelId(modelId1)
                .tokenScoreUpperBound(tokenScoreUpperBound1);

        // Identical to sparseEncodingQueryBuilder_baseline except diff field name
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder_diffFieldName = new SparseEncodingQueryBuilder()
                .fieldName(fieldName2)
                .queryText(queryText1)
                .queryTokens(queryTokens1)
                .modelId(modelId1)
                .boost(boost1)
                .queryName(queryName1)
                .tokenScoreUpperBound(tokenScoreUpperBound1);

        // Identical to sparseEncodingQueryBuilder_baseline except diff query text
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder_diffQueryText = new SparseEncodingQueryBuilder()
                .fieldName(fieldName1)
                .queryText(queryText2)
                .queryTokens(queryTokens1)
                .modelId(modelId1)
                .boost(boost1)
                .queryName(queryName1)
                .tokenScoreUpperBound(tokenScoreUpperBound1);

        // Identical to sparseEncodingQueryBuilder_baseline except diff model ID
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder_diffModelId = new SparseEncodingQueryBuilder()
                .fieldName(fieldName1)
                .queryText(queryText1)
                .queryTokens(queryTokens1)
                .modelId(modelId2)
                .boost(boost1)
                .queryName(queryName1)
                .tokenScoreUpperBound(tokenScoreUpperBound1);

        // Identical to sparseEncodingQueryBuilder_baseline except diff query tokens
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder_diffQueryTokens = new SparseEncodingQueryBuilder()
                .fieldName(fieldName1)
                .queryText(queryText1)
                .queryTokens(queryTokens2)
                .modelId(modelId1)
                .boost(boost1)
                .queryName(queryName1)
                .tokenScoreUpperBound(tokenScoreUpperBound1);

        // Identical to sparseEncodingQueryBuilder_baseline except diff boost
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder_diffBoost = new SparseEncodingQueryBuilder()
                .fieldName(fieldName1)
                .queryText(queryText1)
                .queryTokens(queryTokens1)
                .modelId(modelId1)
                .boost(boost2)
                .queryName(queryName1)
                .tokenScoreUpperBound(tokenScoreUpperBound1);

        // Identical to sparseEncodingQueryBuilder_baseline except diff query name
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder_diffQueryName = new SparseEncodingQueryBuilder()
                .fieldName(fieldName1)
                .queryText(queryText1)
                .queryTokens(queryTokens1)
                .modelId(modelId1)
                .boost(boost1)
                .queryName(queryName2)
                .tokenScoreUpperBound(tokenScoreUpperBound1);

        // Identical to sparseEncodingQueryBuilder_baseline except diff token_score_upper_bound
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder_diffTokenScoreUpperBound = new SparseEncodingQueryBuilder()
                .fieldName(fieldName1)
                .queryText(queryText1)
                .queryTokens(queryTokens1)
                .modelId(modelId1)
                .boost(boost1)
                .queryName(queryName1)
                .tokenScoreUpperBound(tokenScoreUpperBound2);

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

        assertNotEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_diffQueryTokens);
        assertNotEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_diffQueryTokens.hashCode());

        assertNotEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_diffBoost);
        assertNotEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_diffBoost.hashCode());

        assertNotEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_diffQueryName);
        assertNotEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_diffQueryName.hashCode());

        assertNotEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_diffTokenScoreUpperBound);
        assertNotEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_diffTokenScoreUpperBound.hashCode());
    }

    @SneakyThrows
    public void testRewrite_whenQueryTokensNotNull_thenRewriteToSelf() {
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder = new SparseEncodingQueryBuilder()
                .queryTokens(QUERY_TOKENS)
                .fieldName(FIELD_NAME)
                .queryText(QUERY_TEXT)
                .modelId(MODEL_ID);
        QueryBuilder queryBuilder = sparseEncodingQueryBuilder.doRewrite(null);
        assert queryBuilder == sparseEncodingQueryBuilder;
    }

    @SneakyThrows
    public void testRewrite_whenQueryTokensSupplierNull_thenSetQueryTokensSupplier() {
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder = new SparseEncodingQueryBuilder()
                .fieldName(FIELD_NAME)
                .queryText(QUERY_TEXT)
                .modelId(MODEL_ID);
        Map<String, Float> expectedMap = Map.of("1", 1f, "2", 2f);
        MLCommonsClientAccessor mlCommonsClientAccessor = mock(MLCommonsClientAccessor.class);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(List.of(Map.of("response", List.of(expectedMap))));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentencesWithMapResult(any(), any(), any());
        SparseEncodingQueryBuilder.initialize(mlCommonsClientAccessor);

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

        SparseEncodingQueryBuilder queryBuilder = (SparseEncodingQueryBuilder) sparseEncodingQueryBuilder.doRewrite(queryRewriteContext);
        assertNotNull(queryBuilder.queryTokensSupplier());
        assertTrue(inProgressLatch.await(5, TimeUnit.SECONDS));
        assertEquals(expectedMap, queryBuilder.queryTokensSupplier().get());
    }

    @SneakyThrows
    public void testRewrite_whenSupplierContentNull_thenReturnCopy() {
        Supplier<Map<String, Float>> nullSupplier = () -> null;
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder = new SparseEncodingQueryBuilder().fieldName(FIELD_NAME)
                .queryText(QUERY_TEXT)
                .modelId(MODEL_ID)
                .queryTokensSupplier(nullSupplier);
        QueryBuilder queryBuilder = sparseEncodingQueryBuilder.doRewrite(null);
        assertEquals(sparseEncodingQueryBuilder, queryBuilder);
    }

    @SneakyThrows
    public void testRewrite_whenQueryTokensSupplierSet_thenSetQueryTokens() {
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder = new SparseEncodingQueryBuilder()
                .fieldName(FIELD_NAME)
                .queryText(QUERY_TEXT)
                .modelId(MODEL_ID)
                .queryTokensSupplier(QUERY_TOKENS_SUPPLIER);
        QueryBuilder queryBuilder = sparseEncodingQueryBuilder.doRewrite(null);
        SparseEncodingQueryBuilder targetQueryBuilder = new SparseEncodingQueryBuilder()
                .fieldName(FIELD_NAME)
                .queryText(QUERY_TEXT)
                .modelId(MODEL_ID)
                .queryTokens(QUERY_TOKENS_SUPPLIER.get());
        assertEquals(queryBuilder, targetQueryBuilder);
    }
}
