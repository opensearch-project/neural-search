/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;
import static org.opensearch.index.query.AbstractQueryBuilder.BOOST_FIELD;
import static org.opensearch.index.query.AbstractQueryBuilder.NAME_FIELD;
import static org.opensearch.neuralsearch.util.TestUtils.xContentBuilderToMap;
import static org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder.MAX_TOKEN_SCORE_FIELD;
import static org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder.MODEL_ID_FIELD;
import static org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder.NAME;
import static org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder.QUERY_TEXT_FIELD;
import static org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder.QUERY_TOKENS_FIELD;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SetOnce;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.FilterStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.test.OpenSearchTestCase;

import lombok.SneakyThrows;

public class NeuralSparseQueryBuilderTests extends OpenSearchTestCase {

    private static final String TWO_PHASE_ENABLED_SETTING_KEY = "index.neural_sparse.two_phase.default_enabled";
    private static final String TWO_PHASE_WINDOW_SIZE_EXPANSION_SETTING_KEY = "index.neural_sparse.two_phase.default_window_size_expansion";
    private static final String TWO_PHASE_PRUNE_RATIO_SETTING_KEY = "index.neural_sparse.two_phase.default_pruning_ratio";
    private static final String FIELD_NAME = "testField";
    private static final String QUERY_TEXT = "Hello world!";
    private static final String MODEL_ID = "mfgfgdsfgfdgsde";
    private static final float BOOST = 1.8f;
    private static final String QUERY_NAME = "queryName";
    private static final Float MAX_TOKEN_SCORE = 123f;
    private static final Supplier<Map<String, Float>> QUERY_TOKENS_SUPPLIER = () -> Map.of("hello", 1.f, "world", 2.f);

    private final QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);

    @Before
    public void setUpNeuralSparseTwoPhaseParameters() {
        ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);
        when(clusterService.state().getNodes().getMinNodeVersion()).thenReturn(Version.CURRENT);
        when(clusterService.state().getNodes().getMaxNodeVersion()).thenReturn(Version.CURRENT);

        NeuralSearchClusterUtil.instance().initialize(clusterService);
        // initialize mockQueryShardContext
        MappedFieldType mappedFieldType = mock(MappedFieldType.class);
        when(mappedFieldType.typeName()).thenReturn("rank_features");
        when(mockQueryShardContext.fieldMapper(anyString())).thenReturn(mappedFieldType);

        IndexMetadata indexMetadata = IndexMetadata.builder("_index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .creationDate(System.currentTimeMillis())
            .build();

        IndexSettings indexSettings = new IndexSettings(indexMetadata, getDefaultTwoPhaseParameterSettings());
        when(mockQueryShardContext.getIndexSettings()).thenReturn(indexSettings);
    }

    @Before
    public void setupClusterServiceToCurrentVersion() {
        setUpClusterService(Version.CURRENT);
        NeuralSparseQueryBuilder.initialize(mock(MLCommonsClientAccessor.class));
    }

    private Settings getDefaultTwoPhaseParameterSettings() {
        return Settings.builder()
            .put(TWO_PHASE_ENABLED_SETTING_KEY, true)
            .put(TWO_PHASE_WINDOW_SIZE_EXPANSION_SETTING_KEY, 5.0f)
            .put(TWO_PHASE_PRUNE_RATIO_SETTING_KEY, 0.4f)
            .build();
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
        assertNull(sparseEncodingQueryBuilder.neuralSparseTwoPhaseParameters());
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
        assertNull(sparseEncodingQueryBuilder.neuralSparseTwoPhaseParameters());
    }

    @SneakyThrows
    public void testFromXContent_whenBuiltWithTwoPhaseParams_thenBuildSuccessfully() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "model_id": "string",
                "two_phase_settings":{
                      "window_size_expansion": 5,
                      "pruning_ratio": 0.4,
                      "enabled": false
                  }
              }
          }
        */
        NeuralSparseTwoPhaseParameters parameters = new NeuralSparseTwoPhaseParameters().enabled(false)
            .pruning_ratio(0.5f)
            .window_size_expansion(2f);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID);
        parameters.doXContent(xContentBuilder);
        xContentBuilder.endObject().endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = NeuralSparseQueryBuilder.fromXContent(contentParser);

        assertEquals(FIELD_NAME, sparseEncodingQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, sparseEncodingQueryBuilder.queryText());
        assertEquals(MODEL_ID, sparseEncodingQueryBuilder.modelId());
        assertEquals(parameters, sparseEncodingQueryBuilder.neuralSparseTwoPhaseParameters());
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
        assertNull(sparseEncodingQueryBuilder.neuralSparseTwoPhaseParameters());
    }

    @SneakyThrows
    public void testFromXContent_whenBuiltWithIllegalTwoPhaseWindowSizeExpansion_thenBuildSuccessfully() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "model_id": "string",
                "two_phase_settings":{
                      "window_size_expansion": 0.4,
                      "pruning_ratio": 0.4,
                      "enabled": true
                 }
              }
          }
        */
        NeuralSparseTwoPhaseParameters parameters = new NeuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameterSettings())
            .window_size_expansion(0.4f);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID);
        parameters.doXContent(xContentBuilder);
        xContentBuilder.endObject().endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        expectThrows(IllegalArgumentException.class, () -> NeuralSparseQueryBuilder.fromXContent(contentParser));
    }

    @SneakyThrows
    public void testFromXContent_whenBuiltWithIllegalTwoPhasePruningRate_thenBuildSuccessfully() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "model_id": "string",
                "two_phase_settings":{
                      "window_size_expansion": 0.5,
                      "pruning_ratio": -0.001, // or 1.001
                      "enabled": true
                  }
              }
          }
        */
        NeuralSparseTwoPhaseParameters parameters = new NeuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameterSettings()).pruning_ratio(
            -0.001f
        );
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID);
        parameters.doXContent(xContentBuilder);
        xContentBuilder.endObject().endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        expectThrows(IllegalArgumentException.class, () -> NeuralSparseQueryBuilder.fromXContent(contentParser));

        parameters.pruning_ratio(1.001f);
        XContentBuilder xContentBuilder2 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID);
        parameters.doXContent(xContentBuilder2);
        xContentBuilder2.endObject().endObject();

        XContentParser contentParser2 = createParser(xContentBuilder2);
        contentParser2.nextToken();
        expectThrows(IllegalArgumentException.class, () -> NeuralSparseQueryBuilder.fromXContent(contentParser2));
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

    @SneakyThrows
    public void testFromXContent_whenBuiltWithEmptyTwoPhaseParams_thenThrowException() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "model_id": "string",
                "two_phase_settings":{
                      "window_size_expansion": null,
                      "pruning_ratio": null,
                      "enabled": null
                  }
              }
          }
        */
        NeuralSparseTwoPhaseParameters parameters = new NeuralSparseTwoPhaseParameters().enabled(null)
            .pruning_ratio(null)
            .window_size_expansion(null);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID);
        parameters.doXContent(xContentBuilder);
        xContentBuilder.endObject().endObject();
        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        expectThrows(ParsingException.class, () -> NeuralSparseQueryBuilder.fromXContent(contentParser));
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    public void testToXContentWithFullField() {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(FIELD_NAME)
            .modelId(MODEL_ID)
            .queryText(QUERY_TEXT)
            .maxTokenScore(MAX_TOKEN_SCORE)
            .queryTokensSupplier(QUERY_TOKENS_SUPPLIER)
            .neuralSparseTwoPhaseParameters(new NeuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameterSettings()));

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
        assertEquals(6, secondInnerMap.size());

        assertEquals(MODEL_ID, secondInnerMap.get(MODEL_ID_FIELD.getPreferredName()));
        assertEquals(QUERY_TEXT, secondInnerMap.get(QUERY_TEXT_FIELD.getPreferredName()));
        assertEquals(MAX_TOKEN_SCORE, (Double) secondInnerMap.get(MAX_TOKEN_SCORE_FIELD.getPreferredName()), 0.0);
        Map<String, Double> parsedQueryTokens = (Map<String, Double>) secondInnerMap.get(QUERY_TOKENS_FIELD.getPreferredName());
        assertEquals(QUERY_TOKENS_SUPPLIER.get().keySet(), parsedQueryTokens.keySet());
        for (Map.Entry<String, Float> entry : QUERY_TOKENS_SUPPLIER.get().entrySet()) {
            assertEquals(entry.getValue(), parsedQueryTokens.get(entry.getKey()).floatValue(), 0);
        }

        Map<String, Object> thirdInnerMap = (Map<String, Object>) secondInnerMap.get(
            NeuralSparseTwoPhaseParameters.NAME.getPreferredName()
        );
        assertEquals(5.0f, (Double) thirdInnerMap.get(NeuralSparseTwoPhaseParameters.WINDOW_SIZE_EXPANSION.getPreferredName()), 1e-6);

        assertEquals(0.4f, (Double) thirdInnerMap.get(NeuralSparseTwoPhaseParameters.PRUNING_RATIO.getPreferredName()), 1e-6);

        assertEquals(true, thirdInnerMap.get(NeuralSparseTwoPhaseParameters.ENABLED.getPreferredName()));
    }

    @SneakyThrows
    public void testToXContentWithNullableField() {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(FIELD_NAME)
            .modelId(null)
            .queryText(null)
            .maxTokenScore(null)
            .queryTokensSupplier(null)
            .neuralSparseTwoPhaseParameters(null);

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
        assertEquals(1, secondInnerMap.size());
    }

    public void testStreams_whenCurrentVersion_thenSuccess() {
        setUpClusterService(Version.CURRENT);
        testStreams();
        testStreamsWithQueryTokensOnly();
    }

    public void testStreams_whenMinVersionIsBeforeDefaultModelId_thenSuccess() {
        setUpClusterService(Version.V_2_12_0);
        testStreams();
        testStreamsWithQueryTokensOnly();
        testStreamsWithTwoPhaseParams();
    }

    @SneakyThrows
    private void testStreams() {
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

    @SneakyThrows
    private void testStreamsWithTwoPhaseParams() {
        NeuralSparseQueryBuilder original = new NeuralSparseQueryBuilder();
        original.fieldName(FIELD_NAME);
        original.queryText(QUERY_TEXT);
        original.modelId(MODEL_ID);
        original.boost(BOOST);
        original.queryName(QUERY_NAME);
        original.neuralSparseTwoPhaseParameters(new NeuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameterSettings()));

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        original.writeTo(streamOutput);

        FilterStreamInput filterStreamInput = new NamedWriteableAwareStreamInput(
            streamOutput.bytes().streamInput(),
            new NamedWriteableRegistry(
                List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new))
            )
        );

        NeuralSparseQueryBuilder copy = new NeuralSparseQueryBuilder(filterStreamInput);
        if (NeuralSparseTwoPhaseParameters.isClusterOnSameVersionForTwoPhaseSearchSupport()) {
            assertEquals(original, copy);
        } else {
            assertNull(copy.neuralSparseTwoPhaseParameters());
            original.neuralSparseTwoPhaseParameters(null);
            assertEquals(original, copy);
        }

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
        NeuralSparseTwoPhaseParameters parameters1 = new NeuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameterSettings());
        NeuralSparseTwoPhaseParameters parameters2 = NeuralSparseTwoPhaseParametersTests.TWO_PHASE_PARAMETERS;

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

        // Identical to sparseEncodingQueryBuilder_baseline except non-null neuralSparseTwoPhaseParameters
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder_nonNullTwoPhaseParameters = new NeuralSparseQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .boost(boost1)
            .queryName(queryName1)
            .neuralSparseTwoPhaseParameters(parameters1);

        // Identical to sparseEncodingQueryBuilder_baseline except non-null neuralSparseTwoPhaseParameters
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder_diffTwoPhaseParameters = new NeuralSparseQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .boost(boost1)
            .queryName(queryName1)
            .neuralSparseTwoPhaseParameters(parameters2);

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

        assertNotEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_nonNullTwoPhaseParameters);
        assertNotEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_nonNullTwoPhaseParameters.hashCode());

        assertNotEquals(sparseEncodingQueryBuilder_nonNullTwoPhaseParameters, sparseEncodingQueryBuilder_diffTwoPhaseParameters);
        assertNotEquals(
            sparseEncodingQueryBuilder_nonNullTwoPhaseParameters.hashCode(),
            sparseEncodingQueryBuilder_diffTwoPhaseParameters.hashCode()
        );

        assertNotEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_nullQueryText);
        assertNotEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_nullQueryText.hashCode());

        assertNotEquals(sparseEncodingQueryBuilder_baseline, sparseEncodingQueryBuilder_nullModelId);
        assertNotEquals(sparseEncodingQueryBuilder_baseline.hashCode(), sparseEncodingQueryBuilder_nullModelId.hashCode());
    }

    @SneakyThrows
    public void testRewrite_whenQueryTokensSupplierNull_thenSetQueryTokensSupplier() {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID);
        Map<String, Float> expectedMap = Map.of("1", 1f, "2", 2f);
        MLCommonsClientAccessor mlCommonsClientAccessor = mock(MLCommonsClientAccessor.class);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(List.of(Map.of("response", List.of(expectedMap))));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentencesWithMapResult(any(), any(), any());
        NeuralSparseQueryBuilder.initialize(mlCommonsClientAccessor);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        QueryRewriteContext queryRewriteContext = mock(QueryRewriteContext.class, RETURNS_DEEP_STUBS);
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

        when(queryRewriteContext.convertToShardContext()).thenReturn(mockQueryShardContext);
        NeuralSparseQueryBuilder queryBuilder = (NeuralSparseQueryBuilder) sparseEncodingQueryBuilder.doRewrite(queryRewriteContext);
        assertNotNull(queryBuilder.queryTokensSupplier());
        assertTrue(inProgressLatch.await(5, TimeUnit.SECONDS));
        assertEquals(expectedMap, queryBuilder.queryTokensSupplier().get());
    }

    @SneakyThrows
    public void testRewrite_whenQueryTokensSupplierSet_thenReturnSelf() {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .queryTokensSupplier(QUERY_TOKENS_SUPPLIER);

        QueryRewriteContext queryRewriteContext = mock(QueryRewriteContext.class);
        when(queryRewriteContext.convertToShardContext()).thenReturn(mockQueryShardContext);
        QueryBuilder queryBuilder = sparseEncodingQueryBuilder.doRewrite(queryRewriteContext);
        assertSame(queryBuilder, sparseEncodingQueryBuilder);

        sparseEncodingQueryBuilder.queryTokensSupplier(() -> null);
        queryBuilder = sparseEncodingQueryBuilder.doRewrite(queryRewriteContext);
        assertSame(queryBuilder, sparseEncodingQueryBuilder);
    }

    private void setUpClusterService(Version version) {
        ClusterService clusterService = NeuralSearchClusterTestUtils.mockClusterService(version);
        NeuralSearchClusterUtil.instance().initialize(clusterService);
    }

    @SneakyThrows
    public void testTokenDividedByScores_whenDefaultSettings() {
        Map<String, Float> map = new HashMap<>();
        for (int i = 1; i < 11; i++) {
            map.put(String.valueOf(i), (float) i);
        }
        final Supplier<Map<String, Float>> tokenSupplier = () -> map;
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName("rank_features")
            .queryText(QUERY_TEXT)
            .neuralSparseTwoPhaseParameters(new NeuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameterSettings()))
            .modelId(MODEL_ID)
            .queryTokensSupplier(tokenSupplier);

        NeuralSparseQuery neuralSparseQuery = (NeuralSparseQuery) sparseEncodingQueryBuilder.doToQuery(mockQueryShardContext);
        BooleanQuery highScoreTokenQuery = (BooleanQuery) neuralSparseQuery.getHighScoreTokenQuery();
        BooleanQuery lowScoreTokenQuery = (BooleanQuery) neuralSparseQuery.getLowScoreTokenQuery();
        assertEquals(highScoreTokenQuery.clauses().size(), 7);
        assertEquals(lowScoreTokenQuery.clauses().size(), 3);
        sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName("rank_features")
            .queryText(QUERY_TEXT)
            .neuralSparseTwoPhaseParameters(
                new NeuralSparseTwoPhaseParameters().enabled(true).window_size_expansion(5f).pruning_ratio(0.6f)
            )
            .modelId(MODEL_ID)
            .queryTokensSupplier(tokenSupplier);
        neuralSparseQuery = (NeuralSparseQuery) sparseEncodingQueryBuilder.doToQuery(mockQueryShardContext);
        highScoreTokenQuery = (BooleanQuery) neuralSparseQuery.getHighScoreTokenQuery();
        lowScoreTokenQuery = (BooleanQuery) neuralSparseQuery.getLowScoreTokenQuery();
        assertEquals(highScoreTokenQuery.clauses().size(), 5);
        assertEquals(lowScoreTokenQuery.clauses().size(), 5);
    }

    @SneakyThrows
    public void testDoToQuery_whenTwoPhaseParaDisabled_thenDegradeSuccess() {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .queryTokensSupplier(QUERY_TOKENS_SUPPLIER)
            .neuralSparseTwoPhaseParameters(
                new NeuralSparseTwoPhaseParameters().enabled(false).pruning_ratio(0.7f).window_size_expansion(6.0f)
            );
        Query query = sparseEncodingQueryBuilder.doToQuery(mockQueryShardContext);
        assertTrue(query instanceof BooleanQuery);
        List<BooleanClause> booleanClauseList = ((BooleanQuery) query).clauses();
        assertEquals(2, ((BooleanQuery) query).clauses().size());
        List<Query> actualQueries = booleanClauseList.stream().map(BooleanClause::getQuery).collect(Collectors.toList());
        Query expectedQuery1 = FeatureField.newLinearQuery(FIELD_NAME, "world", 2.f);
        Query expectedQuery2 = FeatureField.newLinearQuery(FIELD_NAME, "hello", 1.f);
        assertTrue("Expected query for 'world' not found", actualQueries.contains(expectedQuery1));
        assertTrue("Expected query for 'hello' not found", actualQueries.contains(expectedQuery2));
    }

    @SneakyThrows
    public void testDoToQuery_whenTwoPhaseParaEmpty_thenDegradeSuccess() {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .queryTokensSupplier(QUERY_TOKENS_SUPPLIER);
        Query query = sparseEncodingQueryBuilder.doToQuery(mockQueryShardContext);
        assertTrue(query instanceof BooleanQuery);
        List<BooleanClause> booleanClauseList = ((BooleanQuery) query).clauses();
        assertEquals(2, ((BooleanQuery) query).clauses().size());
        List<Query> actualQueries = booleanClauseList.stream().map(BooleanClause::getQuery).collect(Collectors.toList());
        Query expectedQuery1 = FeatureField.newLinearQuery(FIELD_NAME, "world", 2.f);
        Query expectedQuery2 = FeatureField.newLinearQuery(FIELD_NAME, "hello", 1.f);
        assertTrue("Expected query for 'world' not found", actualQueries.contains(expectedQuery1));
        assertTrue("Expected query for 'hello' not found", actualQueries.contains(expectedQuery2));
    }

    @SneakyThrows
    public void testDoToQuery_whenTwoPhaseEnabled_thenBuildCorrectQuery() {
        Map<String, Float> map = new HashMap<>();
        for (int i = 1; i < 3; i++) {
            map.put(String.valueOf(i), (float) i);
        }
        final Supplier<Map<String, Float>> tokenSupplier = () -> map;
        // token with score [1.0,2.0] will build degrade to allTokenQuery
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .queryTokensSupplier(tokenSupplier)
            .neuralSparseTwoPhaseParameters(new NeuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameterSettings()));
        Query allTokenQuery = sparseEncodingQueryBuilder.doToQuery(mockQueryShardContext);
        assertTrue(allTokenQuery instanceof BooleanQuery);
        assertEquals(((BooleanQuery) allTokenQuery).clauses().size(), 2);
        map.put("Temp", 9.f);
        // token with score [1.0,2.0,9.0] will build a NeuralSparseQuery whose lowTokenQuery including [1.0,2.0]
        Query query = sparseEncodingQueryBuilder.doToQuery(mockQueryShardContext);
        assertTrue(query instanceof NeuralSparseQuery);
        assertEquals(((NeuralSparseQuery) query).getLowScoreTokenQuery(), allTokenQuery);
    }

    @SneakyThrows
    public void testDoToQuery_successfulDoToQuery() {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(FIELD_NAME)
            .maxTokenScore(MAX_TOKEN_SCORE)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .queryTokensSupplier(QUERY_TOKENS_SUPPLIER);

        BooleanQuery.Builder targetQueryBuilder = new BooleanQuery.Builder();
        targetQueryBuilder.add(FeatureField.newLinearQuery(FIELD_NAME, "hello", 1.f), BooleanClause.Occur.SHOULD);
        targetQueryBuilder.add(FeatureField.newLinearQuery(FIELD_NAME, "world", 2.f), BooleanClause.Occur.SHOULD);

        assertEquals(sparseEncodingQueryBuilder.doToQuery(mockQueryShardContext), targetQueryBuilder.build());
    }
}
