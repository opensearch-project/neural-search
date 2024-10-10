/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.index.query.AbstractQueryBuilder.BOOST_FIELD;
import static org.opensearch.index.query.AbstractQueryBuilder.NAME_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.FILTER_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.MAX_DISTANCE_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.MIN_SCORE_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.RESCORE_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.RESCORE_OVERSAMPLE_FIELD;
import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_FLOATS_ASSERTION;
import static org.opensearch.neuralsearch.util.TestUtils.xContentBuilderToMap;
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.K_FIELD;
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.MODEL_ID_FIELD;
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.NAME;
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.QUERY_IMAGE_FIELD;
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.QUERY_TEXT_FIELD;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.opensearch.Version;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.FilterStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.knn.index.query.KNNQueryBuilder;
import org.opensearch.knn.index.query.rescore.RescoreContext;
import org.opensearch.neuralsearch.common.VectorUtil;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.test.OpenSearchTestCase;

import lombok.SneakyThrows;

public class NeuralQueryBuilderTests extends OpenSearchTestCase {

    private static final String FIELD_NAME = "testField";
    private static final String QUERY_TEXT = "Hello world!";
    private static final String IMAGE_TEXT = "base641234567890";
    private static final String MODEL_ID = "mfgfgdsfgfdgsde";
    private static final Integer K = 10;
    private static final Float MAX_DISTANCE = 1.0f;
    private static final Float MIN_SCORE = 0.985f;
    private static final float BOOST = 1.8f;
    private static final String QUERY_NAME = "queryName";
    private static final Supplier<float[]> TEST_VECTOR_SUPPLIER = () -> new float[10];

    private static final QueryBuilder TEST_FILTER = new MatchAllQueryBuilder();

    @SneakyThrows
    public void testFromXContent_whenBuiltWithDefaults_thenBuildSuccessfully() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "query_image": "string",
                "model_id": "string",
                "k": int
              }
          }
        */
        setUpClusterService(Version.V_2_10_0);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(K_FIELD.getPreferredName(), K)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.fromXContent(contentParser);

        assertEquals(FIELD_NAME, neuralQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, neuralQueryBuilder.queryText());
        assertEquals(MODEL_ID, neuralQueryBuilder.modelId());
        assertEquals(K, neuralQueryBuilder.k());
    }

    @SneakyThrows
    public void testFromXContent_withMethodParameters_thenBuildSuccessfully() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "query_image": "string",
                "model_id": "string",
                "k": int
              }
          }
        */
        setUpClusterService(Version.V_2_10_0);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(K_FIELD.getPreferredName(), K)
            .startObject("method_parameters")
            .field("ef_search", 1000)
            .endObject()
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.fromXContent(contentParser);

        assertEquals(Map.of("ef_search", 1000), neuralQueryBuilder.methodParameters());
        assertEquals(FIELD_NAME, neuralQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, neuralQueryBuilder.queryText());
        assertEquals(MODEL_ID, neuralQueryBuilder.modelId());
        assertEquals(K, neuralQueryBuilder.k());
        assertNull(neuralQueryBuilder.rescoreContext());
    }

    @SneakyThrows
    public void testFromXContent_withRescoreContext_thenBuildSuccessfully() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "query_image": "string",
                "model_id": "string",
                "k": int,
                "rescore": {
                    "oversample_factor" : int
                }
              }
          }
        */
        setUpClusterService(Version.V_2_10_0);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(K_FIELD.getPreferredName(), K)
            .startObject(RESCORE_FIELD.getPreferredName())
            .field(RESCORE_OVERSAMPLE_FIELD.getPreferredName(), 1)
            .endObject()
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.fromXContent(contentParser);

        assertEquals(FIELD_NAME, neuralQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, neuralQueryBuilder.queryText());
        assertEquals(MODEL_ID, neuralQueryBuilder.modelId());
        assertEquals(K, neuralQueryBuilder.k());
        assertEquals(
            RescoreContext.getDefault().getOversampleFactor(),
            neuralQueryBuilder.rescoreContext().getOversampleFactor(),
            DELTA_FOR_FLOATS_ASSERTION
        );
        assertNull(neuralQueryBuilder.methodParameters());
    }

    @SneakyThrows
    public void testFromXContent_whenBuiltWithOptionals_thenBuildSuccessfully() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "model_id": "string",
                "k": int,
                "boost": 10.0,
                "_name": "something",
              }
          }
        */
        setUpClusterService(Version.CURRENT);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(QUERY_IMAGE_FIELD.getPreferredName(), IMAGE_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(K_FIELD.getPreferredName(), K)
            .field(BOOST_FIELD.getPreferredName(), BOOST)
            .field(NAME_FIELD.getPreferredName(), QUERY_NAME)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.fromXContent(contentParser);

        assertEquals(FIELD_NAME, neuralQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, neuralQueryBuilder.queryText());
        assertEquals(IMAGE_TEXT, neuralQueryBuilder.queryImage());
        assertEquals(MODEL_ID, neuralQueryBuilder.modelId());
        assertEquals(K, neuralQueryBuilder.k());
        assertEquals(BOOST, neuralQueryBuilder.boost(), 0.0);
        assertEquals(QUERY_NAME, neuralQueryBuilder.queryName());
    }

    @SneakyThrows
    public void testFromXContent_whenBuiltWithFilter_thenBuildSuccessfully() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "model_id": "string",
                "k": int,
                "boost": 10.0,
                "_name": "something",
                "filter": {
                  "match_all": {}
                }
              }
          }
        */
        setUpClusterService(Version.CURRENT);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(K_FIELD.getPreferredName(), K)
            .field(BOOST_FIELD.getPreferredName(), BOOST)
            .field(NAME_FIELD.getPreferredName(), QUERY_NAME)
            .field(FILTER_FIELD.getPreferredName(), TEST_FILTER)
            .endObject()
            .endObject();

        NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(
            List.of(
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(MatchAllQueryBuilder.NAME),
                    MatchAllQueryBuilder::fromXContent
                )
            )
        );
        XContentParser contentParser = createParser(
            namedXContentRegistry,
            xContentBuilder.contentType().xContent(),
            BytesReference.bytes(xContentBuilder)
        );
        contentParser.nextToken();
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.fromXContent(contentParser);

        assertEquals(FIELD_NAME, neuralQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, neuralQueryBuilder.queryText());
        assertEquals(MODEL_ID, neuralQueryBuilder.modelId());
        assertEquals(K, neuralQueryBuilder.k());
        assertEquals(BOOST, neuralQueryBuilder.boost(), 0.0);
        assertEquals(QUERY_NAME, neuralQueryBuilder.queryName());
        assertEquals(TEST_FILTER, neuralQueryBuilder.filter());
    }

    @SneakyThrows
    public void testFromXContent_whenBuildWithMultipleRootFields_thenFail() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "model_id": "string",
                "k": int,
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
            .field(K_FIELD.getPreferredName(), K)
            .field(BOOST_FIELD.getPreferredName(), BOOST)
            .field(NAME_FIELD.getPreferredName(), QUERY_NAME)
            .endObject()
            .field("invalid", 10)
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        expectThrows(ParsingException.class, () -> NeuralQueryBuilder.fromXContent(contentParser));
    }

    @SneakyThrows
    public void testFromXContent_whenBuildWithMissingParameters_thenFail() {
        /*
          {
              "VECTOR_FIELD": {

              }
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject(FIELD_NAME).endObject().endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        expectThrows(IllegalArgumentException.class, () -> NeuralQueryBuilder.fromXContent(contentParser));
    }

    @SneakyThrows
    public void testFromXContent_whenBuildWithDuplicateParameters_thenFail() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "query_text": "string",
                "model_id": "string",
                "model_id": "string",
                "k": int,
                "k": int
              }
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(K_FIELD.getPreferredName(), K)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(K_FIELD.getPreferredName(), K)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        expectThrows(IOException.class, () -> NeuralQueryBuilder.fromXContent(contentParser));
    }

    @SneakyThrows
    public void testFromXContent_whenNoQueryField_thenFail() {
        /*
          {
              "VECTOR_FIELD": {
                "model_id": "string",
                "model_id": "string",
                "k": int,
                "k": int
              }
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(K_FIELD.getPreferredName(), K)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(K_FIELD.getPreferredName(), K)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        expectThrows(IOException.class, () -> NeuralQueryBuilder.fromXContent(contentParser));
    }

    @SneakyThrows
    public void testFromXContent_whenBuiltWithInvalidFilter_thenFail() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "model_id": "string",
                "k": int,
                "boost": 10.0,
                "filter": 12
              }
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(K_FIELD.getPreferredName(), K)
            .field(BOOST_FIELD.getPreferredName(), BOOST)
            .field(NAME_FIELD.getPreferredName(), QUERY_NAME)
            .field(FILTER_FIELD.getPreferredName(), 12)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        expectThrows(ParsingException.class, () -> NeuralQueryBuilder.fromXContent(contentParser));
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    public void testToXContent() {
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder().fieldName(FIELD_NAME)
            .modelId(MODEL_ID)
            .queryText(QUERY_TEXT)
            .k(K)
            .filter(TEST_FILTER);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder = neuralQueryBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        Map<String, Object> out = xContentBuilderToMap(builder);

        Object outer = out.get(NAME);
        if (!(outer instanceof Map)) {
            fail("neural does not map to nested object");
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
        assertEquals(K, secondInnerMap.get(K_FIELD.getPreferredName()));
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        assertEquals(
            xContentBuilderToMap(TEST_FILTER.toXContent(xContentBuilder, EMPTY_PARAMS)),
            secondInnerMap.get(FILTER_FIELD.getPreferredName())
        );
    }

    @SneakyThrows
    public void testStreams_whenClusterServiceWithDifferentVersions() {
        setUpClusterService(Version.V_2_10_0);
        testStreams();
        setUpClusterService(Version.CURRENT);
        testStreams();
    }

    @SneakyThrows
    private void testStreams() {
        NeuralQueryBuilder original = new NeuralQueryBuilder();
        original.fieldName(FIELD_NAME);
        original.queryText(QUERY_TEXT);
        original.queryImage(IMAGE_TEXT);
        original.modelId(MODEL_ID);
        original.k(K);
        original.boost(BOOST);
        original.queryName(QUERY_NAME);
        original.filter(TEST_FILTER);

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        original.writeTo(streamOutput);

        FilterStreamInput filterStreamInput = new NamedWriteableAwareStreamInput(
            streamOutput.bytes().streamInput(),
            new NamedWriteableRegistry(
                List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new))
            )
        );

        NeuralQueryBuilder copy = new NeuralQueryBuilder(filterStreamInput);
        assertEquals(original, copy);
    }

    public void testHashAndEquals() {
        String fieldName1 = "field 1";
        String fieldName2 = "field 2";
        String queryText1 = "query text 1";
        String queryText2 = "query text 2";
        String imageText1 = "query image 1";
        String modelId1 = "model-1";
        String modelId2 = "model-2";
        float boost1 = 1.8f;
        float boost2 = 3.8f;
        String queryName1 = "query-1";
        String queryName2 = "query-2";
        int k1 = 1;
        int k2 = 2;

        QueryBuilder filter1 = new MatchAllQueryBuilder();
        QueryBuilder filter2 = new MatchNoneQueryBuilder();

        NeuralQueryBuilder neuralQueryBuilder_baseline = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .queryImage(imageText1)
            .modelId(modelId1)
            .k(k1)
            .boost(boost1)
            .queryName(queryName1)
            .filter(filter1);

        // Identical to neuralQueryBuilder_baseline
        NeuralQueryBuilder neuralQueryBuilder_baselineCopy = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k1)
            .boost(boost1)
            .queryName(queryName1)
            .filter(filter1);

        // Identical to neuralQueryBuilder_baseline except default boost and query name
        NeuralQueryBuilder neuralQueryBuilder_defaultBoostAndQueryName = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k1)
            .filter(filter1);

        // Identical to neuralQueryBuilder_baseline except diff field name
        NeuralQueryBuilder neuralQueryBuilder_diffFieldName = new NeuralQueryBuilder().fieldName(fieldName2)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k1)
            .boost(boost1)
            .queryName(queryName1)
            .filter(filter1);

        // Identical to neuralQueryBuilder_baseline except diff query text
        NeuralQueryBuilder neuralQueryBuilder_diffQueryText = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText2)
            .modelId(modelId1)
            .k(k1)
            .boost(boost1)
            .queryName(queryName1)
            .filter(filter1);

        // Identical to neuralQueryBuilder_baseline except diff model ID
        NeuralQueryBuilder neuralQueryBuilder_diffModelId = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId2)
            .k(k1)
            .boost(boost1)
            .queryName(queryName1)
            .filter(filter1);

        // Identical to neuralQueryBuilder_baseline except diff k
        NeuralQueryBuilder neuralQueryBuilder_diffK = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k2)
            .boost(boost1)
            .queryName(queryName1)
            .filter(filter1);

        // Identical to neuralQueryBuilder_baseline except diff boost
        NeuralQueryBuilder neuralQueryBuilder_diffBoost = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k1)
            .boost(boost2)
            .queryName(queryName1)
            .filter(filter1);

        // Identical to neuralQueryBuilder_baseline except diff query name
        NeuralQueryBuilder neuralQueryBuilder_diffQueryName = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k1)
            .boost(boost1)
            .queryName(queryName2)
            .filter(filter1);

        // Identical to neuralQueryBuilder_baseline except no filter
        NeuralQueryBuilder neuralQueryBuilder_noFilter = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k1)
            .boost(boost1)
            .queryName(queryName2);

        // Identical to neuralQueryBuilder_baseline except no filter
        NeuralQueryBuilder neuralQueryBuilder_diffFilter = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k1)
            .boost(boost1)
            .queryName(queryName2)
            .filter(filter2);

        assertEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_baseline);
        assertEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_baseline.hashCode());

        assertEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_baselineCopy);
        assertEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_baselineCopy.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_defaultBoostAndQueryName);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_defaultBoostAndQueryName.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffFieldName);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffFieldName.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffQueryText);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffQueryText.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffModelId);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffModelId.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffK);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffK.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffBoost);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffBoost.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffQueryName);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffQueryName.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_noFilter);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_noFilter.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffFilter);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffFilter.hashCode());
    }

    @SneakyThrows
    public void testRewrite_whenVectorSupplierNull_thenSetVectorSupplier() {
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder().fieldName(FIELD_NAME).queryText(QUERY_TEXT).modelId(MODEL_ID).k(K);
        List<Float> expectedVector = Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f, 5.0f);
        MLCommonsClientAccessor mlCommonsClientAccessor = mock(MLCommonsClientAccessor.class);
        doAnswer(invocation -> {
            ActionListener<List<Float>> listener = invocation.getArgument(2);
            listener.onResponse(expectedVector);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(any(), anyMap(), any());
        NeuralQueryBuilder.initialize(mlCommonsClientAccessor);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        QueryRewriteContext queryRewriteContext = mock(QueryRewriteContext.class);
        doAnswer(invocation -> {
            BiConsumer<Client, ActionListener<?>> biConsumer = invocation.getArgument(0);
            biConsumer.accept(
                null,
                ActionListener.wrap(
                    response -> inProgressLatch.countDown(),
                    err -> fail("Failed to set vector supplier: " + err.getMessage())
                )
            );
            return null;
        }).when(queryRewriteContext).registerAsyncAction(any());

        NeuralQueryBuilder queryBuilder = (NeuralQueryBuilder) neuralQueryBuilder.doRewrite(queryRewriteContext);
        assertNotNull(queryBuilder.vectorSupplier());
        assertTrue(inProgressLatch.await(5, TimeUnit.SECONDS));
        assertArrayEquals(VectorUtil.vectorAsListToArray(expectedVector), queryBuilder.vectorSupplier().get(), 0.0f);
    }

    @SneakyThrows
    public void testRewrite_whenVectorSupplierNullAndQueryTextAndImageTextSet_thenSetVectorSupplier() {
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder().fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .queryImage(IMAGE_TEXT)
            .modelId(MODEL_ID)
            .k(K);
        List<Float> expectedVector = Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f, 5.0f);
        MLCommonsClientAccessor mlCommonsClientAccessor = mock(MLCommonsClientAccessor.class);
        doAnswer(invocation -> {
            ActionListener<List<Float>> listener = invocation.getArgument(2);
            listener.onResponse(expectedVector);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(any(), anyMap(), any());
        NeuralQueryBuilder.initialize(mlCommonsClientAccessor);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        QueryRewriteContext queryRewriteContext = mock(QueryRewriteContext.class);
        doAnswer(invocation -> {
            BiConsumer<Client, ActionListener<?>> biConsumer = invocation.getArgument(0);
            biConsumer.accept(
                null,
                ActionListener.wrap(
                    response -> inProgressLatch.countDown(),
                    err -> fail("Failed to set vector supplier: " + err.getMessage())
                )
            );
            return null;
        }).when(queryRewriteContext).registerAsyncAction(any());

        NeuralQueryBuilder queryBuilder = (NeuralQueryBuilder) neuralQueryBuilder.doRewrite(queryRewriteContext);
        assertNotNull(queryBuilder.vectorSupplier());
        assertTrue(inProgressLatch.await(5, TimeUnit.SECONDS));
        assertArrayEquals(VectorUtil.vectorAsListToArray(expectedVector), queryBuilder.vectorSupplier().get(), 0.0f);
    }

    public void testRewrite_whenVectorNull_thenReturnCopy() {
        Supplier<float[]> nullSupplier = () -> null;
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder().fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .queryImage(IMAGE_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .vectorSupplier(nullSupplier);
        QueryBuilder queryBuilder = neuralQueryBuilder.doRewrite(null);
        assertEquals(neuralQueryBuilder, queryBuilder);
    }

    public void testRewrite_whenVectorSupplierAndVectorSet_thenReturnKNNQueryBuilder() {
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder().fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .queryImage(IMAGE_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .methodParameters(Map.of("ef_search", 100))
            .rescoreContext(RescoreContext.getDefault())
            .vectorSupplier(TEST_VECTOR_SUPPLIER);

        KNNQueryBuilder expected = KNNQueryBuilder.builder()
            .k(K)
            .fieldName(neuralQueryBuilder.fieldName())
            .methodParameters(neuralQueryBuilder.methodParameters())
            .rescoreContext(neuralQueryBuilder.rescoreContext())
            .vector(TEST_VECTOR_SUPPLIER.get())
            .build();

        QueryBuilder queryBuilder = neuralQueryBuilder.doRewrite(null);
        assertEquals(expected, queryBuilder);
    }

    public void testRewrite_whenFilterSet_thenKNNQueryBuilderFilterSet() {
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder().fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .vectorSupplier(TEST_VECTOR_SUPPLIER)
            .filter(TEST_FILTER);
        QueryBuilder queryBuilder = neuralQueryBuilder.doRewrite(null);
        assertTrue(queryBuilder instanceof KNNQueryBuilder);
        KNNQueryBuilder knnQueryBuilder = (KNNQueryBuilder) queryBuilder;
        assertEquals(neuralQueryBuilder.filter(), knnQueryBuilder.getFilter());
    }

    public void testQueryCreation_whenCreateQueryWithDoToQuery_thenFail() {
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder().fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .vectorSupplier(TEST_VECTOR_SUPPLIER)
            .filter(TEST_FILTER);
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> neuralQueryBuilder.doToQuery(queryShardContext)
        );
        assertEquals("Query cannot be created by NeuralQueryBuilder directly", exception.getMessage());
    }

    @SneakyThrows
    public void testFromXContent_whenBuiltWithDefaults_whenBuiltWithMaxDistance_thenBuildSuccessfully() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "query_image": "string",
                "model_id": "string",
                "max_distance": float
              }
          }
        */
        setUpClusterService(Version.V_2_14_0);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(MAX_DISTANCE_FIELD.getPreferredName(), MAX_DISTANCE)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.fromXContent(contentParser);

        assertEquals(FIELD_NAME, neuralQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, neuralQueryBuilder.queryText());
        assertEquals(MODEL_ID, neuralQueryBuilder.modelId());
        assertEquals(MAX_DISTANCE, neuralQueryBuilder.maxDistance());
    }

    @SneakyThrows
    public void testFromXContent_whenBuiltWithDefaults_whenBuiltWithMinScore_thenBuildSuccessfully() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "query_image": "string",
                "model_id": "string",
                "min_score": float
              }
          }
        */
        setUpClusterService(Version.V_2_14_0);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(MIN_SCORE_FIELD.getPreferredName(), MIN_SCORE)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.fromXContent(contentParser);

        assertEquals(FIELD_NAME, neuralQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, neuralQueryBuilder.queryText());
        assertEquals(MODEL_ID, neuralQueryBuilder.modelId());
        assertEquals(MIN_SCORE, neuralQueryBuilder.minScore());
    }

    @SneakyThrows
    public void testFromXContent_whenBuiltWithDefaults_whenBuiltWithMinScoreAndK_thenFail() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "query_image": "string",
                "model_id": "string",
                "min_score": float,
                "k": int
              }
          }
        */
        setUpClusterService(Version.V_2_14_0);
        XContentBuilder xContentBuilder = null;
        try {
            xContentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(FIELD_NAME)
                .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
                .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
                .field(MIN_SCORE_FIELD.getPreferredName(), MIN_SCORE)
                .field(K_FIELD.getPreferredName(), K)
                .endObject()
                .endObject();
        } catch (IOException e) {
            fail("Failed to create XContentBuilder");
        }

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        expectThrows(IllegalArgumentException.class, () -> NeuralQueryBuilder.fromXContent(contentParser));
    }

    private void setUpClusterService(Version version) {
        ClusterService clusterService = NeuralSearchClusterTestUtils.mockClusterService(version);
        NeuralSearchClusterUtil.instance().initialize(clusterService);
    }
}
