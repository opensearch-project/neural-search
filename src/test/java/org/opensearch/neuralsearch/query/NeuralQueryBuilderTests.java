/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.index.query.AbstractQueryBuilder.BOOST_FIELD;
import static org.opensearch.index.query.AbstractQueryBuilder.NAME_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.EXPAND_NESTED_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.FILTER_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.MAX_DISTANCE_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.MIN_SCORE_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.RESCORE_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.RESCORE_OVERSAMPLE_FIELD;
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.QUERY_TOKENS_FIELD;
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.SEMANTIC_FIELD_SEARCH_ANALYZER_FIELD;
import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_FLOATS_ASSERTION;
import static org.opensearch.neuralsearch.util.TestUtils.xContentBuilderToMap;
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.K_FIELD;
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.MODEL_ID_FIELD;
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.NAME;
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.QUERY_IMAGE_FIELD;
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.QUERY_TEXT_FIELD;
import static org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils.setUpClusterService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.RankFeaturesFieldMapper;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.query.dto.NeuralQueryBuildStage;
import org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.ParseField;
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
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.knn.index.query.rescore.RescoreContext;
import org.opensearch.neuralsearch.common.MinClusterVersionUtil;
import org.opensearch.neuralsearch.util.TestUtils;
import org.opensearch.test.OpenSearchTestCase;

import lombok.SneakyThrows;

import static org.junit.Assert.assertArrayEquals;

public class NeuralQueryBuilderTests extends OpenSearchTestCase {

    private static final String FIELD_NAME = "testField";
    private static final String QUERY_TEXT = "Hello world!";
    private static final String IMAGE_TEXT = "base641234567890";
    private static final String MODEL_ID = "mfgfgdsfgfdgsde";
    private static final String SEARCH_ANALYZER = "search_analyzer";
    private static final Integer K = 10;
    private static final Float MAX_DISTANCE = 1.0f;
    private static final Float MIN_SCORE = 0.985f;
    private static final float BOOST = 1.8f;
    private static final String QUERY_NAME = "queryName";
    private static final String TERM_QUERY_FIELD_NAME = "termQueryFiledName";
    private static final String TERM_QUERY_FIELD_VALUE = "termQueryFiledValue";
    private static final Supplier<float[]> TEST_VECTOR_SUPPLIER = () -> new float[10];
    private static final Supplier<Map<String, Float>> TEST_QUERY_TOKENS_MAP_SUPPLIER = () -> Map.of("key", 1.0f);
    private static final Map<String, Supplier<Map<String, Float>>> TEST_MODEL_ID_TO_QUERY_TOKENS_SUPPLIER_MAP = Map.of(
        MODEL_ID,
        TEST_QUERY_TOKENS_MAP_SUPPLIER
    );
    private static final Map<String, Supplier<float[]>> TEST_MODEL_ID_TO_VECTOR_SUPPLIER_MAP = Map.of(MODEL_ID, TEST_VECTOR_SUPPLIER);

    private static final QueryBuilder TEST_FILTER = new MatchAllQueryBuilder();
    private static final QueryBuilder ADDITIONAL_TEST_FILTER = new TermQueryBuilder(TERM_QUERY_FIELD_NAME, TERM_QUERY_FIELD_VALUE);

    @Before
    public void setup() throws Exception {
        // Initialize EventStatsManager for tests
        TestUtils.initializeEventStatsManager();
    }

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
    public void testFromXContent_whenBuiltWithSearchAnalyzer_thenBuildFails() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "query_image": "string",
                "search_analyzer": "string",
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
            .field(SEMANTIC_FIELD_SEARCH_ANALYZER_FIELD.getPreferredName(), SEARCH_ANALYZER)
            .field(K_FIELD.getPreferredName(), K)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> NeuralQueryBuilder.fromXContent(contentParser)
        );
        final String expectedMessage = "Failed to build the NeuralQueryBuilder: Target field is a KNN "
            + "field using a dense model. semantic_field_search_analyzer "
            + "is not supported since it is for the sparse model.";
        assertEquals(expectedMessage, exception.getMessage());
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
                "expandNestedDocs": true
              }
          }
        */
        setUpClusterService();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(QUERY_IMAGE_FIELD.getPreferredName(), IMAGE_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(K_FIELD.getPreferredName(), K)
            .field(BOOST_FIELD.getPreferredName(), BOOST)
            .field(NAME_FIELD.getPreferredName(), QUERY_NAME)
            .field(EXPAND_NESTED_FIELD.getPreferredName(), Boolean.TRUE)
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
        assertEquals(Boolean.TRUE, neuralQueryBuilder.expandNested());
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
        setUpClusterService();
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
        NeuralSearchClusterTestUtils.setUpClusterService(Version.V_3_0_0);
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
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .modelId(MODEL_ID)
            .queryText(QUERY_TEXT)
            .k(K)
            .expandNested(Boolean.TRUE)
            .filter(TEST_FILTER)
            .build();

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
        assertEquals(Boolean.TRUE, secondInnerMap.get(EXPAND_NESTED_FIELD.getPreferredName()));
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
        setUpClusterService();
        testStreams();
    }

    @SneakyThrows
    private void testStreams() {
        NeuralQueryBuilder original = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .boost(BOOST)
            .queryName(QUERY_NAME)
            .filter(TEST_FILTER)
            .build();

        if (MinClusterVersionUtil.isClusterOnOrAfterMinReqVersion(QUERY_IMAGE_FIELD.getPreferredName())) {
            original.queryImage(IMAGE_TEXT);
        }

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

    public void testStreams_whenOnOrAfter3_1_0() throws IOException {
        Pair<NeuralQueryBuilder, FilterStreamInput> pair = createStreamOnOrAfter3_1_0();
        NeuralQueryBuilder original = pair.getLeft();
        FilterStreamInput filterStreamInput = pair.getRight();

        NeuralQueryBuilder copy = new NeuralQueryBuilder(filterStreamInput);
        assertEquals(original, copy);
    }

    public void testStreams_whenMix3_0_0AndOnOrAfter3_1_0() throws IOException {
        Pair<NeuralQueryBuilder, FilterStreamInput> pair = createStreamOnOrAfter3_1_0();
        NeuralQueryBuilder original = pair.getLeft();
        FilterStreamInput filterStreamInput = pair.getRight();
        setUpClusterService(Version.V_3_0_0);
        filterStreamInput.setVersion(Version.V_3_0_0);
        NeuralQueryBuilder copy = new NeuralQueryBuilder(filterStreamInput);
        original.vectorSupplier(null);
        original.queryTokensMapSupplier(null);
        original.modelIdToQueryTokensSupplierMap(null);
        original.modelIdToVectorSupplierMap(null);
        assertEquals(original, copy);
    }

    private Pair<NeuralQueryBuilder, FilterStreamInput> createStreamOnOrAfter3_1_0() throws IOException {
        setUpClusterService();
        NeuralQueryBuilder original = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .boost(BOOST)
            .queryName(QUERY_NAME)
            .filter(TEST_FILTER)
            .vectorSupplier(TEST_VECTOR_SUPPLIER)
            .queryTokensMapSupplier(TEST_QUERY_TOKENS_MAP_SUPPLIER)
            .modelIdToQueryTokensSupplierMap(TEST_MODEL_ID_TO_QUERY_TOKENS_SUPPLIER_MAP)
            .modelIdToVectorSupplierMap(TEST_MODEL_ID_TO_VECTOR_SUPPLIER_MAP)
            .build();

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        streamOutput.setVersion(Version.CURRENT);
        original.writeTo(streamOutput);

        FilterStreamInput filterStreamInput = new NamedWriteableAwareStreamInput(
            streamOutput.bytes().streamInput(),
            new NamedWriteableRegistry(
                List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new))
            )
        );
        filterStreamInput.setVersion(Version.CURRENT);
        return Pair.of(original, filterStreamInput);
    }

    public void testHashAndEquals() {
        setUpClusterService();
        final String fieldName2 = "field 2";
        final String queryText2 = "query text 2";
        final String queryImage2 = "query image 2";
        final String modelId2 = "model-2";
        final float boost2 = 3.8f;
        final String queryName2 = "query-2";
        final int k2 = 2;
        final float minScore1 = 1f;
        final float minScore2 = 2f;
        final float maxDistance1 = 10f;
        final float maxDistance2 = 20f;
        final Supplier<float[]> vectorSupplier2 = () -> new float[] { 1.f };
        final Supplier<float[]> vectorSupplier3 = () -> new float[] { 3.f };
        final Map<String, ?> methodParameters2 = Map.of("ef_search", 100);
        final Map<String, ?> methodParameters3 = Map.of("ef_search", 101);
        final RescoreContext rescoreContext2 = RescoreContext.getDefault();
        final RescoreContext rescoreContext3 = RescoreContext.builder().build();
        final String analyzer1 = "standard";
        final String analyzer2 = "bert_uncased";

        final QueryBuilder filter2 = new MatchNoneQueryBuilder();

        final NeuralQueryBuilder neuralQueryBuilder_baseline = getBaselineNeuralQueryBuilder();

        // Identical to neuralQueryBuilder_baseline
        final NeuralQueryBuilder neuralQueryBuilder_baselineCopy = getBaselineNeuralQueryBuilder();
        neuralQueryBuilder_baselineCopy.vectorSupplier(vectorSupplier2);
        neuralQueryBuilder_baselineCopy.methodParameters(methodParameters2);
        neuralQueryBuilder_baselineCopy.rescoreContext(rescoreContext2);

        // Identical to neuralQueryBuilder_baseline except diff field name
        final NeuralQueryBuilder neuralQueryBuilder_diffFieldName = getBaselineNeuralQueryBuilder();
        neuralQueryBuilder_diffFieldName.fieldName(fieldName2);

        // Identical to neuralQueryBuilder_baseline except diff query text
        final NeuralQueryBuilder neuralQueryBuilder_diffQueryText = getBaselineNeuralQueryBuilder();
        neuralQueryBuilder_diffQueryText.queryText(queryText2);

        // Identical to neuralQueryBuilder_baseline except diff image text
        final NeuralQueryBuilder neuralQueryBuilder_diffImageText = getBaselineNeuralQueryBuilder();
        neuralQueryBuilder_diffImageText.queryImage(queryImage2);

        // Identical to neuralQueryBuilder_baseline except diff model ID
        final NeuralQueryBuilder neuralQueryBuilder_diffModelId = getBaselineNeuralQueryBuilder();
        neuralQueryBuilder_diffModelId.modelId(modelId2);

        // Identical to neuralQueryBuilder_baseline except diff k
        final NeuralQueryBuilder neuralQueryBuilder_diffK = getBaselineNeuralQueryBuilder();
        neuralQueryBuilder_diffK.k(k2);

        // Identical to neuralQueryBuilder_baseline except diff maxDistance
        final NeuralQueryBuilder neuralQueryBuilder_baseline_withMaxDistance = getBaselineNeuralQueryBuilder();
        neuralQueryBuilder_baseline_withMaxDistance.k(null);
        neuralQueryBuilder_baseline_withMaxDistance.maxDistance(maxDistance1);
        final NeuralQueryBuilder neuralQueryBuilder_diffMaxDistance = getBaselineNeuralQueryBuilder();
        neuralQueryBuilder_diffMaxDistance.k(null);
        neuralQueryBuilder_diffMaxDistance.maxDistance(maxDistance2);

        // Identical to neuralQueryBuilder_baseline except diff minScore
        final NeuralQueryBuilder neuralQueryBuilder_baseline_withMinScore = getBaselineNeuralQueryBuilder();
        neuralQueryBuilder_baseline_withMinScore.k(null);
        neuralQueryBuilder_baseline_withMinScore.minScore(minScore1);
        final NeuralQueryBuilder neuralQueryBuilder_diffMinScore = getBaselineNeuralQueryBuilder();
        neuralQueryBuilder_diffMinScore.k(null);
        neuralQueryBuilder_diffMinScore.minScore(minScore2);

        // Identical to neuralQueryBuilder_baseline except for expandNested
        final NeuralQueryBuilder neuralQueryBuilder_diffExpandNested = getBaselineNeuralQueryBuilder();
        neuralQueryBuilder_diffExpandNested.expandNested(Boolean.FALSE);

        // Identical to neuralQueryBuilder_baseline except for vectorSupplier
        final NeuralQueryBuilder neuralQueryBuilder_diffVectorSupplier = getBaselineNeuralQueryBuilder();
        neuralQueryBuilder_diffVectorSupplier.vectorSupplier(vectorSupplier3);

        // Identical to neuralQueryBuilder_baseline except diff boost
        final NeuralQueryBuilder neuralQueryBuilder_diffBoost = getBaselineNeuralQueryBuilder();
        neuralQueryBuilder_diffBoost.boost(boost2);

        // Identical to neuralQueryBuilder_baseline except diff query name
        final NeuralQueryBuilder neuralQueryBuilder_diffQueryName = getBaselineNeuralQueryBuilder();
        neuralQueryBuilder_diffQueryName.queryName(queryName2);

        // Identical to neuralQueryBuilder_baseline except diff filter
        final NeuralQueryBuilder neuralQueryBuilder_diffFilter = getBaselineNeuralQueryBuilder();
        neuralQueryBuilder_diffFilter.filter(filter2);

        // Identical to neuralQueryBuilder_baseline except diff methodParameters
        final NeuralQueryBuilder neuralQueryBuilder_diffMethodParameters = getBaselineNeuralQueryBuilder();
        neuralQueryBuilder_diffMethodParameters.methodParameters(methodParameters3);

        // Identical to neuralQueryBuilder_baseline except diff rescoreContext
        final NeuralQueryBuilder neuralQueryBuilder_diffRescoreContext = getBaselineNeuralQueryBuilder();
        neuralQueryBuilder_diffRescoreContext.rescoreContext(rescoreContext3);

        final NeuralQueryBuilder neuralQueryBuilder_analyzer1 = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .searchAnalyzer(analyzer1)
            .embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE)
            .buildStage(NeuralQueryBuildStage.REWRITE)
            .queryText(QUERY_TEXT)
            .isSemanticField(true)
            .build();
        final NeuralQueryBuilder neuralQueryBuilder_analyzer2 = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .searchAnalyzer(analyzer2)
            .embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE)
            .buildStage(NeuralQueryBuildStage.REWRITE)
            .queryText(QUERY_TEXT)
            .isSemanticField(true)
            .build();

        assertEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_baseline);
        assertEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_baseline.hashCode());

        assertEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_baselineCopy);
        assertEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_baselineCopy.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffFieldName);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffFieldName.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffQueryText);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffQueryText.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffImageText);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffImageText.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffModelId);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffModelId.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffK);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffK.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline_withMaxDistance, neuralQueryBuilder_diffMaxDistance);
        assertNotEquals(neuralQueryBuilder_baseline_withMaxDistance.hashCode(), neuralQueryBuilder_diffMaxDistance.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline_withMinScore, neuralQueryBuilder_diffMinScore);
        assertNotEquals(neuralQueryBuilder_baseline_withMinScore.hashCode(), neuralQueryBuilder_diffMinScore.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffBoost);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffBoost.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffQueryName);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffQueryName.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffFilter);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffFilter.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffMethodParameters);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffMethodParameters.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffRescoreContext);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffRescoreContext.hashCode());

        assertNotEquals(neuralQueryBuilder_analyzer1, neuralQueryBuilder_analyzer2);
        assertNotEquals(neuralQueryBuilder_analyzer1.hashCode(), neuralQueryBuilder_analyzer2.hashCode());
    }

    private NeuralQueryBuilder getBaselineNeuralQueryBuilder() {
        final String fieldName1 = "field 1";
        final String queryText1 = "query text 1";
        final String queryImage1 = "query image 1";
        final String modelId1 = "model-1";
        final float boost1 = 1.8f;
        final String queryName1 = "query-1";
        final int k1 = 1;
        final Supplier<float[]> vectorSupplier1 = () -> new float[] { 1.f };
        final Map<String, ?> methodParameters1 = Map.of("ef_search", 100);
        final RescoreContext rescoreContext1 = RescoreContext.getDefault();

        final QueryBuilder filter1 = new MatchAllQueryBuilder();

        return NeuralQueryBuilder.builder()
            .fieldName(fieldName1)
            .queryText(queryText1)
            .queryImage(queryImage1)
            .modelId(modelId1)
            .k(k1)
            .boost(boost1)
            .queryName(queryName1)
            .expandNested(Boolean.TRUE)
            .vectorSupplier(vectorSupplier1)
            .filter(filter1)
            .methodParameters(methodParameters1)
            .rescoreContext(rescoreContext1)
            .build();
    }

    public void testFilter_whenAddBoolQueryBuilderToNeuralQueryBuilder_thenFilterSuccessful() {
        setUpClusterService();
        // Test for Null Case
        NeuralQueryBuilder neuralQueryBuilder = getBaselineNeuralQueryBuilder();
        QueryBuilder updatedNeuralQueryBuilder = neuralQueryBuilder.filter(null);
        assertEquals(neuralQueryBuilder, updatedNeuralQueryBuilder);

        // Test for valid case
        neuralQueryBuilder = getBaselineNeuralQueryBuilder();
        updatedNeuralQueryBuilder = neuralQueryBuilder.filter(ADDITIONAL_TEST_FILTER);
        BoolQueryBuilder expectedUpdatedQueryFilter = new BoolQueryBuilder();
        expectedUpdatedQueryFilter.must(TEST_FILTER);
        expectedUpdatedQueryFilter.filter(ADDITIONAL_TEST_FILTER);
        assertEquals(neuralQueryBuilder, updatedNeuralQueryBuilder);
        assertEquals(expectedUpdatedQueryFilter, neuralQueryBuilder.filter());

        // Test for queryBuilder without filter initialized where filter function would
        // simply assign filter to its filter field.
        neuralQueryBuilder = NeuralQueryBuilder.builder().fieldName(FIELD_NAME).queryText(QUERY_TEXT).modelId(MODEL_ID).k(K).build();
        updatedNeuralQueryBuilder = neuralQueryBuilder.filter(TEST_FILTER);
        assertEquals(neuralQueryBuilder, updatedNeuralQueryBuilder);
        assertEquals(TEST_FILTER, neuralQueryBuilder.filter());
    }

    public void testQueryCreation_whenCreateQueryWithDoToQuery_thenFail() {
        setUpClusterService();
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .vectorSupplier(TEST_VECTOR_SUPPLIER)
            .filter(TEST_FILTER)
            .build();
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.fieldMapper(FIELD_NAME)).thenReturn(mock(MappedFieldType.class));
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> neuralQueryBuilder.doToQuery(queryShardContext)
        );
        assertEquals("Query cannot be created by NeuralQueryBuilder directly", exception.getMessage());
    }

    public void testQueryCreation_whenCreateQueryUnmappedField_thenMatchNoDocQuery() {
        setUpClusterService();
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .vectorSupplier(TEST_VECTOR_SUPPLIER)
            .filter(TEST_FILTER)
            .build();
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.fieldMapper(FIELD_NAME)).thenReturn(null);
        Query query = neuralQueryBuilder.doToQuery(queryShardContext);
        assertTrue(query instanceof MatchNoDocsQuery);
    }

    public void testQueryCreation_whenUnmappedField_thenMatchNoDocsQuery() {
        setUpClusterService();
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .vectorSupplier(TEST_VECTOR_SUPPLIER)
            .filter(TEST_FILTER)
            .build();
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.fieldMapper(FIELD_NAME)).thenReturn(null);
        Query query = neuralQueryBuilder.doToQuery(queryShardContext);
        assertTrue(query instanceof MatchNoDocsQuery);
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

    @SneakyThrows
    public void testFromXContent_whenBuiltWithQueryTokens_3_0_0_thenFail() {
        setUpClusterService(Version.V_3_0_0);
        XContentBuilder xContentBuilder = null;
        try {
            xContentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(FIELD_NAME)
                .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
                .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
                .field(K_FIELD.getPreferredName(), K)
                .startObject(QUERY_TOKENS_FIELD.getPreferredName())
                .field("key1", 1.0f)
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            fail("Failed to create XContentBuilder");
        }

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();

        ParsingException exception = assertThrows(ParsingException.class, () -> NeuralQueryBuilder.fromXContent(contentParser));

        assertEquals("[neural] query does not support [query_tokens]", exception.getMessage());
    }

    @SneakyThrows
    public void testFromXContent_whenBuiltWithQueryTokens_thenBuildSuccessfully() {
        setUpClusterService();
        XContentBuilder xContentBuilder = null;
        try {
            xContentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(FIELD_NAME)
                .startObject(QUERY_TOKENS_FIELD.getPreferredName())
                .field("key1", 1.0f)
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            fail("Failed to create XContentBuilder");
        }

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.fromXContent(contentParser);

        assertEquals(FIELD_NAME, neuralQueryBuilder.fieldName());
        assertEquals(Map.of("key1", 1.0f), neuralQueryBuilder.queryTokensMapSupplier().get());
    }

    @SneakyThrows
    public void testCreateKNNQueryBuilder_whenClusterSupportsNeuralKNNQueryBuilder_thenReturnsNeuralKNNQueryBuilder() {
        setUpClusterService(Version.V_3_0_0);

        // Create a NeuralQueryBuilder instance
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .build();

        // Use reflection to test the private createKNNQueryBuilder method
        float[] testVector = new float[] { 1.0f, 2.0f, 3.0f };

        try {
            // Access the private method using reflection
            java.lang.reflect.Method createKNNQueryBuilderMethod = NeuralQueryBuilder.class.getDeclaredMethod(
                "createKNNQueryBuilder",
                String.class,
                float[].class
            );
            createKNNQueryBuilderMethod.setAccessible(true);

            QueryBuilder result = (QueryBuilder) createKNNQueryBuilderMethod.invoke(neuralQueryBuilder, FIELD_NAME, testVector);

            // Verify the result is NeuralKNNQueryBuilder
            assertNotNull("Result should not be null", result);
            assertTrue("Result should be NeuralKNNQueryBuilder", result instanceof NeuralKNNQueryBuilder);

            // Verify the properties are set correctly
            NeuralKNNQueryBuilder neuralKNNQueryBuilder = (NeuralKNNQueryBuilder) result;
            assertEquals("Original query text should match", QUERY_TEXT, neuralKNNQueryBuilder.getOriginalQueryText());

        } catch (Exception e) {
            fail("Failed to test createKNNQueryBuilder: " + e.getMessage());
        }
    }

    @SneakyThrows
    public void testCreateKNNQueryBuilder_whenClusterDoesNotSupportNeuralKNNQueryBuilder_thenReturnsNeuralQueryBuilder() {
        // Set cluster version to 2.19.0 which doesn't support NeuralKNNQueryBuilder
        setUpClusterService(Version.V_2_19_0);

        // Create a NeuralQueryBuilder instance
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .build();

        // Use reflection to test the private createKNNQueryBuilder method
        float[] testVector = new float[] { 1.0f, 2.0f, 3.0f };

        try {
            // Access the private method using reflection
            java.lang.reflect.Method createKNNQueryBuilderMethod = NeuralQueryBuilder.class.getDeclaredMethod(
                "createKNNQueryBuilder",
                String.class,
                float[].class
            );
            createKNNQueryBuilderMethod.setAccessible(true);

            QueryBuilder result = (QueryBuilder) createKNNQueryBuilderMethod.invoke(neuralQueryBuilder, FIELD_NAME, testVector);

            // Verify the result is NeuralQueryBuilder (not NeuralKNNQueryBuilder)
            assertNotNull("Result should not be null", result);
            assertTrue("Result should be NeuralQueryBuilder", result instanceof NeuralQueryBuilder);
            assertFalse("Result should NOT be NeuralKNNQueryBuilder", result instanceof NeuralKNNQueryBuilder);

            // Verify the vector supplier is set
            NeuralQueryBuilder returnedNeuralQueryBuilder = (NeuralQueryBuilder) result;
            assertNotNull("Vector supplier should be set", returnedNeuralQueryBuilder.vectorSupplier());
            assertArrayEquals("Vector should match", testVector, returnedNeuralQueryBuilder.vectorSupplier().get(), 0.0f);

        } catch (Exception e) {
            fail("Failed to test createKNNQueryBuilder: " + e.getMessage());
        }
    }

    @SneakyThrows
    public void testCreateKNNQueryBuilder_withRadialSearchParameters_whenClusterSupportsNeuralKNNQueryBuilder() {
        setUpClusterService(Version.V_3_0_0);

        // Test with min_score (radial search)
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .minScore(MIN_SCORE)
            .build();

        float[] testVector = new float[] { 1.0f, 2.0f, 3.0f };

        try {
            // Access the private method using reflection
            java.lang.reflect.Method createKNNQueryBuilderMethod = NeuralQueryBuilder.class.getDeclaredMethod(
                "createKNNQueryBuilder",
                String.class,
                float[].class
            );
            createKNNQueryBuilderMethod.setAccessible(true);

            QueryBuilder result = (QueryBuilder) createKNNQueryBuilderMethod.invoke(neuralQueryBuilder, FIELD_NAME, testVector);

            // Verify the result is NeuralKNNQueryBuilder with radial search parameters
            assertNotNull("Result should not be null", result);
            assertTrue("Result should be NeuralKNNQueryBuilder", result instanceof NeuralKNNQueryBuilder);

            NeuralKNNQueryBuilder neuralKNNQueryBuilder = (NeuralKNNQueryBuilder) result;
            assertEquals("Min score should match", MIN_SCORE, neuralKNNQueryBuilder.getKnnQueryBuilder().getMinScore());
            // When using radial search (minScore), k should either be null or 0
            // KNNQueryBuilder may convert null k to 0 internally
            Integer actualK = neuralKNNQueryBuilder.getKnnQueryBuilder().getK();
            assertTrue("K should be null or 0 for radial search, but was: " + actualK, actualK == null || actualK == 0);

        } catch (Exception e) {
            fail("Failed to test createKNNQueryBuilder with radial search: " + e.getMessage());
        }
    }

    @SneakyThrows
    public void testCreateKNNQueryBuilder_withRadialSearchParameters_whenClusterDoesNotSupportNeuralKNNQueryBuilder() {
        // Set cluster version to 2.19.0
        setUpClusterService(Version.V_2_19_0);

        // Test with max_distance (radial search)
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .maxDistance(MAX_DISTANCE)
            .build();

        float[] testVector = new float[] { 1.0f, 2.0f, 3.0f };

        try {
            // Access the private method using reflection
            java.lang.reflect.Method createKNNQueryBuilderMethod = NeuralQueryBuilder.class.getDeclaredMethod(
                "createKNNQueryBuilder",
                String.class,
                float[].class
            );
            createKNNQueryBuilderMethod.setAccessible(true);

            QueryBuilder result = (QueryBuilder) createKNNQueryBuilderMethod.invoke(neuralQueryBuilder, FIELD_NAME, testVector);

            // Verify it returns NeuralQueryBuilder for backward compatibility
            assertNotNull("Result should not be null", result);
            assertTrue("Result should be NeuralQueryBuilder", result instanceof NeuralQueryBuilder);

            NeuralQueryBuilder returnedNeuralQueryBuilder = (NeuralQueryBuilder) result;
            assertEquals("Max distance should be preserved", MAX_DISTANCE, returnedNeuralQueryBuilder.maxDistance());
            assertNull("K should be null", returnedNeuralQueryBuilder.k());

        } catch (Exception e) {
            fail("Failed to test createKNNQueryBuilder with radial search on old version: " + e.getMessage());
        }
    }
}
