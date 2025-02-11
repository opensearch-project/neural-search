/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.index.query.AbstractQueryBuilder.BOOST_FIELD;
import static org.opensearch.index.query.AbstractQueryBuilder.DEFAULT_BOOST;
import static org.opensearch.index.remote.RemoteStoreEnums.PathType.HASHED_PREFIX;
import static org.opensearch.knn.index.query.KNNQueryBuilder.FILTER_FIELD;
import static org.opensearch.neuralsearch.util.TestUtils.xContentBuilderToMap;
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.K_FIELD;
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.MODEL_ID_FIELD;
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.QUERY_TEXT_FIELD;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.FilterStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.knn.index.KNNSettings;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.KNNEngine;
import org.opensearch.knn.index.engine.KNNMethodContext;
import org.opensearch.knn.index.engine.MethodComponentContext;
import org.opensearch.knn.index.mapper.KNNMappingConfig;
import org.opensearch.knn.index.mapper.KNNVectorFieldType;
import org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import lombok.SneakyThrows;

public class HybridQueryBuilderTests extends OpenSearchQueryTestCase {
    static final String VECTOR_FIELD_NAME = "vectorField";
    static final String TEXT_FIELD_NAME = "field";
    static final String QUERY_TEXT = "Hello world!";
    static final String TERM_QUERY_TEXT = "keyword";
    static final String MODEL_ID = "mfgfgdsfgfdgsde";
    static final int K = 10;
    static final float BOOST = 1.8f;
    static final Supplier<float[]> TEST_VECTOR_SUPPLIER = () -> new float[4];
    static final QueryBuilder TEST_FILTER = new MatchAllQueryBuilder();
    @Mock
    private ClusterService clusterService;
    private AutoCloseable openMocks;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        openMocks = MockitoAnnotations.openMocks(this);
        // This is required to make sure that before every test we are initializing the KNNSettings. Not doing this
        // leads to failures of unit tests cases when a unit test is run separately. Try running this test:
        // ./gradlew ':test' --tests "org.opensearch.knn.training.TrainingJobTests.testRun_success" and see it fails
        // but if run along with other tests this test passes.
        initKNNSettings();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        openMocks.close();
    }

    @SneakyThrows
    public void testDoToQuery_whenNoSubqueries_thenBuildSuccessfully() {
        HybridQueryBuilder queryBuilder = new HybridQueryBuilder();
        Index dummyIndex = new Index("dummy", "dummy");
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        Query queryNoSubQueries = queryBuilder.doToQuery(mockQueryShardContext);
        assertTrue(queryNoSubQueries instanceof MatchNoDocsQuery);
    }

    @SneakyThrows
    public void testDoToQuery_whenOneSubquery_thenBuildSuccessfully() {
        HybridQueryBuilder queryBuilder = new HybridQueryBuilder();
        queryBuilder.paginationDepth(10);
        Index dummyIndex = new Index("dummy", "dummy");
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        KNNVectorFieldType mockKNNVectorField = mock(KNNVectorFieldType.class);
        KNNMappingConfig mockKNNMappingConfig = mock(KNNMappingConfig.class);
        KNNMethodContext knnMethodContext = new KNNMethodContext(KNNEngine.FAISS, SpaceType.L2, MethodComponentContext.EMPTY);
        when(mockKNNVectorField.getKnnMappingConfig()).thenReturn(mockKNNMappingConfig);
        when(mockKNNMappingConfig.getKnnMethodContext()).thenReturn(Optional.of(knnMethodContext));
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        when(mockKNNVectorField.getKnnMappingConfig().getDimension()).thenReturn(4);
        when(mockKNNVectorField.getVectorDataType()).thenReturn(VectorDataType.FLOAT);
        when(mockQueryShardContext.fieldMapper(eq(VECTOR_FIELD_NAME))).thenReturn(mockKNNVectorField);
        IndexMetadata indexMetadata = getIndexMetadata();
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, Integer.toString(3)).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        when(mockQueryShardContext.getIndexSettings()).thenReturn(indexSettings);

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(VECTOR_FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .vectorSupplier(TEST_VECTOR_SUPPLIER)
            .build();

        queryBuilder.add(neuralQueryBuilder);
        Query queryOnlyNeural = queryBuilder.doToQuery(mockQueryShardContext);
        assertNotNull(queryOnlyNeural);
        assertTrue(queryOnlyNeural instanceof HybridQuery);
        assertEquals(1, ((HybridQuery) queryOnlyNeural).getSubQueries().size());
        assertTrue(((HybridQuery) queryOnlyNeural).getSubQueries().iterator().next() instanceof NeuralKNNQuery);
        Query knnQuery = ((NeuralKNNQuery) ((HybridQuery) queryOnlyNeural).getSubQueries().iterator().next()).getKnnQuery();
        assertNotNull(knnQuery);
        assertTrue(knnQuery.toString(VECTOR_FIELD_NAME).contains(VECTOR_FIELD_NAME));
    }

    @SneakyThrows
    public void testDoToQuery_whenMultipleSubqueries_thenBuildSuccessfully() {
        HybridQueryBuilder queryBuilder = new HybridQueryBuilder();
        queryBuilder.paginationDepth(10);
        Index dummyIndex = new Index("dummy", "dummy");
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        KNNVectorFieldType mockKNNVectorField = mock(KNNVectorFieldType.class);
        KNNMappingConfig mockKNNMappingConfig = mock(KNNMappingConfig.class);
        KNNMethodContext knnMethodContext = new KNNMethodContext(KNNEngine.FAISS, SpaceType.L2, MethodComponentContext.EMPTY);
        when(mockKNNVectorField.getKnnMappingConfig()).thenReturn(mockKNNMappingConfig);
        when(mockKNNMappingConfig.getKnnMethodContext()).thenReturn(Optional.of(knnMethodContext));
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        when(mockKNNVectorField.getKnnMappingConfig().getDimension()).thenReturn(4);
        when(mockKNNVectorField.getVectorDataType()).thenReturn(VectorDataType.FLOAT);
        when(mockQueryShardContext.fieldMapper(eq(VECTOR_FIELD_NAME))).thenReturn(mockKNNVectorField);
        IndexMetadata indexMetadata = getIndexMetadata();
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, Integer.toString(3)).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        when(mockQueryShardContext.getIndexSettings()).thenReturn(indexSettings);

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(VECTOR_FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .vectorSupplier(TEST_VECTOR_SUPPLIER)
            .build();

        queryBuilder.add(neuralQueryBuilder);

        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT);
        queryBuilder.add(termSubQuery);

        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        Query queryTwoSubQueries = queryBuilder.doToQuery(mockQueryShardContext);
        assertNotNull(queryTwoSubQueries);
        assertTrue(queryTwoSubQueries instanceof HybridQuery);
        assertEquals(2, ((HybridQuery) queryTwoSubQueries).getSubQueries().size());
        // verify knn vector query
        Iterator<Query> queryIterator = ((HybridQuery) queryTwoSubQueries).getSubQueries().iterator();
        Query firstQuery = queryIterator.next();
        assertTrue(firstQuery instanceof NeuralKNNQuery);
        Query knnQuery = ((NeuralKNNQuery) firstQuery).getKnnQuery();
        assertNotNull(knnQuery);
        assertTrue(knnQuery.toString(VECTOR_FIELD_NAME).contains(VECTOR_FIELD_NAME));
        // verify term query
        Query secondQuery = queryIterator.next();
        assertTrue(secondQuery instanceof TermQuery);
        TermQuery termQuery = (TermQuery) secondQuery;
        assertEquals(TEXT_FIELD_NAME, termQuery.getTerm().field());
        assertEquals(TERM_QUERY_TEXT, termQuery.getTerm().text());
    }

    @SneakyThrows
    public void testDoToQuery_whenPaginationDepthIsGreaterThan10000_thenBuildSuccessfully() {
        HybridQueryBuilder queryBuilder = new HybridQueryBuilder();
        queryBuilder.paginationDepth(10001);
        Index dummyIndex = new Index("dummy", "dummy");
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        KNNVectorFieldType mockKNNVectorField = mock(KNNVectorFieldType.class);
        KNNMappingConfig mockKNNMappingConfig = mock(KNNMappingConfig.class);
        KNNMethodContext knnMethodContext = new KNNMethodContext(KNNEngine.FAISS, SpaceType.L2, MethodComponentContext.EMPTY);
        when(mockKNNVectorField.getKnnMappingConfig()).thenReturn(mockKNNMappingConfig);
        when(mockKNNMappingConfig.getKnnMethodContext()).thenReturn(Optional.of(knnMethodContext));
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        when(mockKNNVectorField.getKnnMappingConfig().getDimension()).thenReturn(4);
        when(mockKNNVectorField.getVectorDataType()).thenReturn(VectorDataType.FLOAT);
        when(mockQueryShardContext.fieldMapper(eq(VECTOR_FIELD_NAME))).thenReturn(mockKNNVectorField);
        IndexMetadata indexMetadata = getIndexMetadata();
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, Integer.toString(3)).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        when(mockQueryShardContext.getIndexSettings()).thenReturn(indexSettings);

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(VECTOR_FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .vectorSupplier(TEST_VECTOR_SUPPLIER)
            .build();

        queryBuilder.add(neuralQueryBuilder);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> queryBuilder.doToQuery(mockQueryShardContext)
        );
        assertThat(
            exception.getMessage(),
            containsString("pagination_depth should be less than or equal to index.max_result_window setting")
        );
    }

    @SneakyThrows
    public void testDoToQuery_whenPaginationDepthIsLessThanZero_thenBuildSuccessfully() {
        HybridQueryBuilder queryBuilder = new HybridQueryBuilder();
        queryBuilder.paginationDepth(-1);
        Index dummyIndex = new Index("dummy", "dummy");
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        KNNVectorFieldType mockKNNVectorField = mock(KNNVectorFieldType.class);
        KNNMappingConfig mockKNNMappingConfig = mock(KNNMappingConfig.class);
        KNNMethodContext knnMethodContext = new KNNMethodContext(KNNEngine.FAISS, SpaceType.L2, MethodComponentContext.EMPTY);
        when(mockKNNVectorField.getKnnMappingConfig()).thenReturn(mockKNNMappingConfig);
        when(mockKNNMappingConfig.getKnnMethodContext()).thenReturn(Optional.of(knnMethodContext));
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        when(mockKNNVectorField.getKnnMappingConfig().getDimension()).thenReturn(4);
        when(mockKNNVectorField.getVectorDataType()).thenReturn(VectorDataType.FLOAT);
        when(mockQueryShardContext.fieldMapper(eq(VECTOR_FIELD_NAME))).thenReturn(mockKNNVectorField);
        IndexMetadata indexMetadata = getIndexMetadata();
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, Integer.toString(3)).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        when(mockQueryShardContext.getIndexSettings()).thenReturn(indexSettings);

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(VECTOR_FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .vectorSupplier(TEST_VECTOR_SUPPLIER)
            .build();

        queryBuilder.add(neuralQueryBuilder);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> queryBuilder.doToQuery(mockQueryShardContext)
        );
        assertThat(exception.getMessage(), containsString("pagination_depth should be greater than 0"));
    }

    @SneakyThrows
    public void testDoToQuery_whenTooManySubqueries_thenFail() {
        // create query with 6 sub-queries, which is more than current max allowed
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("queries")
            .startObject()
            .startObject("term")
            .field(TEXT_FIELD_NAME, RandomizedTest.randomAsciiAlphanumOfLength(10))
            .endObject()
            .endObject()
            .startObject()
            .startObject("term")
            .field(TEXT_FIELD_NAME, RandomizedTest.randomAsciiAlphanumOfLength(10))
            .endObject()
            .endObject()
            .startObject()
            .startObject("term")
            .field(TEXT_FIELD_NAME, RandomizedTest.randomAsciiAlphanumOfLength(10))
            .endObject()
            .endObject()
            .startObject()
            .startObject("term")
            .field(TEXT_FIELD_NAME, RandomizedTest.randomAsciiAlphanumOfLength(10))
            .endObject()
            .endObject()
            .startObject()
            .startObject("term")
            .field(TEXT_FIELD_NAME, RandomizedTest.randomAsciiAlphanumOfLength(10))
            .endObject()
            .endObject()
            .startObject()
            .startObject("term")
            .field(TEXT_FIELD_NAME, TERM_QUERY_TEXT)
            .endObject()
            .endObject()
            .endArray()
            .endObject();

        NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(
            List.of(
                new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField(TermQueryBuilder.NAME), TermQueryBuilder::fromXContent),
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(HybridQueryBuilder.NAME),
                    HybridQueryBuilder::fromXContent
                )
            )
        );
        XContentParser contentParser = createParser(
            namedXContentRegistry,
            xContentBuilder.contentType().xContent(),
            BytesReference.bytes(xContentBuilder)
        );
        contentParser.nextToken();

        ParsingException exception = expectThrows(ParsingException.class, () -> HybridQueryBuilder.fromXContent(contentParser));
        assertThat(exception.getMessage(), containsString("Number of sub-queries exceeds maximum supported"));
    }

    /**
     * Tests basic query:
     * {
     *     "query": {
     *         "hybrid": {
     *              "queries": [
     *                  {
     *                      "neural": {
     *                          "text_knn": {
     *                              "query_text": "Hello world",
     *                              "model_id": "dcsdcasd",
     *                              "k": 1
     *                          }
     *                      }
     *                  },
     *                  {
     *                      "term": {
     *                          "text": "keyword"
     *                      }
     *                  }
     *              ]
     *          }
     *      }
     * }
     */
    @SneakyThrows
    public void testFromXContent_whenMultipleSubQueries_thenBuildSuccessfully() {
        setUpClusterService();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("queries")
            .startObject()
            .startObject(NeuralQueryBuilder.NAME)
            .startObject(VECTOR_FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(K_FIELD.getPreferredName(), K)
            .field(BOOST_FIELD.getPreferredName(), BOOST)
            .endObject()
            .endObject()
            .endObject()
            .startObject()
            .startObject(TermQueryBuilder.NAME)
            .field(TEXT_FIELD_NAME, TERM_QUERY_TEXT)
            .endObject()
            .endObject()
            .endArray()
            .field("pagination_depth", 10)
            .endObject();

        NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(
            List.of(
                new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField(TermQueryBuilder.NAME), TermQueryBuilder::fromXContent),
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(NeuralQueryBuilder.NAME),
                    NeuralQueryBuilder::fromXContent
                ),
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(HybridQueryBuilder.NAME),
                    HybridQueryBuilder::fromXContent
                )
            )
        );
        XContentParser contentParser = createParser(
            namedXContentRegistry,
            xContentBuilder.contentType().xContent(),
            BytesReference.bytes(xContentBuilder)
        );
        contentParser.nextToken();

        HybridQueryBuilder queryTwoSubQueries = HybridQueryBuilder.fromXContent(contentParser);
        assertEquals(2, queryTwoSubQueries.queries().size());
        assertTrue(queryTwoSubQueries.queries().get(0) instanceof NeuralQueryBuilder);
        assertTrue(queryTwoSubQueries.queries().get(1) instanceof TermQueryBuilder);
        assertEquals(10, queryTwoSubQueries.paginationDepth().intValue());
        // verify knn vector query
        NeuralQueryBuilder neuralQueryBuilder = (NeuralQueryBuilder) queryTwoSubQueries.queries().get(0);
        assertEquals(VECTOR_FIELD_NAME, neuralQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, neuralQueryBuilder.queryText());
        assertEquals(K, (int) neuralQueryBuilder.k());
        assertEquals(MODEL_ID, neuralQueryBuilder.modelId());
        assertEquals(BOOST, neuralQueryBuilder.boost(), 0f);
        // verify term query
        TermQueryBuilder termQueryBuilder = (TermQueryBuilder) queryTwoSubQueries.queries().get(1);
        assertEquals(TEXT_FIELD_NAME, termQueryBuilder.fieldName());
        assertEquals(TERM_QUERY_TEXT, termQueryBuilder.value());
    }

    @SneakyThrows
    public void testFromXContent_whenIncorrectFormat_thenFail() {
        XContentBuilder unsupportedFieldXContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("random_field")
            .startObject()
            .startObject(NeuralQueryBuilder.NAME)
            .startObject(VECTOR_FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(K_FIELD.getPreferredName(), K)
            .field(BOOST_FIELD.getPreferredName(), BOOST)
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .endObject();

        NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(
            List.of(
                new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField(TermQueryBuilder.NAME), TermQueryBuilder::fromXContent),
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(NeuralQueryBuilder.NAME),
                    NeuralQueryBuilder::fromXContent
                ),
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(HybridQueryBuilder.NAME),
                    HybridQueryBuilder::fromXContent
                )
            )
        );
        XContentParser contentParser = createParser(
            namedXContentRegistry,
            unsupportedFieldXContentBuilder.contentType().xContent(),
            BytesReference.bytes(unsupportedFieldXContentBuilder)
        );
        contentParser.nextToken();

        expectThrows(ParsingException.class, () -> HybridQueryBuilder.fromXContent(contentParser));

        XContentBuilder emptySubQueriesXContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("queries")
            .endArray()
            .endObject();

        XContentParser contentParser2 = createParser(
            namedXContentRegistry,
            unsupportedFieldXContentBuilder.contentType().xContent(),
            BytesReference.bytes(emptySubQueriesXContentBuilder)
        );
        contentParser2.nextToken();

        expectThrows(ParsingException.class, () -> HybridQueryBuilder.fromXContent(contentParser2));
    }

    @SneakyThrows
    public void testToXContent_whenIncomingJsonIsCorrect_thenSuccessful() {
        setUpClusterService();
        HybridQueryBuilder queryBuilder = new HybridQueryBuilder();
        Index dummyIndex = new Index("dummy", "dummy");
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        KNNVectorFieldType mockKNNVectorField = mock(KNNVectorFieldType.class);
        KNNMappingConfig mockKNNMappingConfig = mock(KNNMappingConfig.class);
        when(mockKNNVectorField.getKnnMappingConfig()).thenReturn(mockKNNMappingConfig);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        when(mockKNNVectorField.getKnnMappingConfig().getDimension()).thenReturn(4);
        when(mockQueryShardContext.fieldMapper(eq(VECTOR_FIELD_NAME))).thenReturn(mockKNNVectorField);

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(VECTOR_FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .vectorSupplier(TEST_VECTOR_SUPPLIER)
            .filter(TEST_FILTER)
            .build();

        queryBuilder.add(neuralQueryBuilder);

        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT);
        queryBuilder.add(termSubQuery);

        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder = queryBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        Map<String, Object> out = xContentBuilderToMap(builder);

        Object outer = out.get(HybridQueryBuilder.NAME);
        if (!(outer instanceof Map)) {
            fail("hybrid does not map to nested object");
        }

        Map<String, Object> outerMap = (Map<String, Object>) outer;

        assertNotNull(outerMap);
        assertTrue(outerMap.containsKey("queries"));
        assertTrue(outerMap.get("queries") instanceof List);
        List listWithQueries = (List) outerMap.get("queries");
        assertEquals(2, listWithQueries.size());

        // verify neural search query
        Map<String, Object> vectorFieldInnerMap = getInnerMap(listWithQueries.get(0), NeuralQueryBuilder.NAME, VECTOR_FIELD_NAME);
        assertEquals(MODEL_ID, vectorFieldInnerMap.get(MODEL_ID_FIELD.getPreferredName()));
        assertEquals(QUERY_TEXT, vectorFieldInnerMap.get(QUERY_TEXT_FIELD.getPreferredName()));
        assertEquals(K, vectorFieldInnerMap.get(K_FIELD.getPreferredName()));
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        assertEquals(
            xContentBuilderToMap(TEST_FILTER.toXContent(xContentBuilder, EMPTY_PARAMS)),
            vectorFieldInnerMap.get(FILTER_FIELD.getPreferredName())
        );
        // verify term query
        Map<String, Object> termFieldInnerMap = getInnerMap(listWithQueries.get(1), TermQueryBuilder.NAME, TEXT_FIELD_NAME);
        assertEquals(TERM_QUERY_TEXT, termFieldInnerMap.get("value"));
    }

    @SneakyThrows
    public void testStreams_whenWrittingToStream_thenSuccessful() {
        setUpClusterService();
        HybridQueryBuilder original = new HybridQueryBuilder();
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(VECTOR_FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .build();

        original.add(neuralQueryBuilder);

        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT);
        original.add(termSubQuery);

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        original.writeTo(streamOutput);

        FilterStreamInput filterStreamInput = new NamedWriteableAwareStreamInput(
            streamOutput.bytes().streamInput(),
            new NamedWriteableRegistry(
                List.of(
                    new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new),
                    new NamedWriteableRegistry.Entry(QueryBuilder.class, NeuralQueryBuilder.NAME, NeuralQueryBuilder::new),
                    new NamedWriteableRegistry.Entry(QueryBuilder.class, HybridQueryBuilder.NAME, HybridQueryBuilder::new)
                )
            )
        );

        HybridQueryBuilder copy = new HybridQueryBuilder(filterStreamInput);
        assertEquals(original, copy);
    }

    public void testHashAndEquals_whenSameOrIdenticalObject_thenReturnEqual() {
        HybridQueryBuilder hybridQueryBuilderBaseline = new HybridQueryBuilder();
        hybridQueryBuilderBaseline.add(
            NeuralQueryBuilder.builder()
                .fieldName(VECTOR_FIELD_NAME)
                .queryText(QUERY_TEXT)
                .modelId(MODEL_ID)
                .k(K)
                .vectorSupplier(TEST_VECTOR_SUPPLIER)
                .filter(TEST_FILTER)
                .build()
        );
        hybridQueryBuilderBaseline.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT));

        HybridQueryBuilder hybridQueryBuilderBaselineCopy = new HybridQueryBuilder();
        hybridQueryBuilderBaselineCopy.add(
            NeuralQueryBuilder.builder()
                .fieldName(VECTOR_FIELD_NAME)
                .queryText(QUERY_TEXT)
                .modelId(MODEL_ID)
                .k(K)
                .vectorSupplier(TEST_VECTOR_SUPPLIER)
                .filter(TEST_FILTER)
                .build()
        );
        hybridQueryBuilderBaselineCopy.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT));

        assertEquals(hybridQueryBuilderBaseline, hybridQueryBuilderBaseline);
        assertEquals(hybridQueryBuilderBaseline.hashCode(), hybridQueryBuilderBaseline.hashCode());

        assertEquals(hybridQueryBuilderBaselineCopy, hybridQueryBuilderBaselineCopy);
        assertEquals(hybridQueryBuilderBaselineCopy.hashCode(), hybridQueryBuilderBaselineCopy.hashCode());
    }

    public void testHashAndEquals_whenSubQueriesDifferent_thenReturnNotEqual() {
        setUpClusterService();
        String modelId = "testModelId";
        String fieldName = "fieldTwo";
        String queryText = "query text";
        String termText = "another keyword";

        HybridQueryBuilder hybridQueryBuilderBaseline = new HybridQueryBuilder();
        hybridQueryBuilderBaseline.add(
            NeuralQueryBuilder.builder()
                .fieldName(VECTOR_FIELD_NAME)
                .queryText(QUERY_TEXT)
                .modelId(MODEL_ID)
                .k(K)
                .vectorSupplier(TEST_VECTOR_SUPPLIER)
                .filter(TEST_FILTER)
                .build()
        );
        hybridQueryBuilderBaseline.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT));

        HybridQueryBuilder hybridQueryBuilderOnlyOneSubQuery = new HybridQueryBuilder();
        hybridQueryBuilderOnlyOneSubQuery.add(
            NeuralQueryBuilder.builder()
                .fieldName(VECTOR_FIELD_NAME)
                .queryText(QUERY_TEXT)
                .modelId(MODEL_ID)
                .k(K)
                .vectorSupplier(TEST_VECTOR_SUPPLIER)
                .filter(TEST_FILTER)
                .build()
        );

        HybridQueryBuilder hybridQueryBuilderOnlyDifferentModelId = new HybridQueryBuilder();
        hybridQueryBuilderOnlyDifferentModelId.add(
            NeuralQueryBuilder.builder()
                .fieldName(VECTOR_FIELD_NAME)
                .queryText(QUERY_TEXT)
                .modelId(modelId)
                .k(K)
                .vectorSupplier(TEST_VECTOR_SUPPLIER)
                .filter(TEST_FILTER)
                .build()
        );
        hybridQueryBuilderBaseline.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT));

        HybridQueryBuilder hybridQueryBuilderOnlyDifferentFieldName = new HybridQueryBuilder();
        hybridQueryBuilderOnlyDifferentFieldName.add(
            NeuralQueryBuilder.builder()
                .fieldName(fieldName)
                .queryText(QUERY_TEXT)
                .modelId(MODEL_ID)
                .k(K)
                .vectorSupplier(TEST_VECTOR_SUPPLIER)
                .filter(TEST_FILTER)
                .build()
        );
        hybridQueryBuilderOnlyDifferentFieldName.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT));

        HybridQueryBuilder hybridQueryBuilderOnlyDifferentQuery = new HybridQueryBuilder();
        hybridQueryBuilderOnlyDifferentQuery.add(
            NeuralQueryBuilder.builder()
                .fieldName(VECTOR_FIELD_NAME)
                .queryText(queryText)
                .modelId(MODEL_ID)
                .k(K)
                .vectorSupplier(TEST_VECTOR_SUPPLIER)
                .filter(TEST_FILTER)
                .build()
        );
        hybridQueryBuilderOnlyDifferentQuery.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT));

        HybridQueryBuilder hybridQueryBuilderOnlyDifferentTermValue = new HybridQueryBuilder();
        hybridQueryBuilderOnlyDifferentTermValue.add(
            NeuralQueryBuilder.builder()
                .fieldName(VECTOR_FIELD_NAME)
                .queryText(QUERY_TEXT)
                .modelId(MODEL_ID)
                .k(K)
                .vectorSupplier(TEST_VECTOR_SUPPLIER)
                .filter(TEST_FILTER)
                .build()
        );
        hybridQueryBuilderOnlyDifferentTermValue.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, termText));

        assertNotEquals(hybridQueryBuilderBaseline, hybridQueryBuilderOnlyOneSubQuery);
        assertNotEquals(hybridQueryBuilderBaseline.hashCode(), hybridQueryBuilderOnlyOneSubQuery.hashCode());

        assertNotEquals(hybridQueryBuilderBaseline, hybridQueryBuilderOnlyDifferentModelId);
        assertNotEquals(hybridQueryBuilderBaseline.hashCode(), hybridQueryBuilderOnlyDifferentModelId.hashCode());

        assertNotEquals(hybridQueryBuilderBaseline, hybridQueryBuilderOnlyDifferentFieldName);
        assertNotEquals(hybridQueryBuilderBaseline.hashCode(), hybridQueryBuilderOnlyDifferentFieldName.hashCode());

        assertNotEquals(hybridQueryBuilderBaseline, hybridQueryBuilderOnlyDifferentQuery);
        assertNotEquals(hybridQueryBuilderBaseline.hashCode(), hybridQueryBuilderOnlyDifferentQuery.hashCode());

        assertNotEquals(hybridQueryBuilderBaseline, hybridQueryBuilderOnlyDifferentTermValue);
        assertNotEquals(hybridQueryBuilderBaseline.hashCode(), hybridQueryBuilderOnlyDifferentTermValue.hashCode());
    }

    @SneakyThrows
    public void testRewrite_whenMultipleSubQueries_thenReturnBuilderForEachSubQuery() {
        setUpClusterService();
        HybridQueryBuilder queryBuilder = new HybridQueryBuilder();
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(VECTOR_FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .vectorSupplier(TEST_VECTOR_SUPPLIER)
            .build();

        queryBuilder.add(neuralQueryBuilder);

        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT);
        queryBuilder.add(termSubQuery);

        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        KNNVectorFieldType mockKNNVectorField = mock(KNNVectorFieldType.class);
        KNNMappingConfig mockKNNMappingConfig = mock(KNNMappingConfig.class);
        when(mockKNNVectorField.getKnnMappingConfig()).thenReturn(mockKNNMappingConfig);
        Index dummyIndex = new Index("dummy", "dummy");
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        when(mockKNNVectorField.getKnnMappingConfig().getDimension()).thenReturn(4);
        when(mockQueryShardContext.fieldMapper(eq(VECTOR_FIELD_NAME))).thenReturn(mockKNNVectorField);

        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        QueryBuilder queryBuilderAfterRewrite = queryBuilder.doRewrite(mockQueryShardContext);
        assertTrue(queryBuilderAfterRewrite instanceof HybridQueryBuilder);
        HybridQueryBuilder hybridQueryBuilder = (HybridQueryBuilder) queryBuilderAfterRewrite;
        assertNotNull(hybridQueryBuilder.queries());
        assertEquals(2, hybridQueryBuilder.queries().size());
        List<QueryBuilder> queryBuilders = hybridQueryBuilder.queries();
        // verify each sub-query builder
        assertTrue(queryBuilders.get(0) instanceof NeuralKNNQueryBuilder);
        NeuralKNNQueryBuilder neuralKNNQueryBuilder = (NeuralKNNQueryBuilder) queryBuilders.get(0);
        assertEquals(neuralQueryBuilder.fieldName(), neuralKNNQueryBuilder.fieldName());
        assertEquals((int) neuralQueryBuilder.k(), neuralKNNQueryBuilder.k());
        assertTrue(queryBuilders.get(1) instanceof TermQueryBuilder);
        TermQueryBuilder termQueryBuilder = (TermQueryBuilder) queryBuilders.get(1);
        assertEquals(termSubQuery.fieldName(), termQueryBuilder.fieldName());
        assertEquals(termSubQuery.value(), termQueryBuilder.value());
    }

    /**
     * Tests query with boost:
     * {
     *     "query": {
     *         "hybrid": {
     *              "queries": [
     *                  {
     *                      "term": {
     *                          "text": "keyword"
     *                      }
     *                  },
     *                  {
     *                      "term": {
     *                          "text": "keyword"
     *                       }
     *                  }
     *              ],
     *              "boost" : 2.0
     *          }
     *      }
     * }
     */
    @SneakyThrows
    public void testBoost_whenNonDefaultBoostSet_thenFail() {
        XContentBuilder xContentBuilderWithNonDefaultBoost = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("queries")
            .startObject()
            .startObject("term")
            .field(TEXT_FIELD_NAME, RandomizedTest.randomAsciiAlphanumOfLength(10))
            .endObject()
            .endObject()
            .startObject()
            .startObject("term")
            .field(TEXT_FIELD_NAME, RandomizedTest.randomAsciiAlphanumOfLength(10))
            .endObject()
            .endObject()
            .endArray()
            .field("boost", 2.0f)
            .endObject();

        NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(
            List.of(
                new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField(TermQueryBuilder.NAME), TermQueryBuilder::fromXContent),
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(HybridQueryBuilder.NAME),
                    HybridQueryBuilder::fromXContent
                )
            )
        );
        XContentParser contentParser = createParser(
            namedXContentRegistry,
            xContentBuilderWithNonDefaultBoost.contentType().xContent(),
            BytesReference.bytes(xContentBuilderWithNonDefaultBoost)
        );
        contentParser.nextToken();

        ParsingException exception = expectThrows(ParsingException.class, () -> HybridQueryBuilder.fromXContent(contentParser));
        assertThat(exception.getMessage(), containsString("query does not support [boost]"));
    }

    @SneakyThrows
    public void testBoost_whenDefaultBoostSet_thenBuildSuccessfully() {
        setUpClusterService();
        // create query with 6 sub-queries, which is more than current max allowed
        XContentBuilder xContentBuilderWithNonDefaultBoost = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("queries")
            .startObject()
            .startObject("term")
            .field(TEXT_FIELD_NAME, RandomizedTest.randomAsciiAlphanumOfLength(10))
            .endObject()
            .endObject()
            .startObject()
            .startObject("term")
            .field(TEXT_FIELD_NAME, RandomizedTest.randomAsciiAlphanumOfLength(10))
            .endObject()
            .endObject()
            .endArray()
            .field("boost", DEFAULT_BOOST)
            .endObject();

        NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(
            List.of(
                new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField(TermQueryBuilder.NAME), TermQueryBuilder::fromXContent),
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(HybridQueryBuilder.NAME),
                    HybridQueryBuilder::fromXContent
                )
            )
        );
        XContentParser contentParser = createParser(
            namedXContentRegistry,
            xContentBuilderWithNonDefaultBoost.contentType().xContent(),
            BytesReference.bytes(xContentBuilderWithNonDefaultBoost)
        );
        contentParser.nextToken();

        HybridQueryBuilder hybridQueryBuilder = HybridQueryBuilder.fromXContent(contentParser);
        assertNotNull(hybridQueryBuilder);
    }

    @SneakyThrows
    public void testBuild_whenValidParameters_thenCreateQuery() {
        String queryText = "test query";
        String modelId = "test_model";
        String fieldName = "rank_features";

        // Create mock context
        QueryShardContext context = mock(QueryShardContext.class);
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(context.fieldMapper(fieldName)).thenReturn(fieldType);
        when(fieldType.typeName()).thenReturn("rank_features");
        IndexMetadata indexMetadata = getIndexMetadata();
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, Integer.toString(3)).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        when(context.getIndexSettings()).thenReturn(indexSettings);

        // Create HybridQueryBuilder instance (no spy since it's final)
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder();
        neuralSparseQueryBuilder.fieldName(fieldName)
            .queryText(queryText)
            .modelId(modelId)
            .queryTokensSupplier(() -> Map.of("token1", 1.0f, "token2", 0.5f));
        HybridQueryBuilder builder = new HybridQueryBuilder().add(neuralSparseQueryBuilder);
        builder.paginationDepth(10);

        // Build query
        Query query = builder.toQuery(context);

        // Verify
        assertNotNull("Query should not be null", query);
        assertTrue("Should be HybridQuery", query instanceof HybridQuery);
    }

    @SneakyThrows
    public void testDoEquals_whenSameParameters_thenEqual() {
        setUpClusterService();
        // Create neural queries
        NeuralQueryBuilder neuralQueryBuilder1 = NeuralQueryBuilder.builder()
            .fieldName("test")
            .queryText("test")
            .modelId("test_model")
            .build();

        NeuralQueryBuilder neuralQueryBuilder2 = NeuralQueryBuilder.builder()
            .fieldName("test")
            .queryText("test")
            .modelId("test_model")
            .build();

        // Create neural sparse queries with queryTokensSupplier
        NeuralSparseQueryBuilder neuralSparseQueryBuilder1 = new NeuralSparseQueryBuilder().fieldName("test_field")
            .queryText("test")
            .modelId("test_model")
            .queryTokensSupplier(() -> Map.of("token1", 1.0f));

        NeuralSparseQueryBuilder neuralSparseQueryBuilder2 = new NeuralSparseQueryBuilder().fieldName("test_field")
            .queryText("test")
            .modelId("test_model")
            .queryTokensSupplier(() -> Map.of("token1", 1.0f));

        // Create builders
        HybridQueryBuilder builder1 = new HybridQueryBuilder().add(neuralQueryBuilder1).add(neuralSparseQueryBuilder1);

        HybridQueryBuilder builder2 = new HybridQueryBuilder().add(neuralQueryBuilder2).add(neuralSparseQueryBuilder2);

        // Verify
        assertTrue("Builders should be equal", builder1.equals(builder2));
        assertEquals("Hash codes should match", builder1.hashCode(), builder2.hashCode());
    }

    public void testValidate_whenInvalidParameters_thenThrowException() {
        // Test null query builder
        HybridQueryBuilder builderWithNull = new HybridQueryBuilder();
        IllegalArgumentException nullException = assertThrows(IllegalArgumentException.class, () -> builderWithNull.add(null));
        assertEquals("inner hybrid query clause cannot be null", nullException.getMessage());
    }

    public void testVisit() {
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder().add(
            NeuralQueryBuilder.builder().fieldName("test").queryText("test").build()
        ).add(new NeuralSparseQueryBuilder());
        List<QueryBuilder> visitedQueries = new ArrayList<>();
        hybridQueryBuilder.visit(createTestVisitor(visitedQueries));
        assertEquals(3, visitedQueries.size());
    }

    private Map<String, Object> getInnerMap(Object innerObject, String queryName, String fieldName) {
        if (!(innerObject instanceof Map)) {
            fail("field name does not map to nested object");
        }
        Map<String, Object> secondInnerMap = (Map<String, Object>) innerObject;
        assertTrue(secondInnerMap.containsKey(queryName));
        assertTrue(secondInnerMap.get(queryName) instanceof Map);
        Map<String, Object> neuralInnerMap = (Map<String, Object>) secondInnerMap.get(queryName);
        assertTrue(neuralInnerMap.containsKey(fieldName));
        assertTrue(neuralInnerMap.get(fieldName) instanceof Map);
        Map<String, Object> vectorFieldInnerMap = (Map<String, Object>) neuralInnerMap.get(fieldName);
        return vectorFieldInnerMap;
    }

    private void setUpClusterService() {
        ClusterService clusterService = NeuralSearchClusterTestUtils.mockClusterService(Version.CURRENT);
        NeuralSearchClusterUtil.instance().initialize(clusterService);
    }

    private void initKNNSettings() {
        Set<Setting<?>> defaultClusterSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        defaultClusterSettings.addAll(
            KNNSettings.state()
                .getSettings()
                .stream()
                .filter(s -> s.getProperties().contains(Setting.Property.NodeScope))
                .collect(Collectors.toList())
        );
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY, defaultClusterSettings));
        KNNSettings.state().setClusterService(clusterService);
    }

    private static IndexMetadata getIndexMetadata() {
        Map<String, String> remoteCustomData = Map.of(
            RemoteStoreEnums.PathType.NAME,
            HASHED_PREFIX.name(),
            RemoteStoreEnums.PathHashAlgorithm.NAME,
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64.name(),
            IndexMetadata.TRANSLOG_METADATA_KEY,
            "false"
        );
        Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder("test").settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putCustom(IndexMetadata.REMOTE_STORE_CUSTOM_KEY, remoteCustomData)
            .build();
        return indexMetadata;
    }
}
