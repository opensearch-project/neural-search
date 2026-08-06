/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.index.query.AbstractQueryBuilder.BOOST_FIELD;
import static org.opensearch.index.query.AbstractQueryBuilder.DEFAULT_BOOST;
import static org.opensearch.index.remote.RemoteStoreEnums.PathType.HASHED_PREFIX;
import static org.opensearch.knn.index.query.KNNQueryBuilder.FILTER_FIELD;
import static org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils.setUpClusterService;
import static org.opensearch.neuralsearch.util.TestUtils.xContentBuilderToMap;
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.K_FIELD;
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.MODEL_ID_FIELD;
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.QUERY_TEXT_FIELD;

import java.io.IOException;
import java.util.HashMap;
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
import org.apache.lucene.search.join.ScoreMode;
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
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.InnerHitContextBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryCoordinatorContext;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.builder.SearchSourceBuilder;
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

import com.carrotsearch.randomizedtesting.RandomizedTest;

import lombok.SneakyThrows;
import org.opensearch.neuralsearch.util.TestUtils;

public class HybridQueryBuilderTests extends OpenSearchQueryTestCase {
    static final String VECTOR_FIELD_NAME = "vectorField";
    static final String TEXT_FIELD_NAME = "field";
    static final String QUERY_TEXT = "Hello world!";
    static final String TERM_QUERY_TEXT = "keyword";
    static final String FILTER_TERM_QUERY_TEXT = "filterKeyword";
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
        TestUtils.initializeEventStatsManager();
        initKNNSettings();
        TestUtils.initializeEventStatsManager();
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
        setUpClusterService(Version.V_3_0_0);
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
        setUpClusterService(Version.V_3_0_0);
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
        setUpClusterService(Version.V_3_0_0);
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
        setUpClusterService(Version.V_3_0_0);
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

    // ---- resolver (fused) mode: `fusion` parameter parse / round-trip / validation (PR1, execution inert) ----

    private XContentParser fusionTestParser(XContentBuilder xContentBuilder) throws IOException {
        NamedXContentRegistry registry = new NamedXContentRegistry(
            List.of(
                new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField(TermQueryBuilder.NAME), TermQueryBuilder::fromXContent),
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(HybridQueryBuilder.NAME),
                    HybridQueryBuilder::fromXContent
                )
            )
        );
        XContentParser parser = createParser(registry, xContentBuilder.contentType().xContent(), BytesReference.bytes(xContentBuilder));
        parser.nextToken();
        return parser;
    }

    private XContentBuilder hybridWithOneTermQuery() throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startArray("queries")
            .startObject()
            .startObject(TermQueryBuilder.NAME)
            .field(TEXT_FIELD_NAME, TERM_QUERY_TEXT)
            .endObject()
            .endObject()
            .endArray();
    }

    @SneakyThrows
    public void testFromXContent_whenFusionObject_thenParsedAndInert() {
        setUpClusterService();
        XContentBuilder xContentBuilder = hybridWithOneTermQuery().startObject("fusion")
            .startObject("normalization")
            .field("technique", "l2")
            .endObject()
            .endObject()
            .endObject();

        HybridQueryBuilder builder = HybridQueryBuilder.fromXContent(fusionTestParser(xContentBuilder));
        assertNotNull("fusion block must be parsed and retained", builder.fusion());
        assertTrue(builder.fusion().containsKey("normalization"));
        // doRewrite on a shard context (no coordinator) is a no-op — the coordinator self-erase is the sole entry.
        QueryRewriteContext shardRewrite = mock(QueryRewriteContext.class);
        when(shardRewrite.convertToCoordinatorContext()).thenReturn(null);
        assertSame(builder, builder.doRewrite(shardRewrite));
    }

    @SneakyThrows
    public void testFromXContent_whenFusionStringPipeline_thenNormalizedToSourceMap() {
        setUpClusterService();
        XContentBuilder xContentBuilder = hybridWithOneTermQuery().field("fusion", "pipeline").endObject();
        HybridQueryBuilder builder = HybridQueryBuilder.fromXContent(fusionTestParser(xContentBuilder));
        assertNotNull(builder.fusion());
        assertEquals("pipeline", builder.fusion().get("source"));
    }

    @SneakyThrows
    public void testFromXContent_whenNoFusion_thenClassicUnchanged() {
        setUpClusterService();
        XContentBuilder xContentBuilder = hybridWithOneTermQuery().endObject();
        HybridQueryBuilder builder = HybridQueryBuilder.fromXContent(fusionTestParser(xContentBuilder));
        assertNull("absent fusion => classic path", builder.fusion());
    }

    @SneakyThrows
    public void testFromXContent_whenFusionSourcePlusInlineTechniques_then400() {
        setUpClusterService();
        XContentBuilder xContentBuilder = hybridWithOneTermQuery().startObject("fusion")
            .field("source", "pipeline")
            .startObject("normalization")
            .field("technique", "l2")
            .endObject()
            .endObject()
            .endObject();
        ParsingException e = expectThrows(ParsingException.class, () -> HybridQueryBuilder.fromXContent(fusionTestParser(xContentBuilder)));
        assertThat(e.getMessage(), containsString("cannot combine"));
    }

    @SneakyThrows
    public void testFromXContent_whenFusionUnknownKey_then400() {
        setUpClusterService();
        XContentBuilder xContentBuilder = hybridWithOneTermQuery().startObject("fusion").field("bogus", "x").endObject().endObject();
        ParsingException e = expectThrows(ParsingException.class, () -> HybridQueryBuilder.fromXContent(fusionTestParser(xContentBuilder)));
        assertThat(e.getMessage(), containsString("unknown key"));
    }

    @SneakyThrows
    public void testFromXContent_whenFusionWindowSizeNonPositive_then400() {
        setUpClusterService();
        XContentBuilder xContentBuilder = hybridWithOneTermQuery().startObject("fusion").field("window_size", 0).endObject().endObject();
        ParsingException e = expectThrows(ParsingException.class, () -> HybridQueryBuilder.fromXContent(fusionTestParser(xContentBuilder)));
        assertThat(e.getMessage(), containsString("greater than 0"));
    }

    @SneakyThrows
    public void testFromXContent_whenFusionWithPaginationDepth_then400() {
        setUpClusterService();
        XContentBuilder xContentBuilder = hybridWithOneTermQuery().field("pagination_depth", 10)
            .startObject("fusion")
            .field("window_size", 50)
            .endObject()
            .endObject();
        ParsingException e = expectThrows(ParsingException.class, () -> HybridQueryBuilder.fromXContent(fusionTestParser(xContentBuilder)));
        assertThat(e.getMessage(), containsString("pagination_depth"));
    }

    @SneakyThrows
    public void testToXContent_whenFusionPresent_thenEmitsFusionField() {
        setUpClusterService();
        HybridQueryBuilder original = new HybridQueryBuilder();
        original.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT));
        original.fusion(new HashMap<>()); // empty fusion:{} must be emitted (and stay non-null)

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        original.toXContent(xContentBuilder, EMPTY_PARAMS);
        Map<String, Object> asMap = xContentBuilderToMap(xContentBuilder);
        @SuppressWarnings("unchecked")
        Map<String, Object> hybrid = (Map<String, Object>) asMap.get(HybridQueryBuilder.NAME);
        assertTrue("toXContent must emit the fusion field", hybrid.containsKey("fusion"));
    }

    @SneakyThrows
    public void testFromXContent_whenEmptyFusionObject_thenNonNullAndInert() {
        // fusion:{} — presence enables the resolver; must survive parse as a non-null (empty) map, not collapse to null.
        setUpClusterService();
        XContentBuilder xContentBuilder = hybridWithOneTermQuery().startObject("fusion").endObject().endObject();
        HybridQueryBuilder reparsed = HybridQueryBuilder.fromXContent(fusionTestParser(xContentBuilder));
        assertNotNull("fusion:{} must survive as non-null", reparsed.fusion());
        assertTrue(reparsed.fusion().isEmpty());
    }

    // ---- PR3: wire round-trip + coordinator self-erase lifecycle ----

    @SneakyThrows
    public void testSerialization_whenFusionPresent_thenSurvivesNonNull() {
        // Wire round-trip on a fused cluster (V_3_8_0): fusion:{} must survive as a non-null empty map, else the
        // resolver silently flips off on the receiving node.
        setUpClusterService(Version.V_3_8_0);
        HybridQueryBuilder original = new HybridQueryBuilder();
        original.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT));
        original.fusion(new HashMap<>());

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        original.writeTo(streamOutput);
        FilterStreamInput in = new NamedWriteableAwareStreamInput(
            streamOutput.bytes().streamInput(),
            new NamedWriteableRegistry(
                List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new))
            )
        );
        HybridQueryBuilder copy = new HybridQueryBuilder(in);
        assertNotNull("fusion must survive the wire as non-null", copy.fusion());
        assertTrue(copy.fusion().isEmpty());
        assertEquals(original, copy);
    }

    @SneakyThrows
    public void testSerialization_whenNoFusion_thenClassicWireForm() {
        // Absence of fusion writes only a false boolean → equal classic builder round-trips unchanged.
        setUpClusterService(Version.V_3_8_0);
        HybridQueryBuilder original = new HybridQueryBuilder();
        original.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT));

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        original.writeTo(streamOutput);
        FilterStreamInput in = new NamedWriteableAwareStreamInput(
            streamOutput.bytes().streamInput(),
            new NamedWriteableRegistry(
                List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new))
            )
        );
        HybridQueryBuilder copy = new HybridQueryBuilder(in);
        assertNull(copy.fusion());
        assertEquals(original, copy);
    }

    @SneakyThrows
    public void testSerialization_whenPeerStreamOnPreFusionVersion_thenFusionGatedByStreamVersion() {
        // Mixed-version wire safety (mirrors AOSS CR-290524846): even on a fused-capable cluster singleton, a stream
        // pinned to a pre-fusion peer version must NOT read/write the fusion field — the gate keys off
        // StreamInput/StreamOutput#getVersion(), not the cluster-min-version singleton. An old peer that mistakenly
        // wrote the field would corrupt the classic wire form on the receiving node.
        setUpClusterService(Version.V_3_8_0);
        HybridQueryBuilder original = new HybridQueryBuilder();
        original.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT));
        original.fusion(new HashMap<>());

        // A peer negotiated below the fused-mode minimum (V_3_8_0).
        Version oldPeer = Version.V_3_7_0;
        BytesStreamOutput streamOutput = new BytesStreamOutput();
        streamOutput.setVersion(oldPeer);
        original.writeTo(streamOutput);

        FilterStreamInput in = new NamedWriteableAwareStreamInput(
            streamOutput.bytes().streamInput(),
            new NamedWriteableRegistry(
                List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new))
            )
        );
        in.setVersion(oldPeer);
        HybridQueryBuilder copy = new HybridQueryBuilder(in);

        assertNull("fusion must not cross the wire to a pre-fusion peer", copy.fusion());
    }

    @SneakyThrows
    public void testSerialization_whenPeerStreamOnFusedVersion_thenFusionSurvivesRegardlessOfSingleton() {
        // Symmetric to the above: a stream pinned to a fused-capable version round-trips fusion even when the cluster
        // singleton would report an older min version, proving the wire format follows the negotiated stream version.
        setUpClusterService(Version.V_3_0_0);
        HybridQueryBuilder original = new HybridQueryBuilder();
        original.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT));
        original.fusion(new HashMap<>());

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        streamOutput.setVersion(Version.V_3_8_0);
        original.writeTo(streamOutput);

        FilterStreamInput in = new NamedWriteableAwareStreamInput(
            streamOutput.bytes().streamInput(),
            new NamedWriteableRegistry(
                List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new))
            )
        );
        in.setVersion(Version.V_3_8_0);
        HybridQueryBuilder copy = new HybridQueryBuilder(in);

        assertNotNull("fusion must survive a fused-version stream", copy.fusion());
        assertTrue(copy.fusion().isEmpty());
    }

    @SneakyThrows
    public void testDoToQuery_whenFusedReachesShard_thenThrows() {
        // Safety net: a fused builder must self-erase at the coordinator; if it reaches a shard's doToQuery, fail loudly.
        setUpClusterService();
        HybridQueryBuilder builder = new HybridQueryBuilder();
        builder.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT));
        builder.fusion(new HashMap<>());
        QueryShardContext shardContext = mock(QueryShardContext.class);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> builder.doToQuery(shardContext));
        assertThat(e.getMessage(), containsString("must not reach a shard"));
    }

    @SneakyThrows
    public void testDoRewrite_whenFusedOnShardContext_thenNoOp() {
        // On a shard (convertToCoordinatorContext == null), fused doRewrite is a no-op — it returns itself and waits.
        setUpClusterService();
        HybridQueryBuilder builder = new HybridQueryBuilder();
        builder.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT));
        builder.fusion(new HashMap<>());
        QueryRewriteContext shardRewrite = mock(QueryRewriteContext.class);
        when(shardRewrite.convertToCoordinatorContext()).thenReturn(null);
        assertSame(builder, builder.doRewrite(shardRewrite));
    }

    private HybridQueryBuilder fusedBuilder(Map<String, Object> fusion) {
        HybridQueryBuilder builder = new HybridQueryBuilder();
        builder.add(new MatchQueryBuilder(TEXT_FIELD_NAME, "hello"));
        builder.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT));
        builder.fusion(fusion);
        return builder;
    }

    /** A coordinator rewrite context whose SearchRequest wraps the given builder as the top-level query. */
    private QueryCoordinatorContext coordinatorContextFor(HybridQueryBuilder builder) {
        SearchRequest searchRequest = new SearchRequest("test-index").source(new SearchSourceBuilder().query(builder));
        QueryCoordinatorContext ctx = mock(QueryCoordinatorContext.class);
        when(ctx.convertToCoordinatorContext()).thenReturn(ctx);
        when(ctx.getSearchRequest()).thenReturn(searchRequest);
        return ctx;
    }

    // NOTE: the "no resolvable config → fail fast" path routes through FusionConfigResolver's index-default lookup,
    // which needs real cluster-state metadata (see FusionConfigResolverTests); it is covered by the integration tests.

    @SneakyThrows
    public void testDoRewriteFused_whenUnsupportedTechnique_thenFailsFast() {
        setUpClusterService();
        // inline l2 normalization resolves fine, but only min_max is wired today → fail fast at rewrite.
        HybridQueryBuilder builder = fusedBuilder(new HashMap<>(Map.of("normalization", Map.of("technique", "l2"))));
        QueryCoordinatorContext ctx = coordinatorContextFor(builder);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.doRewrite(ctx));
        assertThat(e.getMessage(), containsString("currently supports only"));
    }

    @SneakyThrows
    public void testDoRewriteFused_whenRrf_thenFailsFast() {
        setUpClusterService();
        HybridQueryBuilder builder = fusedBuilder(
            new HashMap<>(Map.of("combination", Map.of("technique", "rrf", "parameters", Map.of("rank_constant", 60))))
        );
        QueryCoordinatorContext ctx = coordinatorContextFor(builder);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.doRewrite(ctx));
        assertThat(e.getMessage(), containsString("currently supports only"));
    }

    @SneakyThrows
    public void testDoRewriteFused_whenSupportedInlineConfig_thenRegistersAsyncAndReturnsMarker() {
        initClusterUtilWithMaxResultWindow(10000);
        // inline min_max + arithmetic_mean resolves and is supported → registers the leg MultiSearch async action and
        // returns a distinct marker builder (round 1). The marker still carries the fusion block.
        HybridQueryBuilder builder = fusedBuilder(
            new HashMap<>(Map.of("normalization", Map.of("technique", "min_max"), "combination", Map.of("technique", "arithmetic_mean")))
        );
        QueryCoordinatorContext ctx = coordinatorContextFor(builder);
        java.util.concurrent.atomic.AtomicInteger asyncRegistered = new java.util.concurrent.atomic.AtomicInteger();
        doAnswer(invocation -> {
            asyncRegistered.incrementAndGet();
            return null;
        }).when(ctx).registerAsyncAction(org.mockito.ArgumentMatchers.any());

        QueryBuilder rewritten = builder.doRewrite(ctx);

        assertEquals("exactly one leg MultiSearch async action registered", 1, asyncRegistered.get());
        assertTrue(rewritten instanceof HybridQueryBuilder);
        assertNotSame("round 1 returns a marker, not the original", builder, rewritten);
        assertNotNull("marker carries the fusion block", ((HybridQueryBuilder) rewritten).fusion());
    }

    @SneakyThrows
    public void testDoRewriteFused_whenRound2SupplierUnset_thenMarkerWaits() {
        // Round-2 path: the marker returned by round 1 holds a supplier that is empty until the async action completes;
        // rewriting it again while the supplier is empty returns itself (waits) rather than erroring.
        initClusterUtilWithMaxResultWindow(10000);
        HybridQueryBuilder builder = fusedBuilder(
            new HashMap<>(Map.of("normalization", Map.of("technique", "min_max"), "combination", Map.of("technique", "arithmetic_mean")))
        );
        QueryCoordinatorContext ctx = coordinatorContextFor(builder);
        doAnswer(invocation -> null).when(ctx).registerAsyncAction(org.mockito.ArgumentMatchers.any());
        QueryBuilder marker = builder.doRewrite(ctx);
        QueryBuilder round2 = marker.rewrite(ctx);
        assertSame("supplier empty → marker waits (returns itself)", marker, round2);
    }

    @SneakyThrows
    public void testDoRewriteFused_whenWindowSizeInFusion_thenSupportedAndRegisters() {
        // A fusion block carrying window_size + supported techniques resolves and registers without error (the leg
        // size wiring itself is asserted in HybridFusionOrchestratorTests#testBuildLegMultiSearch_perLegSourceShape).
        initClusterUtilWithMaxResultWindow(10000);
        HybridQueryBuilder withWindow = fusedBuilder(
            new HashMap<>(Map.of("normalization", Map.of("technique", "min_max"), "window_size", 25))
        );
        QueryCoordinatorContext ctx = coordinatorContextFor(withWindow);
        java.util.concurrent.atomic.AtomicInteger asyncRegistered = new java.util.concurrent.atomic.AtomicInteger();
        doAnswer(invocation -> {
            asyncRegistered.incrementAndGet();
            return null;
        }).when(ctx).registerAsyncAction(org.mockito.ArgumentMatchers.any());
        withWindow.doRewrite(ctx);
        assertEquals(1, asyncRegistered.get());
    }

    /** Initialize NeuralSearchClusterUtil with a cluster state that resolves NO pipeline (empty metadata, no default). */
    private void initClusterUtilWithNoPipeline() {
        org.opensearch.cluster.metadata.Metadata metadata = mock(org.opensearch.cluster.metadata.Metadata.class);
        org.opensearch.cluster.ClusterState clusterState = mock(org.opensearch.cluster.ClusterState.class);
        org.opensearch.cluster.service.ClusterService clusterService = mock(org.opensearch.cluster.service.ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterState.getMetadata()).thenReturn(metadata);
        org.opensearch.cluster.metadata.IndexNameExpressionResolver resolver = mock(
            org.opensearch.cluster.metadata.IndexNameExpressionResolver.class
        );
        // No concrete indices → resolveIndexDefaultPipelineId returns null → resolve() returns null.
        when(resolver.concreteIndices(any(org.opensearch.cluster.ClusterState.class), any(org.opensearch.action.IndicesRequest.class)))
            .thenReturn(new org.opensearch.core.index.Index[0]);
        org.opensearch.neuralsearch.util.NeuralSearchClusterUtil.instance().initialize(clusterService, resolver);
    }

    @SneakyThrows
    public void testDoRewriteFused_whenNoResolvableConfig_thenFailsFast() {
        // fusion:{source: pipeline} but the cluster resolves no pipeline / no index default → fail fast at rewrite.
        initClusterUtilWithNoPipeline();
        HybridQueryBuilder builder = fusedBuilder(new HashMap<>(Map.of("source", "pipeline")));
        QueryCoordinatorContext ctx = coordinatorContextFor(builder);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.doRewrite(ctx));
        assertThat(e.getMessage(), containsString("requires a normalization or score-ranker processor"));
    }

    @SneakyThrows
    public void testDoRewriteFused_whenSearchRequestNotResolvable_thenReturnsThis() {
        // If the coordinator context's request is not a SearchRequest, doRewriteFused is a no-op (returns itself).
        setUpClusterService();
        HybridQueryBuilder builder = fusedBuilder(new HashMap<>(Map.of("normalization", Map.of("technique", "min_max"))));
        QueryCoordinatorContext ctx = mock(QueryCoordinatorContext.class);
        when(ctx.convertToCoordinatorContext()).thenReturn(ctx);
        when(ctx.getSearchRequest()).thenReturn(mock(org.opensearch.action.IndicesRequest.class));
        assertSame(builder, builder.doRewrite(ctx));
    }

    /** A MultiSearch item wrapping a SearchResponse whose hits carry the given (_id -> score) pairs. */
    private org.opensearch.action.search.MultiSearchResponse.Item legItem(Map<String, Float> idToScore) {
        org.opensearch.search.SearchHit[] hits = new org.opensearch.search.SearchHit[idToScore.size()];
        int i = 0;
        for (Map.Entry<String, Float> e : idToScore.entrySet()) {
            org.opensearch.search.SearchHit hit = new org.opensearch.search.SearchHit(i, e.getKey(), Map.of(), Map.of());
            hit.score(e.getValue());
            hits[i++] = hit;
        }
        org.opensearch.search.SearchHits searchHits = new org.opensearch.search.SearchHits(
            hits,
            new org.apache.lucene.search.TotalHits(hits.length, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO),
            1.0f
        );
        org.opensearch.action.search.SearchResponseSections sections = new org.opensearch.action.search.SearchResponseSections(
            searchHits,
            null,
            null,
            false,
            false,
            null,
            0
        );
        org.opensearch.action.search.SearchResponse response = new org.opensearch.action.search.SearchResponse(
            sections,
            null,
            1,
            1,
            0,
            10,
            null,
            null
        );
        return new org.opensearch.action.search.MultiSearchResponse.Item(response, null);
    }

    @SneakyThrows
    public void testDoRewriteFused_endToEnd_asyncActionProducesFusedQuery() {
        // Drives the full round-1 → round-2 lifecycle: capture the registered async action, run it with a mock client
        // that returns a fake per-leg MultiSearchResponse, and confirm the marker's supplier then yields the fused
        // HybridFusionQuery (exercises the registerAsyncAction lambda body: buildLegMultiSearch + buildFusedQuery).
        initClusterUtilWithMaxResultWindow(10000);
        HybridQueryBuilder builder = fusedBuilder(
            new HashMap<>(Map.of("normalization", Map.of("technique", "min_max"), "combination", Map.of("technique", "arithmetic_mean")))
        );
        QueryCoordinatorContext ctx = coordinatorContextFor(builder);

        // Capture the async action registered in round 1.
        java.util.concurrent.atomic.AtomicReference<
            java.util.function.BiConsumer<org.opensearch.transport.client.Client, org.opensearch.core.action.ActionListener<?>>> captured =
                new java.util.concurrent.atomic.AtomicReference<>();
        doAnswer(invocation -> {
            captured.set(invocation.getArgument(0));
            return null;
        }).when(ctx).registerAsyncAction(org.mockito.ArgumentMatchers.any());

        QueryBuilder marker = builder.doRewrite(ctx);
        assertTrue(marker instanceof HybridQueryBuilder);
        assertNotNull("an async action must have been registered", captured.get());

        // Mock client: multiSearch(request, listener) → return a two-leg fake response.
        org.opensearch.transport.client.Client client = mock(org.opensearch.transport.client.Client.class);
        org.opensearch.action.search.MultiSearchResponse msResponse = new org.opensearch.action.search.MultiSearchResponse(
            new org.opensearch.action.search.MultiSearchResponse.Item[] {
                legItem(Map.of("1", 0.9f, "2", 0.5f)),
                legItem(Map.of("2", 0.8f, "3", 0.4f)) },
            10L
        );
        doAnswer(invocation -> {
            org.opensearch.core.action.ActionListener<org.opensearch.action.search.MultiSearchResponse> l = invocation.getArgument(1);
            l.onResponse(msResponse);
            return null;
        }).when(client).multiSearch(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());

        // Run the captured async action; its inner listener sets the fused query on the marker's SetOnce.
        java.util.concurrent.atomic.AtomicBoolean done = new java.util.concurrent.atomic.AtomicBoolean();
        captured.get().accept(client, org.opensearch.core.action.ActionListener.wrap(r -> done.set(true), e -> fail(e.getMessage())));
        assertTrue("async action should complete", done.get());

        // Round 2: rewriting the marker now yields the self-erased fused query.
        QueryBuilder fused = marker.rewrite(ctx);
        assertTrue("round 2 returns the fused HybridFusionQuery", fused instanceof HybridFusionQuery);
    }

    /** Initialize NeuralSearchClusterUtil so getIndexMetadataList returns one index with the given max_result_window. */
    private void initClusterUtilWithMaxResultWindow(int maxResultWindow) {
        org.opensearch.cluster.metadata.Metadata metadata = mock(org.opensearch.cluster.metadata.Metadata.class);
        org.opensearch.cluster.ClusterState clusterState = mock(org.opensearch.cluster.ClusterState.class);
        org.opensearch.cluster.service.ClusterService clusterService = mock(org.opensearch.cluster.service.ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterState.getMetadata()).thenReturn(metadata);
        when(metadata.custom(org.opensearch.search.pipeline.SearchPipelineMetadata.TYPE)).thenReturn(
            new org.opensearch.search.pipeline.SearchPipelineMetadata(Map.of())
        );
        org.opensearch.core.index.Index index = new org.opensearch.core.index.Index("test-index", "uuid-1");
        org.opensearch.cluster.metadata.IndexNameExpressionResolver resolver = mock(
            org.opensearch.cluster.metadata.IndexNameExpressionResolver.class
        );
        when(resolver.concreteIndices(any(org.opensearch.cluster.ClusterState.class), any(org.opensearch.action.IndicesRequest.class)))
            .thenReturn(new org.opensearch.core.index.Index[] { index });
        Settings settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", org.opensearch.Version.CURRENT.id)
            .put("index.max_result_window", maxResultWindow)
            .build();
        when(metadata.index(index)).thenReturn(IndexMetadata.builder("test-index").settings(settings).build());
        org.opensearch.neuralsearch.util.NeuralSearchClusterUtil.instance().initialize(clusterService, resolver);
    }

    @SneakyThrows
    public void testDoRewriteFused_whenWindowSizeExceedsMaxResultWindow_thenFailsFast() {
        // window_size above index.max_result_window is rejected at rewrite (each leg fires size=window per shard).
        initClusterUtilWithMaxResultWindow(100);
        HybridQueryBuilder builder = fusedBuilder(
            new HashMap<>(Map.of("normalization", Map.of("technique", "min_max"), "window_size", 500))
        );
        QueryCoordinatorContext ctx = coordinatorContextFor(builder);
        doAnswer(invocation -> null).when(ctx).registerAsyncAction(org.mockito.ArgumentMatchers.any());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.doRewrite(ctx));
        assertThat(e.getMessage(), containsString("max_result_window"));
    }

    @SneakyThrows
    public void testDoRewriteFused_whenWindowSizeWithinMaxResultWindow_thenProceeds() {
        // window_size at/under the ceiling proceeds and registers the leg MultiSearch.
        initClusterUtilWithMaxResultWindow(1000);
        HybridQueryBuilder builder = fusedBuilder(
            new HashMap<>(Map.of("normalization", Map.of("technique", "min_max"), "window_size", 500))
        );
        QueryCoordinatorContext ctx = coordinatorContextFor(builder);
        java.util.concurrent.atomic.AtomicInteger asyncRegistered = new java.util.concurrent.atomic.AtomicInteger();
        doAnswer(invocation -> {
            asyncRegistered.incrementAndGet();
            return null;
        }).when(ctx).registerAsyncAction(org.mockito.ArgumentMatchers.any());
        builder.doRewrite(ctx);
        assertEquals(1, asyncRegistered.get());
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
     *              "filter": {
     *                  "term": {
     *                      "text": "filterKeyword"
     *                  }
     *              }
     *          }
     *      }
     * }
     */
    @SneakyThrows
    public void testFromXContent_whenMultipleSubQueriesAndFilter_thenBuildSuccessfully() {
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
            .startObject("filter")
            .startObject(TermQueryBuilder.NAME)
            .field(TEXT_FIELD_NAME, FILTER_TERM_QUERY_TEXT)
            .endObject()
            .endObject()
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

        assertTrue(queryTwoSubQueries.queries().get(1) instanceof BoolQueryBuilder);
        assertEquals(1, ((BoolQueryBuilder) queryTwoSubQueries.queries().get(1)).must().size());
        assertTrue(((BoolQueryBuilder) queryTwoSubQueries.queries().get(1)).must().get(0) instanceof TermQueryBuilder);
        assertEquals(1, ((BoolQueryBuilder) queryTwoSubQueries.queries().get(1)).filter().size());

        assertEquals(10, queryTwoSubQueries.paginationDepth().intValue());
        // verify knn vector query
        NeuralQueryBuilder neuralQueryBuilder = (NeuralQueryBuilder) queryTwoSubQueries.queries().get(0);
        assertEquals(VECTOR_FIELD_NAME, neuralQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, neuralQueryBuilder.queryText());
        assertEquals(K, (int) neuralQueryBuilder.k());
        assertEquals(MODEL_ID, neuralQueryBuilder.modelId());
        assertEquals(BOOST, neuralQueryBuilder.boost(), 0f);
        assertEquals(
            new TermQueryBuilder(TEXT_FIELD_NAME, FILTER_TERM_QUERY_TEXT),
            ((NeuralQueryBuilder) queryTwoSubQueries.queries().get(0)).queryfilter()
        );
        // verify term query
        assertEquals(
            new TermQueryBuilder(TEXT_FIELD_NAME, TERM_QUERY_TEXT),
            ((BoolQueryBuilder) queryTwoSubQueries.queries().get(1)).must().get(0)
        );
        assertEquals(
            new TermQueryBuilder(TEXT_FIELD_NAME, FILTER_TERM_QUERY_TEXT),
            ((BoolQueryBuilder) queryTwoSubQueries.queries().get(1)).filter().get(0)
        );
    }

    /**
     * Tests that array format for filter produces a helpful error message:
     * {
     *     "queries": [...],
     *     "filter": [
     *         {"term": {"field1": "value1"}},
     *         {"term": {"field2": "value2"}}
     *     ]
     * }
     */
    @SneakyThrows
    public void testFromXContent_whenFilterIsArray_thenFailWithHelpfulMessage() {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("queries")
            .startObject()
            .startObject(TermQueryBuilder.NAME)
            .field(TEXT_FIELD_NAME, TERM_QUERY_TEXT)
            .endObject()
            .endObject()
            .endArray()
            .startArray("filter")
            .startObject()
            .startObject(TermQueryBuilder.NAME)
            .field("field1", "value1")
            .endObject()
            .endObject()
            .startObject()
            .startObject(TermQueryBuilder.NAME)
            .field("field2", "value2")
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
        assertThat(exception.getMessage(), containsString("[hybrid] query's [filter] field must be a query object"));
    }

    /**
     * Tests that scalar format for filter produces a helpful error message:
     * {
     *     "queries": [...],
     *     "filter": "invalid"
     * }
     */
    @SneakyThrows
    public void testFromXContent_whenFilterIsScalarValue_thenFailWithHelpfulMessage() {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("queries")
            .startObject()
            .startObject(TermQueryBuilder.NAME)
            .field(TEXT_FIELD_NAME, TERM_QUERY_TEXT)
            .endObject()
            .endObject()
            .endArray()
            .field("filter", "invalid")
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
        assertThat(exception.getMessage(), containsString("[hybrid] query's [filter] field must be a query object"));
    }

    /**
     * Tests that an unsupported top-level object field returns the generic hybrid error
     * without echoing the customer-provided field name in the exception message.
     */
    @SneakyThrows
    public void testFromXContent_whenUnsupportedFieldIsObject_thenFailWithGenericMessage() {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("queries")
            .startObject()
            .startObject(TermQueryBuilder.NAME)
            .field(TEXT_FIELD_NAME, TERM_QUERY_TEXT)
            .endObject()
            .endObject()
            .endArray()
            .startObject("random_field")
            .startObject(TermQueryBuilder.NAME)
            .field(TEXT_FIELD_NAME, TERM_QUERY_TEXT)
            .endObject()
            .endObject()
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
        assertThat(exception.getMessage(), containsString("Field is not supported by [hybrid] query"));
        assertThat(exception.getMessage(), not(containsString("random_field")));
    }

    /**
     * Tests that an unsupported top-level array field returns the generic hybrid error
     * without echoing the customer-provided field name in the exception message.
     */
    @SneakyThrows
    public void testFromXContent_whenUnsupportedFieldIsArray_thenFailWithGenericMessage() {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("queries")
            .startObject()
            .startObject(TermQueryBuilder.NAME)
            .field(TEXT_FIELD_NAME, TERM_QUERY_TEXT)
            .endObject()
            .endObject()
            .endArray()
            .startArray("random_field")
            .startObject()
            .startObject(TermQueryBuilder.NAME)
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
        assertThat(exception.getMessage(), containsString("Field is not supported by [hybrid] query"));
        assertThat(exception.getMessage(), not(containsString("random_field")));
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
        setUpClusterService();
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
        setUpClusterService(Version.V_3_0_0);
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
        setUpClusterService();
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
            .queryTokensMapSupplier(() -> Map.of("token1", 1.0f, "token2", 0.5f));
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
            .queryTokensMapSupplier(() -> Map.of("token1", 1.0f));

        NeuralSparseQueryBuilder neuralSparseQueryBuilder2 = new NeuralSparseQueryBuilder().fieldName("test_field")
            .queryText("test")
            .modelId("test_model")
            .queryTokensMapSupplier(() -> Map.of("token1", 1.0f));

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
        setUpClusterService();
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder().add(
            NeuralQueryBuilder.builder().fieldName("test").queryText("test").build()
        ).add(new NeuralSparseQueryBuilder());
        List<QueryBuilder> visitedQueries = new ArrayList<>();
        hybridQueryBuilder.visit(createTestVisitor(visitedQueries));
        assertEquals(3, visitedQueries.size());
    }

    public void testFilter() {
        setUpClusterService();
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder().add(
            NeuralQueryBuilder.builder().fieldName("test").queryText("test").build()
        ).add(new NeuralSparseQueryBuilder());
        // Test for Null filter Case
        QueryBuilder queryBuilder = hybridQueryBuilder.filter(null);
        assertEquals(queryBuilder, hybridQueryBuilder);

        // Test for Non-Null filter case and assert every field as expected
        HybridQueryBuilder updatedHybridQueryBuilder = (HybridQueryBuilder) hybridQueryBuilder.filter(new MatchAllQueryBuilder());
        assertEquals(updatedHybridQueryBuilder.queryName(), hybridQueryBuilder.queryName());
        assertEquals(updatedHybridQueryBuilder.paginationDepth(), hybridQueryBuilder.paginationDepth());
        NeuralQueryBuilder updatedNeuralQueryBuilder = (NeuralQueryBuilder) updatedHybridQueryBuilder.queries().get(0);
        assertEquals(new MatchAllQueryBuilder(), updatedNeuralQueryBuilder.queryfilter());
        BoolQueryBuilder updatedNeuralSparseQueryBuilder = (BoolQueryBuilder) updatedHybridQueryBuilder.queries().get(1);
        assertEquals(new NeuralSparseQueryBuilder(), updatedNeuralSparseQueryBuilder.must().get(0));
        assertEquals(new MatchAllQueryBuilder(), updatedNeuralSparseQueryBuilder.filter().get(0));
    }

    public void testExtractInnerHitsBuilders() {
        NestedQueryBuilder nestedQueryBuilder1 = new NestedQueryBuilder(
            "path1",
            new MatchQueryBuilder("testFieldName1", "testValue1"),
            ScoreMode.Max
        );
        nestedQueryBuilder1.innerHit(new InnerHitBuilder());
        NestedQueryBuilder nestedQueryBuilder2 = new NestedQueryBuilder(
            "path2",
            new MatchQueryBuilder("testFieldName2", "testValue2"),
            ScoreMode.Max
        );
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder().add(nestedQueryBuilder1).add(nestedQueryBuilder2);
        Map<String, InnerHitContextBuilder> innerHitsMap = new HashMap<>();
        hybridQueryBuilder.extractInnerHitBuilders(innerHitsMap);
        assertEquals("path1", innerHitsMap.keySet().iterator().next());
        assertEquals(1, innerHitsMap.size());
    }

    public void testExtractInnerHitsBuilders_whenMultipleInnerHitsOnSamePath_thenFail() {
        InnerHitBuilder innerHitBuilder = new InnerHitBuilder();
        NestedQueryBuilder nestedQueryBuilder1 = new NestedQueryBuilder(
            "path1",
            new MatchQueryBuilder("testFieldName1", "testValue1"),
            ScoreMode.Max
        );
        nestedQueryBuilder1.innerHit(innerHitBuilder);
        NestedQueryBuilder nestedQueryBuilder2 = new NestedQueryBuilder(
            "path1",
            new MatchQueryBuilder("testFieldName1", "testValue2"),
            ScoreMode.Max
        );
        nestedQueryBuilder2.innerHit(innerHitBuilder);
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder().add(nestedQueryBuilder1).add(nestedQueryBuilder2);
        Map<String, InnerHitContextBuilder> innerHitsMap = new HashMap<>();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> hybridQueryBuilder.extractInnerHitBuilders(innerHitsMap)
        );
        assertEquals("[inner_hits] already contains an entry for key [path1]", e.getMessage());
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
