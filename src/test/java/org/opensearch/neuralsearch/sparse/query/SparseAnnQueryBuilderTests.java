/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.lucene.index.CompositeReaderContext;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.Version;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.mapper.SparseVectorFieldMapper;
import org.opensearch.neuralsearch.sparse.mapper.SparseVectorFieldType;
import org.opensearch.neuralsearch.util.TestUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.QUANTIZATION_CEILING_SEARCH_FIELD;
import static org.opensearch.neuralsearch.sparse.query.SparseQueryTwoPhaseInfo.DEFAULT_EXPANSION_RATIO;
import static org.opensearch.neuralsearch.sparse.query.SparseQueryTwoPhaseInfo.DEFAULT_MAX_WINDOW_SIZE;
import static org.opensearch.neuralsearch.sparse.query.SparseQueryTwoPhaseInfo.DEFAULT_PRUNE_TYPE;
import static org.opensearch.neuralsearch.sparse.query.SparseQueryTwoPhaseInfo.DEFAULT_STATUS;

public class SparseAnnQueryBuilderTests extends AbstractSparseTestBase {
    private static final String MATCH_TERM_FIELD = "field";
    private static final String MATCH_TERM = "text";
    private static final int CUT = 2;
    private static final int K = 10;
    private static final float HEAP_FACTOR = 1.5f;
    private static final String FIELD_NAME = "test_field";

    private QueryBuilder filter;
    private QueryBuilder unequalFilter;
    private SparseAnnQueryBuilder queryBuilder;
    private Map<String, Float> queryTokens;

    @Mock
    private IndexSearcher indexSearcher;
    @Mock
    private QueryShardContext context;
    @Mock
    private SparseVectorFieldType fieldType;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        setUpClusterService(Version.CURRENT);
        when(context.searcher()).thenReturn(indexSearcher);
        when(fieldType.typeName()).thenReturn(SparseVectorFieldMapper.CONTENT_TYPE);
        when(context.fieldMapper(FIELD_NAME)).thenReturn(fieldType);

        filter = new BoolQueryBuilder().filter(new TermQueryBuilder(MATCH_TERM_FIELD, MATCH_TERM));
        unequalFilter = new BoolQueryBuilder().filter(new TermQueryBuilder("other term", MATCH_TERM));

        queryTokens = new HashMap<>();
        queryTokens.put("1", 0.8f);
        queryTokens.put("2", 0.6f);
        queryTokens.put("3", 0.4f);
        SparseQueryTwoPhaseInfo sparseQueryTwoPhaseInfo = new SparseQueryTwoPhaseInfo();
        queryBuilder = SparseAnnQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryCut(CUT)
            .k(K)
            .heapFactor(HEAP_FACTOR)
            .queryTokens(queryTokens)
            .filter(filter)
            .sparseQueryTwoPhaseInfo(sparseQueryTwoPhaseInfo)
            .build();
        // Initialize EventStatsManager for tests
        TestUtils.initializeEventStatsManager();
    }

    public void testBuilder_withValidParameters_createsQueryBuilder() {
        assertNotNull(queryBuilder);
        assertEquals(FIELD_NAME, queryBuilder.fieldName());
        assertEquals(Integer.valueOf(2), queryBuilder.queryCut());
        assertEquals(Integer.valueOf(10), queryBuilder.k());
        assertEquals(Float.valueOf(1.5f), queryBuilder.heapFactor());
        assertEquals(queryTokens, queryBuilder.queryTokens());
        assertEquals(new SparseQueryTwoPhaseInfo(), queryBuilder.sparseQueryTwoPhaseInfo());
    }

    public void testFromXContent_withValidJson_parsesCorrectly() throws IOException {
        String json = "{\"top_n\": 5, \"k\": 20, \"heap_factor\": 2.0}";
        XContentParser parser = createParser(json);
        parser.nextToken();

        SparseAnnQueryBuilder parsed = SparseAnnQueryBuilder.fromXContent(parser);

        assertNotNull(parsed);
        assertEquals(Integer.valueOf(5), parsed.queryCut());
        assertEquals(Integer.valueOf(20), parsed.k());
        assertEquals(Float.valueOf(2.0f), parsed.heapFactor());
    }

    public void testFromXContent_withValidTwoPhaseJson_parsesCorrectly() throws IOException {
        String json = "{\"top_n\": 5, \"k\": 20, \"two_phase_enabled\": true, "
            + "\"two_phase_parameter\": {\"prune_type\": \"max_ratio\", \"prune_ratio\": 0.5, "
            + "\"expansion_rate\": 3.0, \"max_window_size\": 5000}}";
        XContentParser parser = createParser(json);
        parser.nextToken();

        SparseAnnQueryBuilder parsed = SparseAnnQueryBuilder.fromXContent(parser);

        assertNotNull(parsed);
        assertEquals(Integer.valueOf(5), parsed.queryCut());
        assertEquals(Integer.valueOf(20), parsed.k());
        assertNotNull(parsed.sparseQueryTwoPhaseInfo());
        assertEquals(0.5f, parsed.sparseQueryTwoPhaseInfo().getTwoPhasePruneRatio(), 0.001f);
        assertEquals(3.0f, parsed.sparseQueryTwoPhaseInfo().getExpansionRatio(), 0.001f);
        assertEquals(5000, parsed.sparseQueryTwoPhaseInfo().getMaxWindowSize());
    }

    public void testFromXContent_withValidTwoPhaseJsonButNotEnabled_parsesCorrectly() throws IOException {
        String json = "{\"top_n\": 5, \"k\": 20, \"two_phase_enabled\": false, "
            + "\"two_phase_parameter\": {\"prune_type\": \"max_ratio\", \"prune_ratio\": 0.5, "
            + "\"expansion_rate\": 3.0, \"max_window_size\": 5000}}";
        XContentParser parser = createParser(json);
        parser.nextToken();

        SparseAnnQueryBuilder parsed = SparseAnnQueryBuilder.fromXContent(parser);

        assertNotNull(parsed);
        assertEquals(Integer.valueOf(5), parsed.queryCut());
        assertEquals(Integer.valueOf(20), parsed.k());
        assertNull(parsed.sparseQueryTwoPhaseInfo());
    }

    public void testFromXContent_withValidTwoPhaseJsonButInvalidSize() throws IOException {
        String json = "{\"top_n\": 5, \"k\": 20, \"two_phase_enabled\": true, "
            + "\"two_phase_parameter\": {\"prune_type\": \"max_ratio\", \"prune_ratio\": 0.5, "
            + "\"expansion_rate\": 3.0, \"max_window_size\": 50}}";
        XContentParser parser = createParser(json);
        parser.nextToken();

        expectThrows(IllegalArgumentException.class, () -> { SparseAnnQueryBuilder.fromXContent(parser); });
    }

    public void testFromXContent_withInvalidField_throwsException() throws IOException {
        String json = "{\"invalid_field\": \"value\"}";
        XContentParser parser = createParser(json);
        parser.nextToken();

        ParsingException exception = expectThrows(ParsingException.class, () -> { SparseAnnQueryBuilder.fromXContent(parser); });
        assertTrue(exception.getMessage().contains("unknown field [invalid_field]"));
    }

    public void testDoXContent_withAllFields_serializesCorrectly() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        queryBuilder.doXContent(builder, null);
        builder.endObject();

        String result = builder.toString();
        assertTrue(result.contains("\"top_n\":2"));
        assertTrue(result.contains("\"k\":10"));
        assertTrue(result.contains("\"heap_factor\":1.5"));
        assertTrue(result.contains("\"filter\":{"));
        assertTrue(result.contains("\"two_phase_enabled\":true"));
        assertTrue(result.contains("\"two_phase_parameter\":{"));
    }

    public void testDoXContent_withNullCut() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        queryBuilder = SparseAnnQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .k(10)
            .heapFactor(1.5f)
            .queryTokens(queryTokens)
            .filter(filter)
            .build();
        queryBuilder.doXContent(builder, null);
        builder.endObject();

        String result = builder.toString();
        assertFalse(result.contains("\"top_n\":"));
        assertTrue(result.contains("\"k\":10"));
        assertTrue(result.contains("\"heap_factor\":1.5"));
        assertTrue(result.contains("\"filter\":{"));
    }

    public void testDoXContent_withNullK() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        queryBuilder = SparseAnnQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryCut(2)
            .heapFactor(1.5f)
            .queryTokens(queryTokens)
            .filter(filter)
            .build();
        queryBuilder.doXContent(builder, null);
        builder.endObject();

        String result = builder.toString();
        assertTrue(result.contains("\"top_n\":2"));
        assertFalse(result.contains("\"k\":"));
        assertTrue(result.contains("\"heap_factor\":1.5"));
        assertTrue(result.contains("\"filter\":{"));
    }

    public void testDoXContent_withNullHeapFactor() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        queryBuilder = SparseAnnQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryCut(2)
            .k(10)
            .queryTokens(queryTokens)
            .filter(filter)
            .build();
        queryBuilder.doXContent(builder, null);
        builder.endObject();

        String result = builder.toString();
        assertTrue(result.contains("\"top_n\":2"));
        assertTrue(result.contains("\"k\":10"));
        assertFalse(result.contains("\"heap_factor\":"));
        assertTrue(result.contains("\"filter\":{"));
    }

    public void testDoXContent_withNullFilter() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        queryBuilder = SparseAnnQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryCut(2)
            .k(10)
            .heapFactor(1.5f)
            .queryTokens(queryTokens)
            .build();
        queryBuilder.doXContent(builder, null);
        builder.endObject();

        String result = builder.toString();
        assertTrue(result.contains("\"top_n\":2"));
        assertTrue(result.contains("\"k\":10"));
        assertTrue(result.contains("\"heap_factor\":"));
        assertFalse(result.contains("\"filter\":"));
    }

    public void testDoXContent_withNullTwoPhaseInfo() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        queryBuilder = SparseAnnQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryCut(2)
            .k(10)
            .heapFactor(1.5f)
            .queryTokens(queryTokens)
            .filter(filter)
            .build();
        queryBuilder.doXContent(builder, null);
        builder.endObject();

        String result = builder.toString();
        assertTrue(result.contains("\"top_n\":2"));
        assertTrue(result.contains("\"k\":10"));
        assertTrue(result.contains("\"heap_factor\":"));
        assertTrue(result.contains("\"filter\":{"));
        assertFalse(result.contains("\"two_phase_enabled\":"));
        assertFalse(result.contains("\"two_phase_parameter\":"));
    }

    public void testValidateFieldType_withValidFieldType_passes() {
        SparseAnnQueryBuilder.validateFieldType(fieldType);
    }

    public void testValidateFieldType_withInvalidFieldType_throwsException() {
        when(fieldType.typeName()).thenReturn("text");

        expectThrows(IllegalArgumentException.class, () -> { SparseAnnQueryBuilder.validateFieldType(fieldType); });
    }

    public void testEquals_withSameValues_returnsTrue() {
        QueryBuilder filter = new BoolQueryBuilder().filter(new TermQueryBuilder(MATCH_TERM_FIELD, MATCH_TERM));
        SparseAnnQueryBuilder other = SparseAnnQueryBuilder.builder()
            .queryCut(2)
            .k(10)
            .heapFactor(1.5f)
            .filter(filter)
            .sparseQueryTwoPhaseInfo(new SparseQueryTwoPhaseInfo())
            .build();

        assertTrue(queryBuilder.doEquals(other));
    }

    public void testEquals_returnFalse() {
        SparseQueryTwoPhaseInfo info = new SparseQueryTwoPhaseInfo();
        SparseAnnQueryBuilder other = SparseAnnQueryBuilder.builder()
            .queryCut(2)
            .k(10)
            .heapFactor(1.5f)
            .filter(filter)
            .sparseQueryTwoPhaseInfo(info)
            .build();
        assertTrue(queryBuilder.doEquals(other));
        other = SparseAnnQueryBuilder.builder()
            .queryCut(2)
            .k(10)
            .heapFactor(1.5f)
            .filter(unequalFilter)
            .sparseQueryTwoPhaseInfo(info)
            .build();
        assertFalse(queryBuilder.doEquals(other));
        other = SparseAnnQueryBuilder.builder().queryCut(2).k(10).heapFactor(1.0f).filter(filter).sparseQueryTwoPhaseInfo(info).build();
        assertFalse(queryBuilder.doEquals(other));
        other = SparseAnnQueryBuilder.builder().queryCut(2).k(1).heapFactor(1.5f).filter(filter).sparseQueryTwoPhaseInfo(info).build();
        assertFalse(queryBuilder.doEquals(other));
        other = SparseAnnQueryBuilder.builder().queryCut(1).k(10).heapFactor(1.5f).filter(filter).sparseQueryTwoPhaseInfo(info).build();
        assertFalse(queryBuilder.doEquals(other));
        other = SparseAnnQueryBuilder.builder()
            .queryCut(2)
            .k(10)
            .heapFactor(1.5f)
            .filter(filter)
            .sparseQueryTwoPhaseInfo(
                new SparseQueryTwoPhaseInfo(DEFAULT_STATUS, 1.0f, DEFAULT_PRUNE_TYPE, DEFAULT_EXPANSION_RATIO, DEFAULT_MAX_WINDOW_SIZE)
            )
            .build();
        assertFalse(queryBuilder.doEquals(other));
    }

    public void testHashCode_withSameValues_returnsSameHashCode() {
        SparseAnnQueryBuilder other = SparseAnnQueryBuilder.builder()
            .queryCut(2)
            .k(10)
            .heapFactor(1.5f)
            .filter(filter)
            .sparseQueryTwoPhaseInfo(new SparseQueryTwoPhaseInfo())
            .build();
        assertEquals(queryBuilder.doHashCode(), other.doHashCode());
    }

    public void testHashCode_withSameValues_returnsDifferentHashCode() {
        SparseQueryTwoPhaseInfo info = new SparseQueryTwoPhaseInfo();
        SparseAnnQueryBuilder other = SparseAnnQueryBuilder.builder()
            .queryCut(2)
            .k(10)
            .heapFactor(1.5f)
            .filter(filter)
            .sparseQueryTwoPhaseInfo(info)
            .build();
        assertEquals(queryBuilder.doHashCode(), other.doHashCode());
        other = SparseAnnQueryBuilder.builder()
            .queryCut(2)
            .k(10)
            .heapFactor(1.5f)
            .filter(unequalFilter)
            .sparseQueryTwoPhaseInfo(info)
            .build();
        assertNotEquals(queryBuilder.doHashCode(), other.doHashCode());
        other = SparseAnnQueryBuilder.builder().queryCut(2).k(10).heapFactor(1.0f).filter(filter).sparseQueryTwoPhaseInfo(info).build();
        assertNotEquals(queryBuilder.doHashCode(), other.doHashCode());
        other = SparseAnnQueryBuilder.builder().queryCut(2).k(1).heapFactor(1.5f).filter(filter).sparseQueryTwoPhaseInfo(info).build();
        assertNotEquals(queryBuilder.doHashCode(), other.doHashCode());
        other = SparseAnnQueryBuilder.builder().queryCut(1).k(10).heapFactor(1.5f).filter(filter).sparseQueryTwoPhaseInfo(info).build();
        assertNotEquals(queryBuilder.doHashCode(), other.doHashCode());
        other = SparseAnnQueryBuilder.builder()
            .queryCut(2)
            .k(10)
            .heapFactor(1.5f)
            .filter(filter)
            .sparseQueryTwoPhaseInfo(
                new SparseQueryTwoPhaseInfo(DEFAULT_STATUS, 1.0f, DEFAULT_PRUNE_TYPE, DEFAULT_EXPANSION_RATIO, DEFAULT_MAX_WINDOW_SIZE)
            )
            .build();
        assertNotEquals(queryBuilder.doHashCode(), other.doHashCode());
    }

    public void testDoToQuery_withValidContext_returnsQuery() throws IOException {
        QueryBuilder mockFilter = mock(QueryBuilder.class);
        Query filterQuery = mock(Query.class);
        when(mockFilter.toQuery(any())).thenReturn(filterQuery);
        queryBuilder = SparseAnnQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryCut(CUT)
            .k(K)
            .heapFactor(HEAP_FACTOR)
            .queryTokens(queryTokens)
            .filter(mockFilter)
            .build();

        CompositeReaderContext compositeReaderContext = mock(CompositeReaderContext.class);
        when(indexSearcher.getTopReaderContext()).thenReturn(compositeReaderContext);
        LeafReaderContext leafReaderContext = mock(LeafReaderContext.class);
        when(compositeReaderContext.leaves()).thenReturn(List.of(leafReaderContext));
        LeafReader leafReader = mock(LeafReader.class);
        when(leafReaderContext.reader()).thenReturn(leafReader);
        FieldInfos fieldInfos = mock(FieldInfos.class);
        when(leafReader.getFieldInfos()).thenReturn(fieldInfos);
        FieldInfo fieldInfo = mock(FieldInfo.class);
        when(fieldInfos.fieldInfo(FIELD_NAME)).thenReturn(fieldInfo);
        when(fieldInfo.getAttribute(QUANTIZATION_CEILING_SEARCH_FIELD)).thenReturn("5.0f");

        MappedFieldType fieldType2 = mock(MappedFieldType.class);
        when(context.fieldMapper("field")).thenReturn(fieldType2);
        Query termQuery = mock(Query.class);
        when(fieldType2.termQuery(any(), any())).thenReturn(termQuery);

        queryBuilder.fallbackQuery(mock(Query.class));

        SparseVectorQuery query = (SparseVectorQuery) queryBuilder.doToQuery(context);
        queryTokens.remove("3"); // "3" has the smallest value
        assertEquals(queryTokens.keySet(), new HashSet<>(query.getQueryContext().getTokens()));
        assertEquals(filterQuery, query.getFilter());
        assertEquals(HEAP_FACTOR, query.getQueryContext().getHeapFactor(), DELTA_FOR_ASSERTION);
        assertEquals(K, query.getQueryContext().getK());
    }

    public void testDoToQuery_withValidContext_defaultParameter() throws IOException {
        queryTokens.put("4", 0.5f);
        queryTokens.put("5", 0.6f);
        queryTokens.put("6", 0.7f);
        queryTokens.put("7", 0.8f);
        queryTokens.put("8", 0.9f);
        queryTokens.put("9", 1.0f);
        queryTokens.put("10", 1.1f);
        queryTokens.put("11", 1.2f);
        queryBuilder = SparseAnnQueryBuilder.builder().fieldName(FIELD_NAME).queryTokens(queryTokens).build();
        MappedFieldType fieldType2 = mock(MappedFieldType.class);
        when(context.fieldMapper("field")).thenReturn(fieldType2);
        Query termQuery = mock(Query.class);
        when(fieldType2.termQuery(any(), any())).thenReturn(termQuery);

        queryBuilder.fallbackQuery(mock(Query.class));

        SparseVectorQuery query = (SparseVectorQuery) queryBuilder.doToQuery(context);
        queryTokens.remove("3"); // "3" has the smallest value
        assertEquals(queryTokens.keySet(), new HashSet<>(query.getQueryContext().getTokens()));
        assertNull(query.getFilter());
        assertEquals(1.0, query.getQueryContext().getHeapFactor(), DELTA_FOR_ASSERTION);
        assertEquals(10, query.getQueryContext().getK());
    }

    public void testDoToQuery_invalidFieldType() {
        queryBuilder = SparseAnnQueryBuilder.builder()
            .fieldName("test_field")
            .queryCut(CUT)
            .k(K)
            .heapFactor(HEAP_FACTOR)
            .queryTokens(queryTokens)
            .filter(filter)
            .build();
        QueryShardContext context = mock(QueryShardContext.class);
        when(context.fieldMapper(FIELD_NAME)).thenReturn(null);
        expectThrows(IllegalArgumentException.class, () -> queryBuilder.doToQuery(context));
    }

    private XContentParser createParser(String json) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .rawValue(new java.io.ByteArrayInputStream(json.getBytes(Charset.defaultCharset())), XContentType.JSON);
        return createParser(builder);
    }

    public void testStreamConstructor_readsCorrectly() throws IOException {
        StreamInput streamInput = mock(StreamInput.class);
        when(streamInput.readOptionalInt()).thenReturn(5, 20);
        when(streamInput.readOptionalFloat()).thenReturn(1.5f);

        SparseAnnQueryBuilder fromStream = new SparseAnnQueryBuilder(streamInput);

        assertEquals(Integer.valueOf(5), fromStream.queryCut());
        assertEquals(Integer.valueOf(20), fromStream.k());
        assertEquals(Float.valueOf(1.5f), fromStream.heapFactor());
    }

    public void testDoWriteTo_writesCorrectly() throws IOException {
        StreamOutput streamOutput = mock(StreamOutput.class);

        queryBuilder.doWriteTo(streamOutput);

        verify(streamOutput).writeOptionalInt(2);
        verify(streamOutput).writeOptionalInt(10);
        verify(streamOutput).writeOptionalFloat(1.5f);
        verify(streamOutput).writeOptionalBoolean(true);
    }

    public void testFromXContent_withInvalidStartToken_throwsException() throws IOException {
        String json = "\"invalid_start\"";
        XContentParser parser = createParser(json);
        parser.nextToken();

        ParsingException exception = expectThrows(ParsingException.class, () -> { SparseAnnQueryBuilder.fromXContent(parser); });
        assertTrue(exception.getMessage().contains("must be an object"));
    }

    public void testEquals_withSameInstance_returnsTrue() {
        assertTrue(queryBuilder.doEquals(queryBuilder));
    }

    public void testEquals_withNull_returnsFalse() {
        assertFalse(queryBuilder.doEquals(null));
    }

    public void testFromXContent_withFilter_triggersFilterParsing() throws IOException {
        String json = "{\"top_n\": 3, \"filter\": {\"match_all\": {}}}";
        XContentParser parser = createParser(json);
        parser.nextToken();

        Exception exception = expectThrows(Exception.class, () -> { SparseAnnQueryBuilder.fromXContent(parser); });

        assertTrue(
            "Should fail at QueryBuilder parsing",
            exception.getMessage().contains("unknown query")
                || exception.getMessage().contains("NamedObjectNotFoundException")
                || exception.getMessage().contains("unknown named object category")
        );
    }

    public void testFromXContent_withUnknownToken_throwsException() throws IOException {
        String json = "{\"top_n\": {\"nested\": \"object\"}}";
        XContentParser parser = createParser(json);
        parser.nextToken();

        ParsingException exception = expectThrows(ParsingException.class, () -> { SparseAnnQueryBuilder.fromXContent(parser); });
        assertTrue(exception.getMessage().contains("unknown token"));
    }

    public void testDoRewrite_returnsNewInstance() {
        TermQueryBuilder filter = new TermQueryBuilder("status", "active");
        queryBuilder.filter(filter);

        SparseAnnQueryBuilder rewritten = (SparseAnnQueryBuilder) queryBuilder.doRewrite(null);

        assertNotNull(rewritten);
        assertNotSame(queryBuilder, rewritten);
        assertEquals(queryBuilder.fieldName(), rewritten.fieldName());
        assertEquals(queryBuilder.queryCut(), rewritten.queryCut());
        assertEquals(queryBuilder.k(), rewritten.k());
        assertEquals(queryBuilder.heapFactor(), rewritten.heapFactor());
        assertEquals(queryBuilder.filter(), rewritten.filter());
        assertEquals(queryBuilder.sparseQueryTwoPhaseInfo(), rewritten.sparseQueryTwoPhaseInfo());
    }

    public void testDoToQuery_withFilter_appliesFilter() throws IOException {
        queryBuilder.sparseQueryTwoPhaseInfo(null);
        QueryBuilder filter = mock(QueryBuilder.class);
        Query filterQuery = mock(Query.class);
        when(filter.toQuery(context)).thenReturn(filterQuery);

        queryBuilder.filter(filter);
        queryBuilder.fallbackQuery(mock(Query.class));

        assertNotNull(queryBuilder.doToQuery(context));
        verify(filter).toQuery(context);
    }

    public void testDoToQuery_withFilter_appliesFilterTwiceWithTwoPhase() throws IOException {
        QueryBuilder filter = mock(QueryBuilder.class);
        Query filterQuery = mock(Query.class);
        when(filter.toQuery(context)).thenReturn(filterQuery);

        queryBuilder.filter(filter);
        queryBuilder.fallbackQuery(mock(Query.class));

        assertNotNull(queryBuilder.doToQuery(context));
        verify(filter, times(2)).toQuery(context);
    }

    public void testQueryTokens_SetterCanProcessTokens() {
        Map<String, Float> queryTokens = Map.of("1", 1.0f, "65537", 2.0f, "2", 100f, "65538", 1.0f);
        SparseAnnQueryBuilder builder = SparseAnnQueryBuilder.builder().queryTokens(queryTokens).build();
        Map<String, Float> expectedQueryTokens = Map.of("1", 2.0f, "2", 100f);
        assertEquals(expectedQueryTokens, builder.queryTokens());

        SparseAnnQueryBuilder builder2 = new SparseAnnQueryBuilder();
        builder2.queryTokens(queryTokens);
        assertEquals(expectedQueryTokens, builder2.queryTokens());

        SparseAnnQueryBuilder builder3 = new SparseAnnQueryBuilder("name", 3, 10, 1.0f, null, null, queryTokens, null);
        assertEquals(expectedQueryTokens, builder3.queryTokens());
    }

    public void testQueryTokens_SetterThrowExceptionForInvalidToken() {
        Map<String, Float> tokens1 = Map.of("hello", 1.0f, "65537", 2.0f, "2", 100f, "65538", 1.0f);
        expectThrows(IllegalArgumentException.class, () -> SparseAnnQueryBuilder.builder().queryTokens(tokens1).build());
        Map<String, Float> tokens2 = Map.of("-1", 1.0f, "65537", 2.0f, "2", 100f, "65538", 1.0f);
        expectThrows(IllegalArgumentException.class, () -> SparseAnnQueryBuilder.builder().queryTokens(tokens2).build());
    }

    public void testDoToQuery_withTwoPhaseInfo_buildsTwoPhaseQueries() throws IOException {
        SparseQueryTwoPhaseInfo twoPhaseInfo = new SparseQueryTwoPhaseInfo();
        queryBuilder = SparseAnnQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryCut(CUT)
            .k(K)
            .heapFactor(HEAP_FACTOR)
            .queryTokens(queryTokens)
            .sparseQueryTwoPhaseInfo(twoPhaseInfo)
            .build();
        queryBuilder.fallbackQuery(mock(Query.class));

        SparseVectorQuery query = (SparseVectorQuery) queryBuilder.doToQuery(context);

        assertNotNull(query.getRankFeaturesPhaseOneQuery());
        assertNotNull(query.getRankFeaturesPhaseTwoQuery());
        assertEquals(twoPhaseInfo, query.getSparseQueryTwoPhaseInfo());
    }
}
