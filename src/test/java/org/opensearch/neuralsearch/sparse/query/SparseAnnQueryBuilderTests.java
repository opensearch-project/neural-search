/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.lucene.search.Query;
import org.junit.Before;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensFieldMapper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SparseAnnQueryBuilderTests extends AbstractSparseTestBase {
    private SparseAnnQueryBuilder queryBuilder;
    private Map<String, Float> queryTokens;
    private static final String MATCH_TERM_FIELD = "field";
    private static final String MATCH_TERM = "text";
    private static final int CUT = 2;
    private static final int K = 10;
    private static final float HEAP_FACTOR = 1.5f;
    private QueryBuilder filter;
    private QueryBuilder unequalFilter;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        filter = new BoolQueryBuilder().filter(new TermQueryBuilder(MATCH_TERM_FIELD, MATCH_TERM));
        unequalFilter = new BoolQueryBuilder().filter(new TermQueryBuilder("other term", MATCH_TERM));

        queryTokens = new HashMap<>();
        queryTokens.put("1", 0.8f);
        queryTokens.put("2", 0.6f);
        queryTokens.put("3", 0.4f);

        queryBuilder = SparseAnnQueryBuilder.builder()
            .fieldName("test_field")
            .queryCut(CUT)
            .k(K)
            .heapFactor(HEAP_FACTOR)
            .queryTokens(queryTokens)
            .filter(filter)
            .build();
    }

    public void testBuilder_withValidParameters_createsQueryBuilder() {
        assertNotNull(queryBuilder);
        assertEquals("test_field", queryBuilder.fieldName());
        assertEquals(Integer.valueOf(2), queryBuilder.queryCut());
        assertEquals(Integer.valueOf(10), queryBuilder.k());
        assertEquals(Float.valueOf(1.5f), queryBuilder.heapFactor());
        assertEquals(queryTokens, queryBuilder.queryTokens());
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
    }

    public void testDoXContent_withNullCut() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        queryBuilder = SparseAnnQueryBuilder.builder()
            .fieldName("test_field")
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
            .fieldName("test_field")
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
            .fieldName("test_field")
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
            .fieldName("test_field")
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

    public void testValidateFieldType_withValidFieldType_passes() {
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.typeName()).thenReturn(SparseTokensFieldMapper.CONTENT_TYPE);

        SparseAnnQueryBuilder.validateFieldType(fieldType);
    }

    public void testValidateFieldType_withInvalidFieldType_throwsException() {
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.typeName()).thenReturn("text");

        expectThrows(IllegalArgumentException.class, () -> { SparseAnnQueryBuilder.validateFieldType(fieldType); });
    }

    public void testEquals_withSameValues_returnsTrue() {
        QueryBuilder filter = new BoolQueryBuilder().filter(new TermQueryBuilder(MATCH_TERM_FIELD, MATCH_TERM));
        SparseAnnQueryBuilder other = SparseAnnQueryBuilder.builder().queryCut(2).k(10).heapFactor(1.5f).filter(filter).build();

        assertTrue(queryBuilder.doEquals(other));
    }

    public void testEquals_returnFalse() {
        SparseAnnQueryBuilder other = SparseAnnQueryBuilder.builder().queryCut(2).k(10).heapFactor(1.5f).filter(unequalFilter).build();
        assertFalse(queryBuilder.doEquals(other));
        other = SparseAnnQueryBuilder.builder().queryCut(2).k(10).heapFactor(1.0f).filter(filter).build();
        assertFalse(queryBuilder.doEquals(other));
        other = SparseAnnQueryBuilder.builder().queryCut(2).k(1).heapFactor(1.5f).filter(filter).build();
        assertFalse(queryBuilder.doEquals(other));
        other = SparseAnnQueryBuilder.builder().queryCut(1).k(10).heapFactor(1.5f).filter(filter).build();
        assertFalse(queryBuilder.doEquals(other));
    }

    public void testHashCode_withSameValues_returnsSameHashCode() {
        SparseAnnQueryBuilder other = SparseAnnQueryBuilder.builder().queryCut(2).k(10).heapFactor(1.5f).filter(filter).build();
        assertEquals(queryBuilder.doHashCode(), other.doHashCode());
    }

    public void testHashCode_withSameValues_returnsDifferentHashCode() {
        SparseAnnQueryBuilder other = SparseAnnQueryBuilder.builder().queryCut(2).k(10).heapFactor(1.5f).filter(unequalFilter).build();
        assertNotEquals(queryBuilder.doHashCode(), other.doHashCode());
        other = SparseAnnQueryBuilder.builder().queryCut(2).k(10).heapFactor(1.0f).filter(filter).build();
        assertNotEquals(queryBuilder.doHashCode(), other.doHashCode());
        other = SparseAnnQueryBuilder.builder().queryCut(2).k(1).heapFactor(1.5f).filter(filter).build();
        assertNotEquals(queryBuilder.doHashCode(), other.doHashCode());
        other = SparseAnnQueryBuilder.builder().queryCut(1).k(10).heapFactor(1.5f).filter(filter).build();
        assertNotEquals(queryBuilder.doHashCode(), other.doHashCode());
    }

    public void testDoToQuery_withValidContext_returnsQuery() throws IOException {
        QueryBuilder mockFilter = mock(QueryBuilder.class);
        Query filterQuery = mock(Query.class);
        when(mockFilter.toQuery(any())).thenReturn(filterQuery);
        queryBuilder = SparseAnnQueryBuilder.builder()
            .fieldName("test_field")
            .queryCut(CUT)
            .k(K)
            .heapFactor(HEAP_FACTOR)
            .queryTokens(queryTokens)
            .filter(mockFilter)
            .build();
        QueryShardContext context = mock(QueryShardContext.class);
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.typeName()).thenReturn(SparseTokensFieldMapper.CONTENT_TYPE);
        when(context.fieldMapper("test_field")).thenReturn(fieldType);
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
        queryBuilder = SparseAnnQueryBuilder.builder().fieldName("test_field").queryTokens(queryTokens).build();
        QueryShardContext context = mock(QueryShardContext.class);
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.typeName()).thenReturn(SparseTokensFieldMapper.CONTENT_TYPE);
        when(context.fieldMapper("test_field")).thenReturn(fieldType);
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

    public void testDoToQuery_invalidFieldType() throws IOException {
        queryBuilder = SparseAnnQueryBuilder.builder()
            .fieldName("test_field")
            .queryCut(CUT)
            .k(K)
            .heapFactor(HEAP_FACTOR)
            .queryTokens(queryTokens)
            .filter(filter)
            .build();
        QueryShardContext context = mock(QueryShardContext.class);
        when(context.fieldMapper("test_field")).thenReturn(null);
        expectThrows(IllegalArgumentException.class, () -> queryBuilder.doToQuery(context));
    }

    private XContentParser createParser(String json) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .rawValue(new java.io.ByteArrayInputStream(json.getBytes(Charset.defaultCharset())), XContentType.JSON);
        return createParser(builder);
    }

    public void testStreamConstructor_readsCorrectly() throws IOException {
        org.opensearch.core.common.io.stream.StreamInput streamInput = mock(org.opensearch.core.common.io.stream.StreamInput.class);
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
    }

    public void testDoToQuery_withFilter_appliesFilter() throws IOException {
        QueryShardContext context = mock(QueryShardContext.class);
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.typeName()).thenReturn(SparseTokensFieldMapper.CONTENT_TYPE);
        when(context.fieldMapper("test_field")).thenReturn(fieldType);

        QueryBuilder filter = mock(QueryBuilder.class);
        Query filterQuery = mock(Query.class);
        when(filter.toQuery(context)).thenReturn(filterQuery);

        queryBuilder.filter(filter);
        queryBuilder.fallbackQuery(mock(Query.class));

        assertNotNull(queryBuilder.doToQuery(context));
        verify(filter).toQuery(context);
    }

    public void testQueryTokens_SetterCanProcessTokens() {
        Map<String, Float> queryTokens = Map.of("1", 1.0f, "65537", 2.0f, "2", 100f, "65538", 1.0f);
        SparseAnnQueryBuilder builder = SparseAnnQueryBuilder.builder().queryTokens(queryTokens).build();
        Map<String, Float> expectedQueryTokens = Map.of("1", 2.0f, "2", 100f);
        assertEquals(expectedQueryTokens, builder.queryTokens());

        SparseAnnQueryBuilder builder2 = new SparseAnnQueryBuilder();
        builder2.queryTokens(queryTokens);
        assertEquals(expectedQueryTokens, builder2.queryTokens());

        SparseAnnQueryBuilder builder3 = new SparseAnnQueryBuilder("name", 3, 10, 1.0f, null, null, queryTokens);
        assertEquals(expectedQueryTokens, builder3.queryTokens());
    }

    public void testQueryTokens_SetterThrowExceptionForInvalidToken() {
        Map<String, Float> tokens1 = Map.of("hello", 1.0f, "65537", 2.0f, "2", 100f, "65538", 1.0f);
        expectThrows(IllegalArgumentException.class, () -> SparseAnnQueryBuilder.builder().queryTokens(tokens1).build());
        Map<String, Float> tokens2 = Map.of("-1", 1.0f, "65537", 2.0f, "2", 100f, "65538", 1.0f);
        expectThrows(IllegalArgumentException.class, () -> SparseAnnQueryBuilder.builder().queryTokens(tokens2).build());
    }
}
