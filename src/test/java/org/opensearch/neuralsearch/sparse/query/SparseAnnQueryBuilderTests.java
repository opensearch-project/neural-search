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
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensFieldMapper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SparseAnnQueryBuilderTests extends AbstractSparseTestBase {
    private static SparseAnnQueryBuilder queryBuilder;
    private static Map<String, Float> queryTokens;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        queryTokens = new HashMap<>();
        queryTokens.put("1", 0.8f);
        queryTokens.put("2", 0.6f);
        queryTokens.put("3", 0.4f);

        queryBuilder = SparseAnnQueryBuilder.builder()
            .fieldName("test_field")
            .queryCut(2)
            .k(10)
            .heapFactor(1.5f)
            .queryTokens(queryTokens)
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

    public void testGetWriteableName_returnsCorrectName() {
        assertEquals("sparse_ann", queryBuilder.getWriteableName());
    }

    public void testFromXContent_withValidJson_parsesCorrectly() throws IOException {
        String json = "{\"cut\": 5, \"k\": 20, \"heap_factor\": 2.0}";
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
        assertTrue(result.contains("\"cut\":2"));
        assertTrue(result.contains("\"k\":10"));
        assertTrue(result.contains("\"heap_factor\":1.5"));
    }

    public void testValidateFieldType_withValidFieldType_passes() {
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.typeName()).thenReturn(SparseTokensFieldMapper.CONTENT_TYPE);

        SparseAnnQueryBuilder.validateFieldType(fieldType);
    }

    public void testValidateFieldType_withInvalidFieldType_throwsException() {
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.typeName()).thenReturn("text");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            SparseAnnQueryBuilder.validateFieldType(fieldType);
        });
        assertTrue(exception.getMessage().contains("query only works on [sparse_tokens] fields"));
    }

    public void testEquals_withSameValues_returnsTrue() {
        SparseAnnQueryBuilder other = SparseAnnQueryBuilder.builder().queryCut(2).k(10).heapFactor(1.5f).build();

        assertTrue(queryBuilder.doEquals(other));
    }

    public void testHashCode_withSameValues_returnsSameHashCode() {
        SparseAnnQueryBuilder other = SparseAnnQueryBuilder.builder().queryCut(2).k(10).heapFactor(1.5f).build();

        assertEquals(queryBuilder.doHashCode(), other.doHashCode());
    }

    public void testDoToQuery_withValidContext_returnsQuery() throws IOException {
        QueryShardContext context = mock(QueryShardContext.class);
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.typeName()).thenReturn(SparseTokensFieldMapper.CONTENT_TYPE);
        when(context.fieldMapper("test_field")).thenReturn(fieldType);

        queryBuilder.fallbackQuery(mock(Query.class));

        assertNotNull(queryBuilder.doToQuery(context));
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
        String json = "{\"cut\": 3, \"filter\": {\"match_all\": {}}}";
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
        String json = "{\"cut\": {\"nested\": \"object\"}}";
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
        builder2.setQueryTokens(queryTokens);
        assertEquals(expectedQueryTokens, builder2.queryTokens());

        SparseAnnQueryBuilder builder3 = new SparseAnnQueryBuilder("name", 3, 10, 1.0f, null, null, queryTokens);
        assertEquals(expectedQueryTokens, builder3.queryTokens());
    }
}
