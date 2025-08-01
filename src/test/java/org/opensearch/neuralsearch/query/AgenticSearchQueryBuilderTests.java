/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Arrays;

import static org.mockito.Mockito.mock;

public class AgenticSearchQueryBuilderTests extends OpenSearchTestCase {

    private static final String QUERY_TEXT = "Find me red car";
    private static final List<String> QUERY_FIELDS = Arrays.asList("title", "description");

    public void testBuilder_withRequiredFields() {
        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT);

        assertNotNull("Query builder should not be null", queryBuilder);
        assertEquals("Query text should match", QUERY_TEXT, queryBuilder.getQueryText());
        assertEquals("Writable name should match", "agentic", queryBuilder.getWriteableName());
    }

    public void testBuilder_withAllFields() {
        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).queryFields(QUERY_FIELDS);

        assertNotNull("Query builder should not be null", queryBuilder);
        assertEquals("Query text should match", QUERY_TEXT, queryBuilder.getQueryText());
        assertEquals("Fields should match", QUERY_FIELDS, queryBuilder.getQueryFields());
    }

    public void testFromXContent_withRequiredFields() throws IOException {
        String json = "{\n" + "  \"query_text\": \"" + QUERY_TEXT + "\"\n" + "}";

        XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(), null, json);
        parser.nextToken();

        AgenticSearchQueryBuilder queryBuilder = AgenticSearchQueryBuilder.fromXContent(parser);

        assertNotNull("Query builder should not be null", queryBuilder);
        assertEquals("Query text should match", QUERY_TEXT, queryBuilder.getQueryText());
    }

    public void testFromXContent_withAllFields() throws IOException {
        String json = "{\n" + "  \"query_text\": \"" + QUERY_TEXT + "\",\n" + "  \"query_fields\": [\"title\", \"description\"]\n" + "}";

        XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(), null, json);
        parser.nextToken();

        AgenticSearchQueryBuilder queryBuilder = AgenticSearchQueryBuilder.fromXContent(parser);

        assertNotNull("Query builder should not be null", queryBuilder);
        assertEquals("Query text should match", QUERY_TEXT, queryBuilder.getQueryText());
        assertEquals("Fields should match", QUERY_FIELDS, queryBuilder.getQueryFields());
    }

    public void testFromXContent_missingQueryText() throws IOException {
        String json = "{\n" + "  \"query_fields\": [\"title\"]\n" + "}";

        XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(), null, json);
        parser.nextToken();

        Exception exception = expectThrows(Exception.class, () -> AgenticSearchQueryBuilder.fromXContent(parser));
        assertTrue(exception.getMessage().contains("query_text") && exception.getMessage().contains("required"));
    }

    public void testFromXContent_emptyQueryText() throws IOException {
        String json = "{\n" + "  \"query_text\": \"\"\n" + "}";

        XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(), null, json);
        parser.nextToken();

        Exception exception = expectThrows(Exception.class, () -> AgenticSearchQueryBuilder.fromXContent(parser));
        assertTrue(exception.getMessage().contains("query_text") && exception.getMessage().contains("required"));
    }

    public void testFromXContent_unknownField() throws IOException {
        String json = "{\n" + "  \"query_text\": \"" + QUERY_TEXT + "\",\n" + "  \"unknown_field\": \"value\"\n" + "}";

        XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(), null, json);
        parser.nextToken();

        Exception exception = expectThrows(Exception.class, () -> AgenticSearchQueryBuilder.fromXContent(parser));
        assertTrue(exception.getMessage().contains("Unknown field"));
    }

    public void testDoToQuery_throwsException() {
        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT);
        QueryShardContext mockContext = mock(QueryShardContext.class);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> queryBuilder.doToQuery(mockContext));
        assertEquals(
            "Exception message should match",
            "Agentic search query should be processed by QueryRewriterProcessor",
            exception.getMessage()
        );
    }

    public void testDoRewrite_returnsThis() throws IOException {
        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT);
        QueryRewriteContext mockContext = mock(QueryRewriteContext.class);

        QueryBuilder result = queryBuilder.doRewrite(mockContext);
        assertEquals("doRewrite should return this", queryBuilder, result);
    }

    public void testEquals() {
        AgenticSearchQueryBuilder builder1 = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).queryFields(QUERY_FIELDS);

        AgenticSearchQueryBuilder builder2 = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).queryFields(QUERY_FIELDS);

        assertEquals(builder1, builder2);
        assertEquals(builder1.hashCode(), builder2.hashCode());
    }

    public void testNotEquals() {
        AgenticSearchQueryBuilder builder1 = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT);
        AgenticSearchQueryBuilder builder2 = new AgenticSearchQueryBuilder().queryText("different query");

        assertNotEquals(builder1, builder2);
        assertNotEquals(builder1.hashCode(), builder2.hashCode());
    }

    public void testSerialization() throws IOException {
        AgenticSearchQueryBuilder original = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).queryFields(QUERY_FIELDS);

        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        AgenticSearchQueryBuilder deserialized = new AgenticSearchQueryBuilder(input);

        assertEquals(original, deserialized);
        assertEquals(QUERY_TEXT, deserialized.getQueryText());
        assertEquals(QUERY_FIELDS, deserialized.getQueryFields());
    }

    public void testFieldName() {
        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder();
        assertEquals("agentic", queryBuilder.fieldName());
    }

    public void testGetters() {
        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).queryFields(QUERY_FIELDS);

        assertEquals(QUERY_TEXT, queryBuilder.getQueryText());
        assertEquals(QUERY_FIELDS, queryBuilder.getQueryFields());
    }

    public void testNullQueryFields() {
        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT);
        assertNull("Query fields should be null by default", queryBuilder.getQueryFields());
    }
}
