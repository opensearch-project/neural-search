/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import java.io.IOException;
import java.util.List;
import java.util.Arrays;

import static org.mockito.Mockito.mock;

public class AgenticSearchQueryBuilderTests extends OpenSearchTestCase {

    private static final String QUERY_TEXT = "Find me red car";
    private static final String AGENT_ID = "test-agent";
    private static final List<String> QUERY_FIELDS = Arrays.asList("title", "description");

    public void testBuilder_withRequiredFields() {
        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).agentId(AGENT_ID);

        assertNotNull("Query builder should not be null", queryBuilder);
        assertEquals("Query text should match", QUERY_TEXT, queryBuilder.queryText());
        assertEquals("Agent ID should match", AGENT_ID, queryBuilder.agentId());
        assertEquals("Writable name should match", "agentic_search", queryBuilder.getWriteableName());
    }

    public void testBuilder_withAllFields() {
        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT)
            .agentId(AGENT_ID)
            .queryFields(QUERY_FIELDS);

        assertNotNull("Query builder should not be null", queryBuilder);
        assertEquals("Query text should match", QUERY_TEXT, queryBuilder.queryText());
        assertEquals("Agent ID should match", AGENT_ID, queryBuilder.agentId());
        assertEquals("Fields should match", QUERY_FIELDS, queryBuilder.queryFields());
    }

    public void testFromXContent_withRequiredFields() throws IOException {
        String json = "{\n" + "  \"query_text\": \"" + QUERY_TEXT + "\",\n" + "  \"agent_id\": \"" + AGENT_ID + "\"\n" + "}";

        XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(), null, json);
        parser.nextToken();

        AgenticSearchQueryBuilder queryBuilder = AgenticSearchQueryBuilder.fromXContent(parser);

        assertNotNull("Query builder should not be null", queryBuilder);
        assertEquals("Query text should match", QUERY_TEXT, queryBuilder.queryText());
        assertEquals("Agent ID should match", AGENT_ID, queryBuilder.agentId());
    }

    public void testFromXContent_withAllFields() throws IOException {
        String json = "{\n"
            + "  \"query_text\": \""
            + QUERY_TEXT
            + "\",\n"
            + "  \"agent_id\": \""
            + AGENT_ID
            + "\",\n"
            + "  \"query_fields\": [\"title\", \"description\"]\n"
            + "}";

        XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(), null, json);
        parser.nextToken();

        AgenticSearchQueryBuilder queryBuilder = AgenticSearchQueryBuilder.fromXContent(parser);

        assertNotNull("Query builder should not be null", queryBuilder);
        assertEquals("Query text should match", QUERY_TEXT, queryBuilder.queryText());
        assertEquals("Agent ID should match", AGENT_ID, queryBuilder.agentId());
        assertEquals("Fields should match", QUERY_FIELDS, queryBuilder.queryFields());
    }

    public void testDoToQuery_throwsException() {
        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).agentId(AGENT_ID);

        QueryShardContext mockContext = mock(QueryShardContext.class);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> queryBuilder.doToQuery(mockContext));

        assertEquals("Exception message should match", "Agentic search query must be rewritten first", exception.getMessage());
    }

    public void testDoRewrite_withoutMLClient() {
        AgenticSearchQueryBuilder.resetMLClient();

        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).agentId(AGENT_ID);

        QueryRewriteContext mockContext = mock(QueryRewriteContext.class);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> queryBuilder.doRewrite(mockContext));

        assertEquals("Exception message should match", "ML client not initialized", exception.getMessage());
    }

    public void testDoRewrite_withNonCoordinatorContext() {
        AgenticSearchQueryBuilder.initialize(mock(MLCommonsClientAccessor.class));

        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).agentId(AGENT_ID);

        QueryRewriteContext mockContext = mock(QueryRewriteContext.class);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> queryBuilder.doRewrite(mockContext));

        assertEquals(
            "Exception message should match",
            "Agentic query must be rewritten at the coordinator node. Rewriting at shard level is not supported.",
            exception.getMessage()
        );
    }

    public void testEquals() {
        AgenticSearchQueryBuilder builder1 = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT)
            .agentId(AGENT_ID)
            .queryFields(QUERY_FIELDS);

        AgenticSearchQueryBuilder builder2 = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT)
            .agentId(AGENT_ID)
            .queryFields(QUERY_FIELDS);

        assertEquals("Identical builders should be equal", builder1, builder2);
        assertEquals("Identical builders should have same hash code", builder1.hashCode(), builder2.hashCode());
    }

    public void testNotEquals() {
        AgenticSearchQueryBuilder builder1 = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).agentId(AGENT_ID);

        AgenticSearchQueryBuilder builder2 = new AgenticSearchQueryBuilder().queryText("different query").agentId(AGENT_ID);

        assertNotEquals("Different builders should not be equal", builder1, builder2);
        assertNotEquals("Different builders should have different hash codes", builder1.hashCode(), builder2.hashCode());
    }

    public void testSerialization() throws IOException {
        AgenticSearchQueryBuilder original = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT)
            .agentId(AGENT_ID)
            .queryFields(QUERY_FIELDS);

        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        AgenticSearchQueryBuilder deserialized = new AgenticSearchQueryBuilder(input);

        assertEquals("Deserialized query should equal original", original, deserialized);
        assertEquals("Query text should match", QUERY_TEXT, deserialized.queryText());
        assertEquals("Agent ID should match", AGENT_ID, deserialized.agentId());
        assertEquals("Fields should match", QUERY_FIELDS, deserialized.queryFields());
    }

    public void testFieldName() {
        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder();
        assertEquals("Field name should match", "agentic_search", queryBuilder.fieldName());
    }
}
