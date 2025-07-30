/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryCoordinatorContext;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchModule;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.util.List;
import java.util.Arrays;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

public class AgenticSearchQueryBuilderTests extends OpenSearchTestCase {

    private static final String QUERY_TEXT = "Find me red car";
    private static final String AGENT_ID = "test-agent";
    private static final List<String> QUERY_FIELDS = Arrays.asList("title", "description");
    MLCommonsClientAccessor mockMLClient = mock(MLCommonsClientAccessor.class);
    private QueryCoordinatorContext mockContext;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockContext = createMockCoordinatorContext();

    }

    public void testBuilder_withRequiredFields() {
        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).agentId(AGENT_ID);

        assertNotNull("Query builder should not be null", queryBuilder);
        assertEquals("Query text should match", QUERY_TEXT, queryBuilder.queryText());
        assertEquals("Agent ID should match", AGENT_ID, queryBuilder.agentId());
        assertEquals("Writable name should match", "agentic", queryBuilder.getWriteableName());
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

        assertEquals(builder1, builder2);
        assertEquals(builder1.hashCode(), builder2.hashCode());
    }

    public void testNotEquals() {
        AgenticSearchQueryBuilder builder1 = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).agentId(AGENT_ID);

        AgenticSearchQueryBuilder builder2 = new AgenticSearchQueryBuilder().queryText("different query").agentId(AGENT_ID);

        assertNotEquals(builder1, builder2);
        assertNotEquals(builder1.hashCode(), builder2.hashCode());
    }

    public void testSerialization() throws IOException {
        AgenticSearchQueryBuilder original = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT)
            .agentId(AGENT_ID)
            .queryFields(QUERY_FIELDS);

        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        AgenticSearchQueryBuilder deserialized = new AgenticSearchQueryBuilder(input);

        assertEquals(original, deserialized);
        assertEquals(QUERY_TEXT, deserialized.queryText());
        assertEquals(AGENT_ID, deserialized.agentId());
        assertEquals(QUERY_FIELDS, deserialized.queryFields());
    }

    public void testFieldName() {
        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder();
        assertEquals("agentic", queryBuilder.fieldName());
    }

    public void testDoRewrite_withNoQueryFieldInAgentResponse() throws IOException {
        AgenticSearchQueryBuilder.initialize(mockMLClient);

        // Mock agent response with empty object (no query field)
        doAnswer(invocation -> {
            ActionListener<String> listener = invocation.getArgument(2);
            String responseWithoutQuery = "{}";
            listener.onResponse(responseWithoutQuery);
            return null;
        }).when(mockMLClient).executeAgent(anyString(), anyMap(), any(ActionListener.class));

        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).agentId(AGENT_ID);

        IOException exception = expectThrows(IOException.class, () -> queryBuilder.doRewrite(mockContext));
        assertEquals("Failed to execute agentic search", exception.getMessage());
        String causeMessage = exception.getCause().getMessage();
        assertTrue(causeMessage.contains("No 'query' field found in agent response of " + AGENT_ID));
    }

    public void testDoRewrite_withUnexpectedFieldInAgentResponse() throws IOException {
        AgenticSearchQueryBuilder.initialize(mockMLClient);

        // Mock agent response with unexpected field
        doAnswer(invocation -> {
            ActionListener<String> listener = invocation.getArgument(2);
            String responseWithUnexpectedField = "{\"result\": \"some value\"}";
            listener.onResponse(responseWithUnexpectedField);
            return null;
        }).when(mockMLClient).executeAgent(anyString(), anyMap(), any(ActionListener.class));

        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).agentId(AGENT_ID);

        IOException exception = expectThrows(IOException.class, () -> queryBuilder.doRewrite(mockContext));
        assertEquals("Failed to execute agentic search", exception.getMessage());
        assertTrue(
            exception.getCause().getMessage().contains("Agent response must contain only a 'query' field. Found unexpected field: result")
        );
    }

    public void testFromXContent_missingQueryText() throws IOException {
        String json = "{\n" + "  \"agent_id\": \"" + AGENT_ID + "\"\n" + "}";

        XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(), null, json);
        parser.nextToken();

        Exception exception = expectThrows(Exception.class, () -> AgenticSearchQueryBuilder.fromXContent(parser));
        assertTrue(exception.getMessage().contains("query_text") && exception.getMessage().contains("required"));
    }

    public void testFromXContent_missingAgentId() throws IOException {
        String json = "{\n" + "  \"query_text\": \"" + QUERY_TEXT + "\"\n" + "}";

        XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(), null, json);
        parser.nextToken();

        Exception exception = expectThrows(Exception.class, () -> AgenticSearchQueryBuilder.fromXContent(parser));
        assertTrue(exception.getMessage().contains("agent_id") && exception.getMessage().contains("required"));
    }

    public void testFromXContent_emptyQueryText() throws IOException {
        String json = "{\n" + "  \"query_text\": \"\",\n" + "  \"agent_id\": \"" + AGENT_ID + "\"\n" + "}";

        XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(), null, json);
        parser.nextToken();

        Exception exception = expectThrows(Exception.class, () -> AgenticSearchQueryBuilder.fromXContent(parser));
        assertTrue(exception.getMessage().contains("query_text") && exception.getMessage().contains("required"));
    }

    public void testFromXContent_emptyAgentId() throws IOException {
        String json = "{\n" + "  \"query_text\": \"" + QUERY_TEXT + "\",\n" + "  \"agent_id\": \"\"\n" + "}";

        XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(), null, json);
        parser.nextToken();

        Exception exception = expectThrows(Exception.class, () -> AgenticSearchQueryBuilder.fromXContent(parser));
        assertTrue(exception.getMessage().contains("agent_id") && exception.getMessage().contains("required"));
    }

    public void testDoRewrite_success() throws IOException {
        MLCommonsClientAccessor mockMLClient = mock(MLCommonsClientAccessor.class);
        AgenticSearchQueryBuilder.initialize(mockMLClient);

        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT)
            .agentId(AGENT_ID)
            .queryFields(QUERY_FIELDS);

        // Mock the agent to return a valid query response
        doAnswer(invocation -> {
            ActionListener<String> listener = invocation.getArgument(2);
            listener.onResponse("{\"query\": {\"match\": {\"title\": \"red car\"}}}");
            return null;
        }).when(mockMLClient).executeAgent(anyString(), anyMap(), any(ActionListener.class));

        QueryBuilder result = queryBuilder.doRewrite(mockContext);

        assertNotNull("Rewritten query should not be null", result);
        assertTrue("Should return a MatchQueryBuilder", result instanceof MatchQueryBuilder);

        // Verify that the agent was called with correct parameters
        verify(mockMLClient).executeAgent(anyString(), anyMap(), any(ActionListener.class));
    }

    public void testDoRewrite_withMalformedAgentResponse() throws IOException {
        MLCommonsClientAccessor mockMLClient = mock(MLCommonsClientAccessor.class);
        AgenticSearchQueryBuilder.initialize(mockMLClient);

        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).agentId(AGENT_ID);

        // Mock the agent to return malformed JSON
        doAnswer(invocation -> {
            ActionListener<String> listener = invocation.getArgument(2);
            listener.onResponse("{ malformed json }");
            return null;
        }).when(mockMLClient).executeAgent(anyString(), anyMap(), any(ActionListener.class));

        IOException exception = expectThrows(IOException.class, () -> queryBuilder.doRewrite(mockContext));
        assertTrue(exception.getMessage().contains("Failed to execute agentic search"));
    }

    public void testDoRewrite_withAgentFailure() throws IOException {
        MLCommonsClientAccessor mockMLClient = mock(MLCommonsClientAccessor.class);
        AgenticSearchQueryBuilder.initialize(mockMLClient);
        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).agentId(AGENT_ID);

        // Mock the agent to return failure
        doAnswer(invocation -> {
            ActionListener<String> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException("Agent execution failed"));
            return null;
        }).when(mockMLClient).executeAgent(anyString(), anyMap(), any(ActionListener.class));

        IOException exception = expectThrows(IOException.class, () -> queryBuilder.doRewrite(mockContext));
        assertTrue(exception.getMessage().contains("Failed to execute agentic search"));
    }

    public void testDoRewrite_withMissingQueryField() throws IOException {
        MLCommonsClientAccessor mockMLClient = mock(MLCommonsClientAccessor.class);
        AgenticSearchQueryBuilder.initialize(mockMLClient);
        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).agentId(AGENT_ID);

        // Mock the agent to return response without "query" field
        doAnswer(invocation -> {
            ActionListener<String> listener = invocation.getArgument(2);
            listener.onResponse("{\"result\": {\"match\": {\"field\": \"value\"}}}");
            return null;
        }).when(mockMLClient).executeAgent(anyString(), anyMap(), any(ActionListener.class));

        IOException exception = expectThrows(IOException.class, () -> queryBuilder.doRewrite(mockContext));
        assertTrue(exception.getMessage().contains("Failed to execute agentic search"));
    }

    public void testDoRewrite_idempotentCheck() throws IOException {
        MLCommonsClientAccessor mockMLClient = mock(MLCommonsClientAccessor.class);
        AgenticSearchQueryBuilder.initialize(mockMLClient);

        QueryCoordinatorContext mockContext = mock(QueryCoordinatorContext.class);

        AgenticSearchQueryBuilder queryBuilder = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).agentId(AGENT_ID);

        // Set a rewritten query to test idempotent behavior
        queryBuilder.rewrittenQuery(mock(MatchQueryBuilder.class));

        // Should return the cached rewritten query without calling ML client
        QueryBuilder result = queryBuilder.doRewrite(mockContext);

        assertNotNull(result);
        verify(mockMLClient, never()).executeAgent(anyString(), anyMap(), any(ActionListener.class));
    }

    private QueryCoordinatorContext createMockCoordinatorContext() throws IOException {
        // Mock cluster service and related components
        ClusterService mockClusterService = mock(ClusterService.class);
        ClusterState mockClusterState = mock(ClusterState.class);
        Metadata mockMetadata = mock(Metadata.class);
        IndexMetadata mockIndexMetadata = mock(IndexMetadata.class);
        MappingMetadata mockMappingMetadata = mock(MappingMetadata.class);

        when(mockClusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.metadata()).thenReturn(mockMetadata);
        when(mockMetadata.index("test-index")).thenReturn(mockIndexMetadata);
        when(mockIndexMetadata.mapping()).thenReturn(mockMappingMetadata);
        when(mockMappingMetadata.source()).thenReturn(new CompressedXContent("{\"properties\":{}}"));

        // Initialize NeuralSearchClusterUtil with mocked cluster service
        NeuralSearchClusterUtil.instance().initialize(mockClusterService, null);

        QueryCoordinatorContext mockContext = mock(QueryCoordinatorContext.class);
        SearchRequest mockSearchRequest = mock(SearchRequest.class);

        // Mock the coordinator context to return a search request with indices
        when(mockContext.getSearchRequest()).thenReturn(mockSearchRequest);
        when(mockSearchRequest.indices()).thenReturn(new String[] { "test-index" });
        // Create proper XContentRegistry with QueryBuilder parsers
        SearchModule searchModule = new SearchModule(Settings.EMPTY, java.util.Collections.emptyList());
        NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
        when(mockContext.getXContentRegistry()).thenReturn(namedXContentRegistry);
        return mockContext;
    }

}
