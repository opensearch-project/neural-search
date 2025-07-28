/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.util.Locale;

public class AgenticSearchQueryIT extends BaseNeuralSearchIT {

    private static final String TEST_INDEX = "test-agentic-index";
    private static final String TEST_AGENT_ID = "test-agent-123";
    private static final String TEST_QUERY_TEXT = "Find documents about machine learning";

    public void testAgenticSearchQuery_withValidParameters_thenSuccess() throws Exception {
        createIndex(TEST_INDEX, "{\"mappings\":{\"properties\":{\"passage_text\":{\"type\":\"text\"}}}}");
        addDocument(TEST_INDEX, "1", "passage_text", "This is about science and technology", null, null);

        String queryJson = String.format(
            Locale.ROOT,
            "{" + "\"query\": {" + "\"agentic_search\": {" + "\"query_text\": \"%s\"," + "\"agent_id\": \"%s\"" + "}" + "}" + "}",
            TEST_QUERY_TEXT,
            TEST_AGENT_ID
        );

        Request searchRequest = new Request("POST", "/" + TEST_INDEX + "/_search");
        searchRequest.setJsonEntity(queryJson);

        // TODO Should succeed because the hardcoded DSL query is executed
        Response response = client().performRequest(searchRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    public void testAgenticSearchQuery_withMissingQueryText_thenFail() throws Exception {
        createIndex(TEST_INDEX, "{\"mappings\":{\"properties\":{\"text\":{\"type\":\"text\"}}}}");

        String queryJson = String.format(
            Locale.ROOT,
            "{" + "\"query\": {" + "\"agentic_search\": {" + "\"agent_id\": \"%s\"" + "}" + "}" + "}",
            TEST_AGENT_ID
        );

        Request searchRequest = new Request("POST", "/" + TEST_INDEX + "/_search");
        searchRequest.setJsonEntity(queryJson);

        try {
            client().performRequest(searchRequest);
            fail("Expected parsing exception for missing query_text");
        } catch (Exception e) {
            assertTrue(
                "Should contain query_text required error",
                e.getMessage().contains("query_text") && e.getMessage().contains("required")
            );
        }
    }

    public void testAgenticSearchQuery_withMissingAgentId_thenFail() throws Exception {
        createIndex(TEST_INDEX, "{\"mappings\":{\"properties\":{\"text\":{\"type\":\"text\"}}}}");

        String queryJson = String.format(
            Locale.ROOT,
            "{" + "\"query\": {" + "\"agentic_search\": {" + "\"query_text\": \"%s\"" + "}" + "}" + "}",
            TEST_QUERY_TEXT
        );

        Request searchRequest = new Request("POST", "/" + TEST_INDEX + "/_search");
        searchRequest.setJsonEntity(queryJson);

        try {
            client().performRequest(searchRequest);
            fail("Expected parsing exception for missing agent_id");
        } catch (Exception e) {
            assertTrue(
                "Should contain agent_id required error",
                e.getMessage().contains("agent_id") && e.getMessage().contains("required")
            );
        }
    }

    public void testAgenticSearchQuery_withQueryFields_thenSuccess() throws Exception {
        createIndex(TEST_INDEX, "{\"mappings\":{\"properties\":{\"passage_text\":{\"type\":\"text\"}}}}");
        addDocument(TEST_INDEX, "1", "passage_text", "This is about science and technology", null, null);

        String queryJson = String.format(
            Locale.ROOT,
            "{"
                + "\"query\": {"
                + "\"agentic_search\": {"
                + "\"query_text\": \"%s\","
                + "\"agent_id\": \"%s\","
                + "\"query_fields\": [\"title\", \"content\"]"
                + "}"
                + "}"
                + "}",
            TEST_QUERY_TEXT,
            TEST_AGENT_ID
        );

        Request searchRequest = new Request("POST", "/" + TEST_INDEX + "/_search");
        searchRequest.setJsonEntity(queryJson);

        // TODO Should succeed because the hardcoded DSL query is executed
        Response response = client().performRequest(searchRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }
}
