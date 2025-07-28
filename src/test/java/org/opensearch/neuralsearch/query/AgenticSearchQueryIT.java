/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;

public class AgenticSearchQueryIT extends BaseNeuralSearchIT {

    private static final String TEST_INDEX = "test-agentic-index";
    private static final String TEST_AGENT_ID = "test-agent-123";
    private static final String TEST_QUERY_TEXT = "Find documents about machine learning";

    public void testAgenticSearchQuery_withValidParameters_thenSuccess() throws Exception {
        initializeIndexIfNotExist(TEST_INDEX);

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
        initializeIndexIfNotExist(TEST_INDEX);

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
        initializeIndexIfNotExist(TEST_INDEX);

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
        initializeIndexIfNotExist(TEST_INDEX);

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

    public void testAgenticSearchQuery_withSingleShard_thenSuccess() throws Exception {
        String singleShardIndex = TEST_INDEX + "-single-shard";
        initializeIndexIfNotExist(singleShardIndex, 1);

        String queryJson = String.format(
            Locale.ROOT,
            "{" + "\"query\": {" + "\"agentic_search\": {" + "\"query_text\": \"%s\"," + "\"agent_id\": \"%s\"" + "}" + "}" + "}",
            TEST_QUERY_TEXT,
            TEST_AGENT_ID
        );

        Request searchRequest = new Request("POST", "/" + singleShardIndex + "/_search");
        searchRequest.setJsonEntity(queryJson);

        Response response = client().performRequest(searchRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    public void testAgenticSearchQuery_withMultipleShards_thenSuccess() throws Exception {
        String multiShardIndex = TEST_INDEX + "-multi-shard";
        initializeIndexIfNotExist(multiShardIndex, 3);

        String queryJson = String.format(
            Locale.ROOT,
            "{" + "\"query\": {" + "\"agentic_search\": {" + "\"query_text\": \"%s\"," + "\"agent_id\": \"%s\"" + "}" + "}" + "}",
            TEST_QUERY_TEXT,
            TEST_AGENT_ID
        );

        Request searchRequest = new Request("POST", "/" + multiShardIndex + "/_search");
        searchRequest.setJsonEntity(queryJson);

        Response response = client().performRequest(searchRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void initializeIndexIfNotExist(String indexName) throws Exception {
        initializeIndexIfNotExist(indexName, 1);
    }

    private void initializeIndexIfNotExist(String indexName, int numberOfShards) throws Exception {
        if (!indexExists(indexName)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(
                    Collections.emptyList(),
                    Map.of(),
                    Collections.emptyList(),
                    Collections.singletonList("passage_text"),
                    Collections.emptyList(),
                    numberOfShards
                ),
                ""
            );
            addDocument(indexName, "1", "passage_text", "This is about science and technology", null, null);
            addDocument(indexName, "2", "passage_text", "Machine learning and artificial intelligence", null, null);
            assertEquals(2, getDocCount(indexName));
        }
    }
}
