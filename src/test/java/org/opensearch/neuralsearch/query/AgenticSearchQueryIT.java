/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.junit.After;
import org.junit.Before;
import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

@AwaitsFix(bugUrl = "Ignoring until we find a way to fetch access, secret key for remote model")
public class AgenticSearchQueryIT extends BaseNeuralSearchIT {

    private static final String TEST_INDEX = "test-agentic-index";
    private static String TEST_AGENT_ID;
    private static String TEST_MODEL_ID;
    private static final String TEST_QUERY_TEXT = "Find documents about machine learning";
    private static final String AWS_ACCESS_KEY_ID = System.getenv("AWS_ACCESS_KEY_ID");

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
        if (TEST_AGENT_ID == null) {
            try {
                String connectorId = createTestConnector();
                TEST_MODEL_ID = registerAndDeployTestModel(connectorId);
                TEST_AGENT_ID = registerTestAgent(TEST_MODEL_ID);
            } catch (Exception e) {
                TEST_AGENT_ID = "dummy-agent-id";
                TEST_MODEL_ID = "dummy-model-id";
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        // if (TEST_MODEL_ID != null && !TEST_MODEL_ID.equals("dummy-model-id")) {
        // undeployModel(TEST_MODEL_ID);
        // }
        if (TEST_AGENT_ID != null && !TEST_AGENT_ID.equals("dummy-agent-id")) {
            deleteAgent(TEST_AGENT_ID);
        }
        super.tearDown();
    }

    public void testAgenticSearchQuery_withValidParameters_thenExpectError() throws Exception {
        initializeIndexIfNotExist(TEST_INDEX);

        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(TEST_QUERY_TEXT).agentId(TEST_AGENT_ID);

        Map<String, Object> searchResponse = search(TEST_INDEX, agenticQuery, null, 10, Map.of());
        assertEquals(1, getHitCount(searchResponse));
    }

    public void testAgenticSearchQuery_withMissingQueryText_thenFail() throws Exception {
        initializeIndexIfNotExist(TEST_INDEX);

        try {
            // This should fail during query builder construction
            AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().agentId(TEST_AGENT_ID);
            search(TEST_INDEX, agenticQuery, null, 10, Map.of());
            fail("Expected parsing exception for missing query_text");
        } catch (Exception e) {
            assertTrue(
                "Should contain query_text required error",
                e.getMessage().contains("query_text") || e.getMessage().contains("required")
            );
        }
    }

    public void testAgenticSearchQuery_withMissingAgentId_thenFail() throws Exception {
        initializeIndexIfNotExist(TEST_INDEX);

        try {
            // This should fail during query builder construction
            AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(TEST_QUERY_TEXT);
            search(TEST_INDEX, agenticQuery, null, 10, Map.of());
            fail("Expected parsing exception for missing agent_id");
        } catch (Exception e) {
            assertTrue(
                "Should contain agent_id required error",
                e.getMessage().contains("agent_id") || e.getMessage().contains("required")
            );
        }
    }

    public void testAgenticSearchQuery_withQueryFields_thenExpectError() throws Exception {
        initializeIndexIfNotExist(TEST_INDEX);

        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(TEST_QUERY_TEXT)
            .agentId(TEST_AGENT_ID)
            .queryFields(java.util.Arrays.asList("title", "content"));

        try {
            Map<String, Object> searchResponse = search(TEST_INDEX, agenticQuery, null, 10, Map.of());
            fail("Expected error due to setup limitations");
        } catch (Exception e) {
            assertTrue(
                "Should be a setup-related error",
                e.getMessage().contains("Agent index not found")
                    || e.getMessage().contains("model not found")
                    || e.getMessage().contains("Failed to execute agentic search")
            );
        }
    }

    public void testAgenticSearchQuery_withSingleShard_thenSuccess() throws Exception {
        String singleShardIndex = TEST_INDEX + "-single-shard";
        initializeIndexIfNotExist(singleShardIndex, 1);

        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(TEST_QUERY_TEXT).agentId(TEST_AGENT_ID);

        Map<String, Object> searchResponse = search(singleShardIndex, agenticQuery, null, 10, Map.of());
        assertEquals(1, getHitCount(searchResponse));
    }

    public void testAgenticSearchQuery_withMultipleShards_thenSuccess() throws Exception {
        String multiShardIndex = TEST_INDEX + "-multi-shard";
        initializeIndexIfNotExist(multiShardIndex, 3);

        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(TEST_QUERY_TEXT).agentId(TEST_AGENT_ID);

        Map<String, Object> searchResponse = search(multiShardIndex, agenticQuery, null, 10, Map.of());
        assertEquals(1, getHitCount(searchResponse));
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

    private String createTestConnector() throws Exception {
        final String createConnectorRequestBody = Files.readString(
            Path.of(classLoader.getResource("agenticsearch/CreateConnectorRequestBody.json").toURI())
        );
        return createConnector(createConnectorRequestBody);
    }

    private String registerAndDeployTestModel(String connectorId) throws Exception {
        final String registerModelRequestBody = String.format(
            Locale.ROOT,
            Files.readString(Path.of(classLoader.getResource("agenticsearch/RegisterModelRequestBody.json").toURI())),
            connectorId
        );
        return registerModelGroupAndUploadModel(registerModelRequestBody);
    }

    private String registerTestAgent(String modelId) throws Exception {
        final String registerAgentRequestBody = String.format(
            Locale.ROOT,
            Files.readString(Path.of(classLoader.getResource("agenticsearch/RegisterAgentRequestBody.json").toURI())),
            modelId
        );
        return registerAgent(registerAgentRequestBody);
    }

}
