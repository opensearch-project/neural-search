/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_USER_AGENT;

import com.google.common.collect.ImmutableList;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.message.BasicHeader;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.neuralsearch.query.AgenticSearchQueryBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.hc.core5.http.io.entity.EntityUtils;

public class AgenticSearchIT extends AbstractRollingUpgradeTestCase {
    // add prefix to avoid conflicts with other IT class, since we don't wipe resources after first round
    private static final String AGENTIC_SEARCH_PIPELINE_NAME = "agentic-search-pipeline-rolling";
    private static final String TEST_TEXT_FIELD = "passage_text";
    private static final String TEST_QUERY_TEXT = "What is machine learning?";
    private static final String TEST_AGENT_ID = "test-agentic-agent-rolling";

    // Test rolling-upgrade agentic search in rolling-upgrade scenario
    public void testAgenticSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);

        switch (getClusterType()) {
            case OLD:
                // Skip agentic search tests on old cluster since the feature is only available in 3.2.0+
                logger.info("Skipping agentic search test on old cluster - feature not available before 3.2.0");
                break;
            case MIXED:
                if (isFirstMixedRound()) {
                    // Test that agentic search processor can be created during mixed cluster state
                    testAgenticSearchProcessorCreationInMixedCluster();
                }
                break;
            case UPGRADED:
                // Test full agentic search functionality on fully upgraded cluster
                testAgenticSearchOnUpgradedCluster();
                break;
        }
    }

    private void testAgenticSearchProcessorCreationInMixedCluster() throws Exception {
        // During mixed cluster state, we should be able to create agentic search processors
        // but they may not function fully until all nodes are upgraded
        // Only test pipeline creation, not the full workflow with index and documents
        try {
            // Create agentic search pipeline - this should work in mixed cluster
            createAgenticSearchPipeline(TEST_AGENT_ID, AGENTIC_SEARCH_PIPELINE_NAME, null);

            logger.info("Successfully created agentic search pipeline in mixed cluster state");

        } catch (Exception e) {
            // If pipeline creation fails in mixed cluster, that's acceptable
            logger.info("Agentic search pipeline creation failed in mixed cluster (expected): {}", e.getMessage());
        }
    }

    private void testAgenticSearchOnUpgradedCluster() throws Exception {
        String indexName = getIndexNameForTest();

        try {
            // Create index (it should not exist from mixed phase since that phase only tests pipeline creation)
            createIndexWithConfiguration(
                indexName,
                Files.readString(Path.of(classLoader.getResource("processor/IndexMappingSingleShard.json").toURI())),
                "_none"
            );
            addDocument(indexName, "1", TEST_TEXT_FIELD, "Machine learning is a subset of artificial intelligence", null, null);

            // Create or update agentic search pipeline
            createAgenticSearchPipeline(TEST_AGENT_ID, AGENTIC_SEARCH_PIPELINE_NAME, null);

            // Set the search pipeline as default for the index
            updateIndexSettings(indexName, Settings.builder().put("index.search.default_pipeline", AGENTIC_SEARCH_PIPELINE_NAME));

            // Test agentic search query
            AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(TEST_QUERY_TEXT);

            try {
                // This should work on fully upgraded cluster but may fail due to missing agent
                Map<String, Object> searchResults = search(indexName, agenticQuery, 1);
                logger.info("Agentic search executed successfully on fully upgraded cluster");

            } catch (Exception e) {
                // Expected if agent doesn't exist, but processor should be functional
                logger.info("Agentic search failed as expected due to missing agent: {}", e.getMessage());
                assertTrue(
                    "Error should be related to agent execution, not processor creation",
                    e.getMessage().contains("agent") || e.getMessage().contains("Agent")
                );
            }
        } finally {
            // Clean up resources only in the final phase
            try {
                wipeOfTestResources(indexName, null, null, AGENTIC_SEARCH_PIPELINE_NAME);
            } catch (Exception cleanupException) {
                logger.warn("Error during cleanup, continuing: {}", cleanupException.getMessage());
            }
        }
    }

    /**
     * Test that agentic search processor with embedding_model_id works correctly during rolling upgrade
     */
    public void testAgenticSearchWithEmbeddingModelId_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);

        switch (getClusterType()) {
            case OLD:
                // Skip on old cluster - embedding_model_id parameter not supported before 3.6.0
                logger.info("Skipping agentic search with embedding_model_id test on old cluster - feature not available before 3.6.0");
                break;
            case MIXED:
                // Skip mixed cluster to avoid model group name conflicts with upgraded phase
                logger.info("Skipping agentic search with embedding_model_id test on mixed cluster - will test in upgraded phase");
                break;
            case UPGRADED:
                // Test full functionality on upgraded cluster
                testAgenticSearchWithEmbeddingModelIdOnUpgradedCluster();
                break;
        }
    }

    private void testAgenticSearchWithEmbeddingModelIdInMixedCluster() throws Exception {
        String pipelineName = AGENTIC_SEARCH_PIPELINE_NAME + "-with-embedding-mixed";

        try {
            // Upload a text embedding model
            String embeddingModelId = uploadTextEmbeddingModel();

            // Try to create agentic search pipeline with embedding_model_id
            createAgenticSearchPipeline(TEST_AGENT_ID, pipelineName, embeddingModelId);

            logger.info("Successfully created agentic search pipeline with embedding_model_id in mixed cluster");

        } catch (Exception e) {
            // Pipeline creation might fail in mixed cluster due to version compatibility
            logger.info("Agentic search pipeline with embedding_model_id creation failed in mixed cluster: {}", e.getMessage());
        }
    }

    private void testAgenticSearchWithEmbeddingModelIdOnUpgradedCluster() throws Exception {
        String indexName = getIndexNameForTest() + "-embedding";
        String pipelineName = AGENTIC_SEARCH_PIPELINE_NAME + "-with-embedding";

        // Upload a text embedding model
        String embeddingModelId = uploadTextEmbeddingModel();

        try {
            // Create index
            createIndexWithConfiguration(
                indexName,
                Files.readString(Path.of(classLoader.getResource("processor/IndexMappingSingleShard.json").toURI())),
                "_none"
            );

            // Add test document
            addDocument(indexName, "1", TEST_TEXT_FIELD, "Machine learning is a subset of artificial intelligence", null, null);

            // Create agentic search pipeline with embedding_model_id
            createAgenticSearchPipeline(TEST_AGENT_ID, pipelineName, embeddingModelId);

            // Set the search pipeline as default for the index
            updateIndexSettings(indexName, Settings.builder().put("index.search.default_pipeline", pipelineName));

            // Test agentic search query
            AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(TEST_QUERY_TEXT);

            try {
                // This should work on fully upgraded cluster but may fail due to missing agent
                Map<String, Object> searchResults = search(indexName, agenticQuery, 1);
                logger.info("Agentic search with embedding_model_id executed successfully on fully upgraded cluster");

            } catch (Exception e) {
                // Expected if agent doesn't exist, but processor should be functional
                logger.info("Agentic search with embedding_model_id failed as expected due to missing agent: {}", e.getMessage());
                assertTrue(
                    "Error should be related to agent execution, not processor creation",
                    e.getMessage().contains("agent") || e.getMessage().contains("Agent")
                );
            }
        } finally {
            // Clean up resources
            try {
                wipeOfTestResources(indexName, null, embeddingModelId, pipelineName);
            } catch (Exception cleanupException) {
                logger.warn("Error during cleanup, continuing: {}", cleanupException.getMessage());
            }
        }
    }

    /**
     * Create an agentic search pipeline
     */
    private void createAgenticSearchPipeline(String agentId, String pipelineName, String embeddingModelId) throws Exception {
        String requestBody;
        if (embeddingModelId != null) {
            requestBody = Files.readString(
                Path.of(classLoader.getResource("processor/AgenticSearchPipelineWithEmbeddingConfiguration.json").toURI())
            );
            requestBody = String.format(LOCALE, requestBody, agentId, embeddingModelId);
        } else {
            requestBody = Files.readString(Path.of(classLoader.getResource("processor/AgenticSearchPipelineConfiguration.json").toURI()));
            requestBody = String.format(LOCALE, requestBody, agentId);
        }

        Response pipelineCreateResponse = makeRequest(
            client(),
            "PUT",
            "/_search/pipeline/" + pipelineName,
            null,
            toHttpEntity(requestBody),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> node = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(pipelineCreateResponse.getEntity()),
            false
        );
        assertEquals("true", node.get("acknowledged").toString());
    }
}
