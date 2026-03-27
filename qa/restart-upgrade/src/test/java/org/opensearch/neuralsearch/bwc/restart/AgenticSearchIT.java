/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.restart;

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

public class AgenticSearchIT extends AbstractRestartUpgradeRestTestCase {
    // add prefix to avoid conflicts with other IT class, since we don't wipe resources after first round
    private static final String AGENTIC_SEARCH_PIPELINE_NAME = "agentic-search-pipeline";
    private static final String TEST_TEXT_FIELD = "passage_text";
    private static final String TEST_QUERY_TEXT = "What is machine learning?";
    private static final String TEST_AGENT_ID = "test-agentic-agent";

    // Test restart-upgrade agentic search in restart-upgrade scenario
    public void testAgenticSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);

        if (isRunningAgainstOldCluster()) {
            // Skip agentic search tests on old cluster since the feature is only available in 3.2.0+
            // This test validates that the feature works correctly after upgrade
            logger.info("Skipping agentic search test on old cluster - feature not available before 3.2.0");
            return;
        } else {
            // Test agentic search functionality on upgraded cluster
            testAgenticSearchOnUpgradedCluster();
        }
    }

    private void testAgenticSearchOnUpgradedCluster() throws Exception {
        // Create a simple index for testing
        String indexName = getIndexNameForTest();
        createIndexWithConfiguration(
            indexName,
            Files.readString(Path.of(classLoader.getResource("processor/IndexMappingSingleShard.json").toURI())),
            "_none"
        );

        // Add test document
        addDocument(indexName, "1", TEST_TEXT_FIELD, "Machine learning is a subset of artificial intelligence", null, null);

        // Create agentic search pipeline without embedding_model_id (BWC test)
        createAgenticSearchPipeline(TEST_AGENT_ID, AGENTIC_SEARCH_PIPELINE_NAME, null);

        // Set the search pipeline as default for the index
        updateIndexSettings(indexName, Settings.builder().put("index.search.default_pipeline", AGENTIC_SEARCH_PIPELINE_NAME));

        // Test agentic search query
        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(TEST_QUERY_TEXT);

        try {
            // This should work on upgraded cluster (3.6.0+) but may fail due to missing agent
            // The important thing is that the processor is created and pipeline works
            Map<String, Object> searchResults = search(indexName, agenticQuery, 1);

            // If we get here, the agentic search processor is working
            logger.info("Agentic search executed successfully on upgraded cluster");

        } catch (Exception e) {
            // Expected if agent doesn't exist, but processor should be functional
            logger.info("Agentic search failed as expected due to missing agent: {}", e.getMessage());
            assertTrue(
                "Error should be related to agent execution, not processor creation",
                e.getMessage().contains("agent") || e.getMessage().contains("Agent")
            );
        } finally {
            // Clean up resources - handle case where pipeline might not exist
            try {
                wipeOfTestResources(indexName, null, null, AGENTIC_SEARCH_PIPELINE_NAME);
            } catch (Exception cleanupException) {
                logger.warn("Error during cleanup, continuing: {}", cleanupException.getMessage());
            }
        }
    }

    /**
     * Test that agentic search processor with embedding_model_id works correctly on upgraded cluster
     */
    public void testAgenticSearchWithEmbeddingModelId_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);

        if (isRunningAgainstOldCluster()) {
            // Skip on old cluster - embedding_model_id parameter not supported before 3.6.0
            logger.info("Skipping agentic search with embedding_model_id test on old cluster - feature not available before 3.6.0");
            return;
        } else {
            // Test agentic search with embedding_model_id on upgraded cluster
            testAgenticSearchWithEmbeddingModelIdOnUpgradedCluster();
        }
    }

    private void testAgenticSearchWithEmbeddingModelIdOnUpgradedCluster() throws Exception {
        String indexName = getIndexNameForTest();
        String pipelineName = AGENTIC_SEARCH_PIPELINE_NAME + "-with-embedding";

        // Upload a text embedding model for testing
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

            // Create agentic search pipeline with embedding_model_id (new feature in 3.6.0+)
            createAgenticSearchPipeline(TEST_AGENT_ID, pipelineName, embeddingModelId);

            // Set the search pipeline as default for the index
            updateIndexSettings(indexName, Settings.builder().put("index.search.default_pipeline", pipelineName));

            // Test agentic search query
            AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(TEST_QUERY_TEXT);

            try {
                // This should work on upgraded cluster (3.6.0+) but may fail due to missing agent
                Map<String, Object> searchResults = search(indexName, agenticQuery, 1);
                logger.info("Agentic search with embedding_model_id executed successfully on upgraded cluster");

            } catch (Exception e) {
                // Expected if agent doesn't exist, but processor should be functional
                logger.info("Agentic search with embedding_model_id failed as expected due to missing agent: {}", e.getMessage());
                assertTrue(
                    "Error should be related to agent execution, not processor creation",
                    e.getMessage().contains("agent") || e.getMessage().contains("Agent")
                );
            }
        } finally {
            // Clean up resources - handle case where pipeline might not exist
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

        logger.info("Creating agentic search pipeline '{}' with request body: {}", pipelineName, requestBody);

        Response pipelineCreateResponse = makeRequest(
            client(),
            "PUT",
            "/_search/pipeline/" + pipelineName,
            null,
            toHttpEntity(requestBody),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );

        String responseBody = EntityUtils.toString(pipelineCreateResponse.getEntity());
        logger.info(
            "Pipeline creation response status: {}, body: {}",
            pipelineCreateResponse.getStatusLine().getStatusCode(),
            responseBody
        );

        // Check if the request was successful
        if (pipelineCreateResponse.getStatusLine().getStatusCode() != 200) {
            throw new IllegalStateException(
                "Pipeline creation failed with status " + pipelineCreateResponse.getStatusLine().getStatusCode() + ": " + responseBody
            );
        }

        Map<String, Object> node = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        if (!"true".equals(node.get("acknowledged").toString())) {
            throw new IllegalStateException("Pipeline creation was not acknowledged: " + responseBody);
        }

        logger.info("Successfully created agentic search pipeline: {}", pipelineName);
    }

    @Override
    protected void wipeOfTestResources(
        final String indexName,
        final String ingestPipeline,
        final String modelId,
        final String searchPipeline
    ) {
        // Handle cleanup more gracefully for BWC tests
        if (ingestPipeline != null) {
            try {
                deleteIngestPipeline(ingestPipeline);
            } catch (Exception e) {
                logger.warn("Failed to delete ingest pipeline '{}': {}", ingestPipeline, e.getMessage());
            }
        }
        if (searchPipeline != null) {
            try {
                deleteSearchPipeline(searchPipeline);
            } catch (Exception e) {
                logger.warn("Failed to delete search pipeline '{}': {}", searchPipeline, e.getMessage());
            }
        }
        if (indexName != null) {
            try {
                deleteIndex(indexName);
            } catch (Exception e) {
                logger.warn("Failed to delete index '{}': {}", indexName, e.getMessage());
            }
        }
        if (modelId != null) {
            try {
                deleteModel(modelId);
            } catch (Exception e) {
                logger.warn("Failed to delete model '{}': {}", modelId, e.getMessage());
            }
        }
    }
}
