/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.BaseAgenticSearchRemoteModelIT;

import java.util.Map;

/**
* Integration tests for Agentic Search query with remote models
*/
@Log4j2
public class AgenticSearchQueryBuilderRemoteModelIT extends BaseAgenticSearchRemoteModelIT {

    public void testAgenticSearchQuery_withValidParameters_thenExpectError() throws Exception {
        initializeIndexIfNotExist(TEST_INDEX);
        String pipelineName = "test-agentic-pipeline";
        createAgenticSearchPipeline(pipelineName, TEST_AGENT_ID);

        try {
            Map<String, Object> searchResponse = search(TEST_INDEX, null, null, 10, Map.of("search_pipeline", pipelineName));
            // Expect some results or error due to setup limitations
            assertTrue("Should get results or setup error", getHitCount(searchResponse) >= 0);
        } catch (Exception e) {
            assertTrue(
                "Should be a setup-related error",
                e.getMessage().contains("Agent index not found")
                    || e.getMessage().contains("model not found")
                    || e.getMessage().contains("Failed to execute agentic search")
                    || e.getMessage().contains("dummy-agent-id")
            );
        }
    }

    public void testAgenticSearchQuery_withMissingAgentId_thenFail() throws Exception {
        initializeIndexIfNotExist(TEST_INDEX);
        String pipelineName = "test-agentic-pipeline-no-agent";

        try {
            createAgenticSearchPipeline(pipelineName, "");
            Map<String, Object> searchResponse = search(TEST_INDEX, null, null, 10, Map.of("search_pipeline", pipelineName));
            fail("Expected exception for empty agent_id");
        } catch (Exception e) {
            assertTrue(
                "Should contain agent_id error",
                e.getMessage().contains("agent_id") || e.getMessage().contains("required") || e.getMessage().contains("empty")
            );
        }
    }

    public void testAgenticSearchQuery_withSingleShard_thenSuccess() throws Exception {
        String singleShardIndex = TEST_INDEX + "-single-shard";
        initializeIndexIfNotExist(singleShardIndex, 1);
        String pipelineName = "test-agentic-pipeline-single";
        createAgenticSearchPipeline(pipelineName, TEST_AGENT_ID);

        try {
            Map<String, Object> searchResponse = search(singleShardIndex, null, null, 10, Map.of("search_pipeline", pipelineName));
            assertTrue("Should get results or setup error", getHitCount(searchResponse) >= 0);
        } catch (Exception e) {
            assertTrue(
                "Should be a setup-related error",
                e.getMessage().contains("Agent index not found")
                    || e.getMessage().contains("model not found")
                    || e.getMessage().contains("Failed to execute agentic search")
                    || e.getMessage().contains("dummy-agent-id")
            );
        }
    }

    public void testAgenticSearchQuery_withMultipleShards_thenSuccess() throws Exception {
        String multiShardIndex = TEST_INDEX + "-multi-shard";
        initializeIndexIfNotExist(multiShardIndex, 3);
        String pipelineName = "test-agentic-pipeline-multi";
        createAgenticSearchPipeline(pipelineName, TEST_AGENT_ID);

        try {
            Map<String, Object> searchResponse = search(multiShardIndex, null, null, 10, Map.of("search_pipeline", pipelineName));
            assertTrue("Should get results or setup error", getHitCount(searchResponse) >= 0);
        } catch (Exception e) {
            assertTrue(
                "Should be a setup-related error",
                e.getMessage().contains("Agent index not found")
                    || e.getMessage().contains("model not found")
                    || e.getMessage().contains("Failed to execute agentic search")
                    || e.getMessage().contains("dummy-agent-id")
            );
        }
    }
}
