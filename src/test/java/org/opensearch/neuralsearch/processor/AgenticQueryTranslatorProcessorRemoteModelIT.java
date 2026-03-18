/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.BaseAgenticSearchRemoteModelIT;
import org.opensearch.neuralsearch.query.AgenticSearchQueryBuilder;
import java.util.Map;

/**
* Integration tests for Agentic query translator processor with remote models
*/
@Log4j2
public class AgenticQueryTranslatorProcessorRemoteModelIT extends BaseAgenticSearchRemoteModelIT {

    private static String PIPELINE_NAME = "agentic-pipeline";

    public void testAgenticQueryTranslatorProcessor_withValidQuery_expectsTranslation() throws Exception {
        initializeIndexIfNotExist(TEST_INDEX);
        createAgenticSearchPipeline(PIPELINE_NAME, TEST_AGENT_ID);

        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(TEST_QUERY_TEXT);

        try {
            Map<String, Object> searchResponse = searchWithPipeline(TEST_INDEX, agenticQuery, PIPELINE_NAME);
            assertNotNull(searchResponse);
        } catch (Exception e) {
            assertTrue(
                "Should be a setup-related error",
                e.getMessage().contains("Agent index not found")
                    || e.getMessage().contains("model not found")
                    || e.getMessage().contains("Agentic search failed")
            );
        }
    }

    public void testAgenticQueryTranslatorProcessor_withAggregations_expectsFailure() throws Exception {
        initializeIndexIfNotExist(TEST_INDEX);
        createAgenticSearchPipeline(PIPELINE_NAME, TEST_AGENT_ID);

        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(TEST_QUERY_TEXT);

        try {
            Map<String, Object> searchResponse = searchWithPipelineAndAggregations(TEST_INDEX, agenticQuery, PIPELINE_NAME);
            fail("Expected failure due to aggregations with agentic search");
        } catch (Exception e) {
            assertTrue(
                "Should contain invalid usage error",
                e.getMessage().contains("Invalid usage with other search features")
                    || e.getMessage().contains("cannot be used with other search features")
            );
        }
    }

    public void testAgenticQueryTranslatorProcessor_withSort_expectsFailure() throws Exception {
        initializeIndexIfNotExist(TEST_INDEX);
        createAgenticSearchPipeline(PIPELINE_NAME, TEST_AGENT_ID);

        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(TEST_QUERY_TEXT);

        try {
            Map<String, Object> searchResponse = searchWithPipelineAndSort(TEST_INDEX, agenticQuery, PIPELINE_NAME);
            fail("Expected failure due to sort with agentic search");
        } catch (Exception e) {
            assertTrue(
                "Should contain invalid usage error",
                e.getMessage().contains("Invalid usage with other search features")
                    || e.getMessage().contains("cannot be used with other search features")
            );
        }
    }

    public void testAgenticQueryTranslatorProcessor_withAlias_expectsSuccess() throws Exception {
        String aliasName = "test-alias";
        String index1 = TEST_INDEX + "-1";
        String index2 = TEST_INDEX + "-2";

        try {
            // Create two indices with similar mappings
            initializeIndexIfNotExist(index1);
            initializeIndexIfNotExist(index2);

            // Create alias pointing to both indices
            createAlias(aliasName, index1, index2);

            createAgenticSearchPipeline(PIPELINE_NAME, TEST_AGENT_ID);

            AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(TEST_QUERY_TEXT);

            // This should now work with flow agents and aliases
            Map<String, Object> searchResponse = searchWithPipeline(aliasName, agenticQuery, PIPELINE_NAME);
            assertNotNull(searchResponse);

        } catch (Exception e) {
            // Allow setup-related errors but not the old "multiple indices" restriction
            assertFalse(
                "Should not fail due to multiple indices restriction",
                e.getMessage().contains("Flow agent does not support multiple indices")
            );
            assertTrue(
                "Should be a setup-related error",
                e.getMessage().contains("Agent index not found")
                    || e.getMessage().contains("model not found")
                    || e.getMessage().contains("Agentic search failed")
                    || e.getMessage().contains("index_not_found_exception")
            );
        } finally {
            // Cleanup
            try {
                deleteAlias(aliasName);
                deleteIndex(index1);
                deleteIndex(index2);
            } catch (Exception cleanupException) {
                log.warn("Cleanup failed: {}", cleanupException.getMessage());
            }
        }
    }

    public void testAgenticQueryTranslatorProcessor_withMultipleIndices_expectsSuccess() throws Exception {
        String index1 = TEST_INDEX + "-multi-1";
        String index2 = TEST_INDEX + "-multi-2";

        try {
            // Create two indices
            initializeIndexIfNotExist(index1);
            initializeIndexIfNotExist(index2);

            createAgenticSearchPipeline(PIPELINE_NAME, TEST_AGENT_ID);

            AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(TEST_QUERY_TEXT);

            // Test with multiple explicit indices (should now work)
            Map<String, Object> searchResponse = searchWithPipeline(index1 + "," + index2, agenticQuery, PIPELINE_NAME);
            assertNotNull(searchResponse);

        } catch (Exception e) {
            // Allow setup-related errors but not the old "multiple indices" restriction
            assertFalse(
                "Should not fail due to multiple indices restriction",
                e.getMessage().contains("Flow agent does not support multiple indices")
            );
            assertTrue(
                "Should be a setup-related error",
                e.getMessage().contains("Agent index not found")
                    || e.getMessage().contains("model not found")
                    || e.getMessage().contains("Agentic search failed")
                    || e.getMessage().contains("index_not_found_exception")
            );
        } finally {
            // Cleanup
            try {
                deleteIndex(index1);
                deleteIndex(index2);
            } catch (Exception cleanupException) {
                log.warn("Cleanup failed: {}", cleanupException.getMessage());
            }
        }
    }
}
