/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import com.google.common.collect.ImmutableList;
import org.opensearch.neuralsearch.query.AgenticSearchQueryBuilder;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.neuralsearch.util.RemoteModelTestUtils;

import org.junit.After;
import org.junit.Before;
import java.io.IOException;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.message.BasicHeader;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_USER_AGENT;

/**
 * Base class for Agentic Search Remote Model integration tests.
 */
@Log4j2
public abstract class BaseAgenticSearchRemoteModelIT extends BaseNeuralSearchIT {

    protected static final String TEST_INDEX = "test-agentic-index";
    protected static final String TEST_QUERY_TEXT = "Find documents about machine learning";

    protected static String TEST_AGENT_ID;
    protected static String TEST_MODEL_ID;
    protected static String TEST_CONNECTOR_ID;
    protected static final String ML_COMMONS_AGENTIC_SEARCH_ENABLED = "plugins.ml_commons.agentic_search_enabled";

    protected boolean isTorchServeAvailable = false;
    protected String torchServeEndpoint;

    @Before
    @SneakyThrows
    @Override
    public void setUp() {
        super.setUp();
        updateClusterSettings();
        updateClusterSettings("plugins.ml_commons.connector.private_ip_enabled", true);
        updateClusterSettings(
            "plugins.ml_commons.trusted_connector_endpoints_regex",
            List.of(
                "^https://runtime\\.sagemaker\\..*[a-z0-9-]\\.amazonaws\\.com/.*$",
                "^http://localhost:.*",
                "^http://127\\.0\\.0\\.1:.*",
                "^http://torchserve:.*"
            )
        );

        // Check for TorchServe endpoint from environment or system properties
        torchServeEndpoint = System.getenv("TORCHSERVE_ENDPOINT");
        if (torchServeEndpoint == null || torchServeEndpoint.isEmpty()) {
            torchServeEndpoint = System.getProperty("tests.torchserve.endpoint");
        }

        if (torchServeEndpoint != null && !torchServeEndpoint.isEmpty()) {
            isTorchServeAvailable = RemoteModelTestUtils.isRemoteEndpointAvailable(torchServeEndpoint);
            if (isTorchServeAvailable) {
                log.info("TorchServe is available at: {}", torchServeEndpoint);

                if (TEST_AGENT_ID == null) {
                    try {
                        // Create connector and deploy remote models
                        TEST_CONNECTOR_ID = createTestConnector(torchServeEndpoint);
                        log.info("Created remote model connector, connector ID: {}", TEST_CONNECTOR_ID);

                        TEST_MODEL_ID = RemoteModelTestUtils.deployRemoteModel(client(), TEST_CONNECTOR_ID, "agentic-search-remote");
                        log.info("Deployed remote agentic search model, model ID: {}", TEST_MODEL_ID);

                        TEST_AGENT_ID = registerTestAgent(TEST_MODEL_ID);
                        log.info("Registered agent, agent ID: {}", TEST_AGENT_ID);

                    } catch (Exception e) {
                        TEST_AGENT_ID = "dummy-agent-id";
                        TEST_MODEL_ID = "dummy-model-id";
                        TEST_CONNECTOR_ID = "dummy-connector-id";
                    }
                }
            } else {
                log.info("TorchServe not available at {}, tests will be skipped", torchServeEndpoint);
            }
        } else {
            log.info("No TorchServe endpoint configured, tests will be skipped");
        }
    }

    @After
    @SneakyThrows
    @Override
    public void tearDown() {
        if (isTorchServeAvailable) {
            // Delete agent
            try {
                deleteAgent(TEST_AGENT_ID);
            } catch (Exception e) {
                log.debug("Failed to delete agent: {}", e.getMessage());
            }
            // Cleanup indexes
            try {
                deleteIndex(TEST_INDEX);
            } catch (Exception e) {
                log.debug("Failed to delete index: {}", e.getMessage());
            }

            // Cleanup connector and models
            RemoteModelTestUtils.deleteModel(client(), TEST_MODEL_ID);
            RemoteModelTestUtils.deleteConnector(client(), TEST_CONNECTOR_ID);
        }

        super.tearDown();
    }

    public void initializeIndexIfNotExist(String indexName) throws Exception {
        initializeIndexIfNotExist(indexName, 1);
    }

    public void initializeIndexIfNotExist(String indexName, int numberOfShards) throws Exception {
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

    public String createTestConnector(String endpoint) throws Exception {
        try {
            String createConnectorRequestBody = Files.readString(
                Path.of(classLoader.getResource("agenticsearch/CreateConnectorRequestBody.json").toURI())
            );
            // Replace placeholders with actual values
            String requestBody = String.format(Locale.ROOT, createConnectorRequestBody, endpoint);
            return createConnector(requestBody);
        } catch (Exception e) {
            throw new IOException("Failed to load connector template from resources", e);
        }
    }

    public void createAgenticSearchPipeline(String pipelineName, String agentId) throws Exception {
        final String pipelineRequestBody = String.format(
            Locale.ROOT,
            Files.readString(Path.of(classLoader.getResource("agenticsearch/AgenticSearchPipelineRequestBody.json").toURI())),
            agentId
        );
        makeRequest(
            client(),
            "PUT",
            String.format(Locale.ROOT, "/_search/pipeline/%s", pipelineName),
            null,
            toHttpEntity(pipelineRequestBody),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
    }

    public String registerTestAgent(String modelId) throws Exception {
        final String registerAgentRequestBody = String.format(
            Locale.ROOT,
            Files.readString(Path.of(classLoader.getResource("agenticsearch/RegisterAgentRequestBody.json").toURI())),
            modelId
        );
        return registerAgent(registerAgentRequestBody);
    }

    public Map<String, Object> searchWithPipeline(String indexName, AgenticSearchQueryBuilder query, String pipelineName) throws Exception {
        return search(indexName, query, null, 10, Map.of("search_pipeline", pipelineName), null);
    }

    public Map<String, Object> searchWithPipelineAndAggregations(String indexName, AgenticSearchQueryBuilder query, String pipelineName)
        throws Exception {
        String searchBody = String.format(Locale.ROOT, """
            {
              "query": {
                "agentic": {
                  "query_text": "%s"
                }
              },
              "aggs": {
                "test_agg": {
                  "terms": {
                    "field": "passage_text.keyword"
                  }
                }
              }
            }
            """, query.getQueryText());

        Response response = makeRequest(
            client(),
            "GET",
            "/" + indexName + "/_search",
            Map.of("search_pipeline", pipelineName),
            toHttpEntity(searchBody),
            null
        );
        String responseBody = EntityUtils.toString(response.getEntity());
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);
    }

    public Map<String, Object> searchWithPipelineAndSort(String indexName, AgenticSearchQueryBuilder query, String pipelineName)
        throws Exception {
        String searchBody = String.format(Locale.ROOT, """
            {
              "query": {
                "agentic": {
                  "query_text": "%s"
                }
              },
              "sort": [
                {"_score": {"order": "desc"}}
              ]
            }
            """, query.getQueryText());

        Response response = makeRequest(
            client(),
            "GET",
            "/" + indexName + "/_search",
            Map.of("search_pipeline", pipelineName),
            toHttpEntity(searchBody),
            null
        );
        String responseBody = EntityUtils.toString(response.getEntity());
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);
    }

}
