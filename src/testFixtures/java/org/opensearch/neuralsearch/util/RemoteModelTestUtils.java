/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Utility class for managing remote model endpoints in tests
 */
@Log4j2
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RemoteModelTestUtils {
    /**
     * Check if a remote endpoint is available
     */
    public static boolean isRemoteEndpointAvailable(String endpoint) {
        if (endpoint == null || endpoint.isEmpty()) {
            return false;
        }

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(endpoint + "/ping");
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                int statusCode = response.getCode();
                log.debug("Endpoint {} returned status code: {}", endpoint, statusCode);
                return statusCode == 200;
            }
        } catch (Exception e) {
            log.debug("Endpoint {} not available: {}", endpoint, e.getMessage());
            return false;
        }
    }

    /**
     * Create a TorchServe connector for semantic highlighting
     */
    public static String createTorchServeConnector(RestClient client, String endpoint, boolean batchEnabled) throws IOException {
        String connectorName = "torchserve-semantic-highlighter-unified";
        String requestBody = createUnifiedTorchServeConnectorBody(connectorName, endpoint);

        Request request = new Request("POST", "/_plugins/_ml/connectors/_create");
        request.setJsonEntity(requestBody);

        Response response = client.performRequest(request);
        Map<String, Object> responseMap = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            response.getEntity().getContent(),
            false
        );

        return (String) responseMap.get("connector_id");
    }

    /**
     * Deploy a model using the connector
     */
    public static String deployRemoteModel(RestClient client, String connectorId, String modelName) throws IOException {
        String requestBody = String.format(Locale.ROOT, """
            {
                "name": "%s",
                "function_name": "remote",
                "connector_id": "%s"
            }
            """, modelName, connectorId);

        Request request = new Request("POST", "/_plugins/_ml/models/_register?deploy=true");
        request.setJsonEntity(requestBody);

        Response response = client.performRequest(request);
        Map<String, Object> responseMap = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            response.getEntity().getContent(),
            false
        );

        return (String) responseMap.get("model_id");
    }

    /**
     * Delete a connector
     */
    public static void deleteConnector(RestClient client, String connectorId) {
        try {
            Request request = new Request("DELETE", "/_plugins/_ml/connectors/" + connectorId);
            client.performRequest(request);
        } catch (Exception e) {
            log.debug("Failed to delete connector {}: {}", connectorId, e.getMessage());
        }
    }

    /**
     * Delete a model
     */
    public static void deleteModel(RestClient client, String modelId) {
        try {
            Request request = new Request("DELETE", "/_plugins/_ml/models/" + modelId);
            client.performRequest(request);
        } catch (Exception e) {
            log.debug("Failed to delete model {}: {}", modelId, e.getMessage());
        }
    }

    private static String createUnifiedTorchServeConnectorBody(String name, String endpoint) {
        // Unified connector that uses ${parameters.inputs} for both single and batch
        // MLCommonsClientAccessor now always provides inputs parameter as an array
        return String.format(Locale.ROOT, """
            {
                "name": "%s",
                "description": "Unified TorchServe connector for semantic highlighter (supports both single and batch)",
                "version": 1,
                "protocol": "http",
                "parameters": {},
                "credential": {
                    "key": "test"
                },
                "actions": [
                    {
                        "action_type": "predict",
                        "method": "POST",
                        "url": "%s/predictions/semantic_highlighter",
                        "headers": {
                            "Content-Type": "application/json"
                        },
                        "request_body": "${parameters.inputs}"
                    }
                ]
            }
            """, name, endpoint);
    }

    // Keep the old methods for reference but they're no longer used
    @Deprecated
    private static String createTorchServeBatchEnabledConnectorBody(String name, String endpoint) {
        return String.format(Locale.ROOT, """
            {
                "name": "%s",
                "description": "TorchServe connector for semantic highlighter with batch enabled",
                "version": 1,
                "protocol": "http",
                "parameters": {},
                "credential": {
                    "key": "test"
                },
                "actions": [
                    {
                        "action_type": "predict",
                        "method": "POST",
                        "url": "%s/predictions/semantic_highlighter",
                        "headers": {
                            "Content-Type": "application/json"
                        },
                        "request_body": "${parameters.inputs}"
                    }
                ]
            }
            """, name, endpoint);
    }

    @Deprecated
    private static String createTorchServeBatchDisabledConnectorBody(String name, String endpoint) {
        return String.format(
            Locale.ROOT,
            """
                {
                    "name": "%s",
                    "description": "TorchServe connector for semantic highlighter with batch disabled",
                    "version": 1,
                    "protocol": "http",
                    "parameters": {},
                    "credential": {
                        "key": "test"
                    },
                    "actions": [
                        {
                            "action_type": "predict",
                            "method": "POST",
                            "url": "%s/predictions/semantic_highlighter",
                            "headers": {
                                "Content-Type": "application/json"
                            },
                            "request_body": "{ \\"question\\": \\"${parameters.question}\\", \\"context\\": \\"${parameters.context}\\" }",
                            "pre_process_function": "// Extract question and context directly from params\\nif (params.question != null && params.context != null) {\\n    return '{\\\"parameters\\\":{\\\"question\\\":\\\"' + params.question + '\\\",\\\"context\\\":\\\"' + params.context + '\\\"}}'; \\n} \\nelse {\\n    throw new IllegalArgumentException(\\\"Missing required parameters: question and context\\\");\\n}"
                        }
                    ]
                }
                """,
            name,
            endpoint
        );
    }
}
