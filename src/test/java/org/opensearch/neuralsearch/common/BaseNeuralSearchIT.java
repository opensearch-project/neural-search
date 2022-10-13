/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.common;

import java.io.IOException;
import java.util.Locale;
import java.util.function.Predicate;

import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.opensearch.client.Response;
import org.opensearch.neuralsearch.utils.TestHelper;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

public abstract class BaseNeuralSearchIT extends OpenSearchRestTestCase {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final Locale locale = Locale.getDefault();

    public String uploadModel(String requestBody) throws Exception {
        Response uploadResponse = TestHelper.makeRequest(
            client(),
            "POST",
            "/_plugins/_ml/models/_upload",
            null,
            TestHelper.toHttpEntity(requestBody),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        JsonNode uploadResJson = objectMapper.readTree(EntityUtils.toString(uploadResponse.getEntity()));
        String taskId = uploadResJson.get("task_id").asText();
        assertNotNull(taskId);

        JsonNode taskQueryResult = getTaskQueryResponse(taskId);
        boolean isComplete = checkComplete(taskQueryResult);
        while (!isComplete) {
            taskQueryResult = getTaskQueryResponse(taskId);
            isComplete = checkComplete(taskQueryResult);
        }
        String modelId = taskQueryResult.get("model_id").asText();
        assertNotNull(modelId);
        return modelId;
    }

    public void loadModel(String modelId) throws IOException {
        Response uploadResponse = TestHelper.makeRequest(
            client(),
            "POST",
            String.format(locale, "/_plugins/_ml/models/%s/_load", modelId),
            null,
            TestHelper.toHttpEntity(""),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        JsonNode uploadResJson = objectMapper.readTree(EntityUtils.toString(uploadResponse.getEntity()));
        String taskId = uploadResJson.get("task_id").asText();
        assertNotNull(taskId);

        JsonNode taskQueryResult = getTaskQueryResponse(taskId);
        boolean isComplete = checkComplete(taskQueryResult);
        while (!isComplete) {
            taskQueryResult = getTaskQueryResponse(taskId);
            isComplete = checkComplete(taskQueryResult);
        }
    }

    public JsonNode getTaskQueryResponse(String taskId) throws IOException {
        Response taskQueryResponse = TestHelper.makeRequest(
            client(),
            "GET",
            String.format(locale, "_plugins/_ml/tasks/%s", taskId),
            null,
            TestHelper.toHttpEntity(""),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        return objectMapper.readTree(EntityUtils.toString(taskQueryResponse.getEntity()));
    }

    public boolean checkComplete(JsonNode node) {
        Predicate<JsonNode> predicate = x -> node.get("error") != null || "COMPLETED".equals(node.get("state").asText());
        return predicate.test(node);
    }

}
