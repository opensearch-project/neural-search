/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.common;

import static org.apache.http.entity.ContentType.APPLICATION_JSON;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.opensearch.client.*;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

public abstract class BaseNeuralSearchIT extends OpenSearchRestTestCase {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final Locale locale = Locale.getDefault();

    public String uploadModel(String requestBody) throws Exception {
        Response uploadResponse = makeRequest(
            client(),
            "POST",
            "/_plugins/_ml/models/_upload",
            null,
            toHttpEntity(requestBody),
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
        Response uploadResponse = makeRequest(
            client(),
            "POST",
            String.format(locale, "/_plugins/_ml/models/%s/_load", modelId),
            null,
            toHttpEntity(""),
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
        Response taskQueryResponse = makeRequest(
            client(),
            "GET",
            String.format(locale, "_plugins/_ml/tasks/%s", taskId),
            null,
            toHttpEntity(""),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        return objectMapper.readTree(EntityUtils.toString(taskQueryResponse.getEntity()));
    }

    public boolean checkComplete(JsonNode node) {
        Predicate<JsonNode> predicate = x -> node.get("error") != null || "COMPLETED".equals(node.get("state").asText());
        return predicate.test(node);
    }

    public static Response makeRequest(
        RestClient client,
        String method,
        String endpoint,
        Map<String, String> params,
        HttpEntity entity,
        List<Header> headers
    ) throws IOException {
        return makeRequest(client, method, endpoint, params, entity, headers, false);
    }

    public static Response makeRequest(
        RestClient client,
        String method,
        String endpoint,
        Map<String, String> params,
        HttpEntity entity,
        List<Header> headers,
        boolean strictDeprecationMode
    ) throws IOException {
        Request request = new Request(method, endpoint);

        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        if (headers != null) {
            headers.forEach(header -> options.addHeader(header.getName(), header.getValue()));
        }
        options.setWarningsHandler(strictDeprecationMode ? WarningsHandler.STRICT : WarningsHandler.PERMISSIVE);
        request.setOptions(options.build());

        if (params != null) {
            params.entrySet().forEach(it -> request.addParameter(it.getKey(), it.getValue()));
        }
        if (entity != null) {
            request.setEntity(entity);
        }
        return client.performRequest(request);
    }

    public static HttpEntity toHttpEntity(String jsonString) {
        return new StringEntity(jsonString, APPLICATION_JSON);
    }

}
