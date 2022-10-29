/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.common;

import static org.apache.http.entity.ContentType.APPLICATION_JSON;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.WarningsHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import com.google.common.collect.ImmutableList;

public abstract class BaseNeuralSearchIT extends OpenSearchRestTestCase {

    private static final Locale LOCALE = Locale.ROOT;

    private static final int MAX_TASK_RESULT_QUERY_TIME_IN_SECOND = 60 * 5;

    private static final int DEFAULT_TASK_RESULT_QUERY_INTERVAL_IN_MILLISECOND = 1000;

    protected final ClassLoader classLoader = this.getClass().getClassLoader();

    public String uploadModel(String requestBody) throws Exception {
        Response uploadResponse = makeRequest(
            client(),
            "POST",
            "/_plugins/_ml/models/_upload",
            null,
            toHttpEntity(requestBody),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        Map<String, Object> uploadResJson = XContentHelper.convertToMap(
            XContentFactory.xContent(XContentType.JSON),
            EntityUtils.toString(uploadResponse.getEntity()),
            false
        );
        String taskId = uploadResJson.get("task_id").toString();
        assertNotNull(taskId);

        Map<String, Object> taskQueryResult = getTaskQueryResponse(taskId);
        boolean isComplete = checkComplete(taskQueryResult);
        for (int i = 0; !isComplete && i < MAX_TASK_RESULT_QUERY_TIME_IN_SECOND; i++) {
            taskQueryResult = getTaskQueryResponse(taskId);
            isComplete = checkComplete(taskQueryResult);
            Thread.sleep(DEFAULT_TASK_RESULT_QUERY_INTERVAL_IN_MILLISECOND);
        }
        String modelId = Optional.ofNullable(taskQueryResult.get("model_id")).map(Object::toString).orElse(null);
        assertNotNull(modelId);
        return modelId;
    }

    public void loadModel(String modelId) throws IOException, InterruptedException {
        Response uploadResponse = makeRequest(
            client(),
            "POST",
            String.format(LOCALE, "/_plugins/_ml/models/%s/_load", modelId),
            null,
            toHttpEntity(""),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        Map<String, Object> uploadResJson = XContentHelper.convertToMap(
            XContentFactory.xContent(XContentType.JSON),
            EntityUtils.toString(uploadResponse.getEntity()),
            false
        );
        String taskId = uploadResJson.get("task_id").toString();
        assertNotNull(taskId);

        Map<String, Object> taskQueryResult = getTaskQueryResponse(taskId);
        boolean isComplete = checkComplete(taskQueryResult);
        for (int i = 0; !isComplete && i < MAX_TASK_RESULT_QUERY_TIME_IN_SECOND; i++) {
            taskQueryResult = getTaskQueryResponse(taskId);
            isComplete = checkComplete(taskQueryResult);
            Thread.sleep(DEFAULT_TASK_RESULT_QUERY_INTERVAL_IN_MILLISECOND);
        }
    }

    protected void createIndexWithConfiguration(String indexName, String indexConfiguration, String pipelineName) throws Exception {
        if (StringUtils.isNotBlank(pipelineName)) {
            indexConfiguration = String.format(LOCALE, indexConfiguration, pipelineName);
        }
        Response response = makeRequest(
            client(),
            "PUT",
            indexName,
            null,
            toHttpEntity(indexConfiguration),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        Map<String, Object> node = XContentHelper.convertToMap(
            XContentFactory.xContent(XContentType.JSON),
            EntityUtils.toString(response.getEntity()),
            false
        );
        assertEquals("true", node.get("acknowledged").toString());
        assertEquals(indexName, node.get("index").toString());
    }

    public void createPipelineProcessor(String modelId, String pipelineName) throws Exception {
        Response pipelineCreateResponse = makeRequest(
            client(),
            "PUT",
            "/_ingest/pipeline/" + pipelineName,
            null,
            toHttpEntity(
                String.format(
                    LOCALE,
                    Files.readString(Path.of(classLoader.getResource("processor/PipelineConfiguration.json").toURI())),
                    modelId
                )
            ),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        Map<String, Object> node = XContentHelper.convertToMap(
            XContentFactory.xContent(XContentType.JSON),
            EntityUtils.toString(pipelineCreateResponse.getEntity()),
            false
        );
        assertEquals("true", node.get("acknowledged").toString());
    }

    public Map<String, Object> getTaskQueryResponse(String taskId) throws IOException {
        Response taskQueryResponse = makeRequest(
            client(),
            "GET",
            String.format(LOCALE, "_plugins/_ml/tasks/%s", taskId),
            null,
            toHttpEntity(""),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        return XContentHelper.convertToMap(
            XContentFactory.xContent(XContentType.JSON),
            EntityUtils.toString(taskQueryResponse.getEntity()),
            false
        );
    }

    public boolean checkComplete(Map<String, Object> node) {
        Predicate<Map<String, Object>> predicate = x -> node.get("error") != null || "COMPLETED".equals(String.valueOf(node.get("state")));
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
            params.forEach(request::addParameter);
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
