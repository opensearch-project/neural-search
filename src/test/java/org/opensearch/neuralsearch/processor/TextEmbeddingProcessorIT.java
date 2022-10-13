/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

public class TextEmbeddingProcessorIT extends OpenSearchRestTestCase {

    private static final String indexName = "text_embedding_index";

    private static final String pipelineName = "pipeline-hybrid";

    private final ClassLoader classLoader = this.getClass().getClassLoader();

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final Locale locale = Locale.getDefault();

    public void test_text_embedding_processor() throws Exception {
        String modelId = uploadModel();
        loadModel(modelId);
        createPipelineProcessor(modelId);
        createIndex();
        ingestDocument();
    }

    private String uploadModel() throws Exception {
        Response uploadResponse = TestHelper.makeRequest(
            client(),
            "POST",
            "/_plugins/_ml/models/_upload",
            null,
            TestHelper.toHttpEntity(Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()))),
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

    private void loadModel(String modelId) throws IOException {
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

    private void createPipelineProcessor(String modelId) throws Exception {
        Response pipelineCreateResponse = TestHelper.makeRequest(
            client(),
            "PUT",
            "/_ingest/pipeline/" + pipelineName,
            null,
            TestHelper.toHttpEntity(
                String.format(
                    locale,
                    Files.readString(Path.of(classLoader.getResource("processor/PipelineConfiguration.json").toURI())),
                    modelId
                )
            ),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        JsonNode node = objectMapper.readTree(EntityUtils.toString(pipelineCreateResponse.getEntity()));
        assertTrue(node.get("acknowledged").asBoolean());
    }

    private void createIndex() throws Exception {
        Response response = TestHelper.makeRequest(
            client(),
            "PUT",
            indexName,
            null,
            TestHelper.toHttpEntity(
                String.format(
                    locale,
                    Files.readString(Path.of(classLoader.getResource("processor/IndexConfiguration.json").toURI())),
                    pipelineName
                )
            ),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        JsonNode node = objectMapper.readTree(EntityUtils.toString(response.getEntity()));
        assertTrue(node.get("acknowledged").asBoolean());
        assertEquals(indexName, node.get("index").asText());
    }

    private void ingestDocument() throws Exception {
        Response response = TestHelper.makeRequest(
            client(),
            "POST",
            indexName + "/_doc",
            null,
            TestHelper.toHttpEntity(Files.readString(Path.of(classLoader.getResource("processor/IngestDocument.json").toURI()))),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        JsonNode node = objectMapper.readTree(EntityUtils.toString(response.getEntity()));
        assertEquals("created", node.get("result").asText());
    }

    private JsonNode getTaskQueryResponse(String taskId) throws IOException {
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

    private boolean checkComplete(JsonNode node) {
        Predicate<JsonNode> predicate = x -> node.get("error") != null || "COMPLETED".equals(node.get("state").asText());
        return predicate.test(node);
    }

}
