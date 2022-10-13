/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.opensearch.client.Response;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.neuralsearch.common.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.utils.TestHelper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

public class TextEmbeddingProcessorIT extends BaseNeuralSearchIT {

    private static final String indexName = "text_embedding_index";

    private static final String pipelineName = "pipeline-hybrid";

    private final ClassLoader classLoader = this.getClass().getClassLoader();

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final Locale locale = Locale.getDefault();

    public void test_text_embedding_processor() throws Exception {
        String modelId = uploadTextEmbeddingModel();
        loadModel(modelId);
        createPipelineProcessor(modelId);
        createTextEmbeddingIndex();
        ingestDocument();
    }

    private String uploadTextEmbeddingModel() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        return uploadModel(requestBody);
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

    private void createTextEmbeddingIndex() throws Exception {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            String settings = Strings.toString(
                builder.startObject()
                    .startObject("index")
                    .field("knn", true)
                    .field("knn.algo_param.ef_search", 100)
                    .field("refresh_interval", "30s")
                    .field("default_pipeline", pipelineName)
                    .endObject()
                    .field("number_of_shards", 1)
                    .field("number_of_replicas", 0)
                    .endObject()
                    .endObject()
            );
            createIndex(
                indexName,
                Settings.builder().loadFromSource(settings, XContentType.JSON).build(),
                Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI()))
            );
        }
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

}
