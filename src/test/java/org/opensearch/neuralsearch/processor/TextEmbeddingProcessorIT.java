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
import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.common.BaseNeuralSearchIT;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

public class TextEmbeddingProcessorIT extends BaseNeuralSearchIT {

    private static final String indexName = "text_embedding_index";

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

    private void createTextEmbeddingIndex() throws Exception {
        Settings settings = Settings.builder()
            .put("index.default_pipeline", "pipeline-hybrid")
            .put("index.knn", true)
            .put("index" + ".knn" + ".algo_param.ef_search", "100")
            .put("index.refresh_interval", "30s")
            .put("number_of_replicas", 0)
            .put("number_of_shards", 1)
            .build();
        String mapping = Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI()));
        mapping = mapping.substring(1, mapping.length() - 1);
        createIndex(indexName, settings, mapping);
    }

    private void ingestDocument() throws Exception {
        Response response = makeRequest(
            client(),
            "POST",
            indexName + "/_doc",
            null,
            toHttpEntity(Files.readString(Path.of(classLoader.getResource("processor/IngestDocument.json").toURI()))),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        JsonNode node = objectMapper.readTree(EntityUtils.toString(response.getEntity()));
        assertEquals("created", node.get("result").asText());
    }

}
