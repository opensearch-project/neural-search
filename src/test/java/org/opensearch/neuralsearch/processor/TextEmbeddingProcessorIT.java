/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;

import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.neuralsearch.common.BaseNeuralSearchIT;

import com.google.common.collect.ImmutableList;

public class TextEmbeddingProcessorIT extends BaseNeuralSearchIT {

    private static final String INDEX_NAME = "text_embedding_index";

    private static final String PIPELINE_NAME = "pipeline-hybrid";

    public void testTextEmbeddingProcessor() throws Exception {
        String modelId = uploadTextEmbeddingModel();
        loadModel(modelId);
        createPipelineProcessor(modelId, PIPELINE_NAME);
        createTextEmbeddingIndex();
        ingestDocument();
    }

    private String uploadTextEmbeddingModel() throws Exception {
        String currentPath = System.getProperty("user.dir");
        Path testClusterPath = Path.of(currentPath).getParent().resolveSibling("testclusters/integTest-0/data");
        Path path = Path.of(testClusterPath + "/all-MiniLM-L6-v2.zip");
        Files.copy(Path.of(classLoader.getResource("model/all-MiniLM-L6-v2.zip").toURI()), path);
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        String request = String.format(Locale.ROOT, requestBody, path);
        return uploadModel(request);
    }

    private void createTextEmbeddingIndex() throws Exception {
        createIndexWithConfiguration(
            INDEX_NAME,
            Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
            PIPELINE_NAME
        );
    }

    private void ingestDocument() throws Exception {
        String ingestDocument = "{\n"
            + "  \"title\": \"This is a good day\",\n"
            + "  \"description\": \"daily logging\",\n"
            + "  \"favor_list\": [\n"
            + "    \"test\",\n"
            + "    \"hello\",\n"
            + "    \"mock\"\n"
            + "  ],\n"
            + "  \"favorites\": {\n"
            + "    \"game\": \"overwatch\",\n"
            + "    \"movie\": null\n"
            + "  }\n"
            + "}\n";
        Response response = makeRequest(
            client(),
            "POST",
            INDEX_NAME + "/_doc",
            null,
            toHttpEntity(ingestDocument),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        Map<String, Object> map = XContentHelper.convertToMap(
            XContentFactory.xContent(XContentType.JSON),
            EntityUtils.toString(response.getEntity()),
            false
        );
        assertEquals("created", map.get("result"));
    }

}
