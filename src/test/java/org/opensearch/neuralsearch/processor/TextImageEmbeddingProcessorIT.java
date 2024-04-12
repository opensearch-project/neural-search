/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import com.google.common.collect.ImmutableList;

/**
 * Testing text_and_image_embedding ingest processor. We can only test text in integ tests, none of pre-built models
 * supports both text and image.
 */
public class TextImageEmbeddingProcessorIT extends BaseNeuralSearchIT {

    private static final String INDEX_NAME = "text_image_embedding_index";
    private static final String PIPELINE_NAME = "ingest-pipeline";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    public void testEmbeddingProcessor_whenIngestingDocumentWithOrWithoutSourceMatchingMapping_thenSuccessful() throws Exception {
        String modelId = null;
        try {
            modelId = uploadModel();
            loadModel(modelId);
            createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_IMAGE_EMBEDDING);
            createTextImageEmbeddingIndex();
            // verify doc with mapping
            ingestDocumentWithTextMappedToEmbeddingField();
            assertEquals(1, getDocCount(INDEX_NAME));
            // verify doc without mapping
            ingestDocumentWithoutMappedFields();
            assertEquals(2, getDocCount(INDEX_NAME));
        } finally {
            wipeOfTestResources(INDEX_NAME, PIPELINE_NAME, modelId, null);
        }
    }

    private String uploadModel() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        return registerModelGroupAndUploadModel(requestBody);
    }

    private void createTextImageEmbeddingIndex() throws Exception {
        createIndexWithConfiguration(
            INDEX_NAME,
            Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
            PIPELINE_NAME
        );
    }

    private void ingestDocumentWithTextMappedToEmbeddingField() throws Exception {
        String ingestDocumentBody = "{\n"
            + "  \"title\": \"This is a good day\",\n"
            + "  \"description\": \"daily logging\",\n"
            + "  \"passage_text\": \"A very nice day today\",\n"
            + "  \"favorites\": {\n"
            + "    \"game\": \"overwatch\",\n"
            + "    \"movie\": null\n"
            + "  }\n"
            + "}\n";
        ingestDocument(ingestDocumentBody);
    }

    private void ingestDocumentWithoutMappedFields() throws Exception {
        String ingestDocumentBody = "{\n"
            + "  \"title\": \"This is a good day\",\n"
            + "  \"description\": \"daily logging\",\n"
            + "  \"some_random_field\": \"Today is a sunny weather\"\n"
            + "}\n";
        ingestDocument(ingestDocumentBody);
    }

    private void ingestDocument(final String ingestDocument) throws Exception {
        Response response = makeRequest(
            client(),
            "POST",
            INDEX_NAME + "/_doc?refresh",
            null,
            toHttpEntity(ingestDocument),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        Map<String, Object> map = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(response.getEntity()),
            false
        );
        assertEquals("created", map.get("result"));
    }
}
