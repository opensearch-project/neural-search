/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import com.google.common.collect.ImmutableList;

public class TextEmbeddingProcessorIT extends BaseNeuralSearchIT {

    private static final String INDEX_NAME = "text_embedding_index";

    private static final String PIPELINE_NAME = "pipeline-hybrid";
    private final String INGEST_DOC1 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc1.json").toURI()));
    private final String INGEST_DOC2 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc2.json").toURI()));
    private final String BULK_ITEM_TEMPLATE = Files.readString(
        Path.of(classLoader.getResource("processor/bulk_item_template.json").toURI())
    );

    public TextEmbeddingProcessorIT() throws IOException, URISyntaxException {}

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    public void testTextEmbeddingProcessor() throws Exception {
        String modelId = null;
        try {
            modelId = uploadTextEmbeddingModel();
            loadModel(modelId);
            createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING);
            createTextEmbeddingIndex();
            ingestDocument(INGEST_DOC1, null);
            assertEquals(1, getDocCount(INDEX_NAME));
        } finally {
            wipeOfTestResources(INDEX_NAME, PIPELINE_NAME, modelId, null);
        }
    }

    public void testTextEmbeddingProcessor_batch() throws Exception {
        String modelId = null;
        try {
            modelId = uploadTextEmbeddingModel();
            loadModel(modelId);
            createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING);
            createTextEmbeddingIndex();
            ingestBatchDocumentWithBulk("batch_");
            assertEquals(2, getDocCount(INDEX_NAME));

            ingestDocument(INGEST_DOC1, "1");
            ingestDocument(INGEST_DOC2, "2");

            assertEquals(getDocById(INDEX_NAME, "1").get("_source"), getDocById(INDEX_NAME, "batch_1").get("_source"));
            assertEquals(getDocById(INDEX_NAME, "2").get("_source"), getDocById(INDEX_NAME, "batch_2").get("_source"));
        } finally {
            wipeOfTestResources(INDEX_NAME, PIPELINE_NAME, modelId, null);
        }
    }

    private String uploadTextEmbeddingModel() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        return registerModelGroupAndUploadModel(requestBody);
    }

    private void createTextEmbeddingIndex() throws Exception {
        createIndexWithConfiguration(
            INDEX_NAME,
            Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
            PIPELINE_NAME
        );
    }

    private void ingestDocument(String doc, String id) throws Exception {
        String endpoint;
        if (StringUtils.isEmpty(id)) {
            endpoint = INDEX_NAME + "/_doc?refresh";
        } else {
            endpoint = INDEX_NAME + "/_doc/" + id + "?refresh";
        }
        Response response = makeRequest(
            client(),
            "POST",
            endpoint,
            null,
            toHttpEntity(doc),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        Map<String, Object> map = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(response.getEntity()),
            false
        );
        assertEquals("created", map.get("result"));
    }

    private void ingestBatchDocumentWithBulk(String idPrefix) throws Exception {
        String doc1 = INGEST_DOC1.replace("\n", "");
        String doc2 = INGEST_DOC2.replace("\n", "");
        final String id1 = idPrefix + "1";
        final String id2 = idPrefix + "2";
        String item1 = BULK_ITEM_TEMPLATE.replace("{{index}}", INDEX_NAME)
            .replace("{{id}}", id1)
            .replace("{{doc}}", doc1)
            .replace("{{comma}}", ",");
        String item2 = BULK_ITEM_TEMPLATE.replace("{{index}}", INDEX_NAME)
            .replace("{{id}}", id2)
            .replace("{{doc}}", doc2)
            .replace("{{comma}}", "\n");
        final String payload = item1 + item2;
        Map<String, String> params = new HashMap<>();
        params.put("refresh", "true");
        params.put("batch_size", "2");
        Response response = makeRequest(
            client(),
            "POST",
            "_bulk",
            params,
            toHttpEntity(payload),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        Map<String, Object> map = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(response.getEntity()),
            false
        );
        assertEquals(false, map.get("errors"));
        assertEquals(2, ((List) map.get("items")).size());
    }
}
