/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.lucene.search.join.ScoreMode;
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import com.google.common.collect.ImmutableList;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

public class TextEmbeddingProcessorIT extends BaseNeuralSearchIT {

    private static final String INDEX_NAME = "text_embedding_index";

    private static final String PIPELINE_NAME = "pipeline-hybrid";
    protected static final String QUERY_TEXT = "hello";
    protected static final String LEVEL_1_FIELD = "nested_passages";
    protected static final String LEVEL_2_FIELD = "level_2";
    protected static final String LEVEL_3_FIELD_TEXT = "level_3_text";
    protected static final String LEVEL_3_FIELD_CONTAINER = "level_3_container";
    protected static final String LEVEL_3_FIELD_EMBEDDING = "level_3_embedding";
    protected static final String TEXT_FIELD_VALUE_1 = "hello";
    protected static final String TEXT_FIELD_VALUE_2 = "clown";
    protected static final String TEXT_FIELD_VALUE_3 = "abc";
    private final String INGEST_DOC1 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc1.json").toURI()));
    private final String INGEST_DOC2 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc2.json").toURI()));
    private final String INGEST_DOC3 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc3.json").toURI()));
    private final String INGEST_DOC4 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc4.json").toURI()));
    private final String INGEST_DOC5 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc5.json").toURI()));
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
            createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING, 2);
            createTextEmbeddingIndex();
            ingestBatchDocumentWithBulk("batch_", 2, Collections.emptySet(), Collections.emptySet());
            assertEquals(2, getDocCount(INDEX_NAME));

            ingestDocument(String.format(LOCALE, INGEST_DOC1, "success"), "1");
            ingestDocument(String.format(LOCALE, INGEST_DOC2, "success"), "2");

            assertEquals(getDocById(INDEX_NAME, "1").get("_source"), getDocById(INDEX_NAME, "batch_1").get("_source"));
            assertEquals(getDocById(INDEX_NAME, "2").get("_source"), getDocById(INDEX_NAME, "batch_2").get("_source"));
        } finally {
            wipeOfTestResources(INDEX_NAME, PIPELINE_NAME, modelId, null);
        }
    }

    public void testNestedFieldMapping_whenDocumentsIngested_thenSuccessful() throws Exception {
        String modelId = null;
        try {
            modelId = uploadTextEmbeddingModel();
            loadModel(modelId);
            createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING_WITH_NESTED_FIELDS_MAPPING);
            createTextEmbeddingIndex();
            ingestDocument(INGEST_DOC3, "3");
            ingestDocument(INGEST_DOC4, "4");

            assertDoc(
                (Map<String, Object>) getDocById(INDEX_NAME, "3").get("_source"),
                TEXT_FIELD_VALUE_1,
                Optional.of(TEXT_FIELD_VALUE_3)
            );
            assertDoc((Map<String, Object>) getDocById(INDEX_NAME, "4").get("_source"), TEXT_FIELD_VALUE_2, Optional.empty());

            NeuralQueryBuilder neuralQueryBuilderQuery = new NeuralQueryBuilder(
                LEVEL_1_FIELD + "." + LEVEL_2_FIELD + "." + LEVEL_3_FIELD_CONTAINER + "." + LEVEL_3_FIELD_EMBEDDING,
                QUERY_TEXT,
                "",
                modelId,
                10,
                null,
                null,
                null,
                null,
                null,
                null
            );
            QueryBuilder queryNestedLowerLevel = QueryBuilders.nestedQuery(
                LEVEL_1_FIELD + "." + LEVEL_2_FIELD,
                neuralQueryBuilderQuery,
                ScoreMode.Total
            );
            QueryBuilder queryNestedHighLevel = QueryBuilders.nestedQuery(LEVEL_1_FIELD, queryNestedLowerLevel, ScoreMode.Total);

            Map<String, Object> searchResponseAsMap = search(INDEX_NAME, queryNestedHighLevel, 2);
            assertNotNull(searchResponseAsMap);

            Map<String, Object> hits = (Map<String, Object>) searchResponseAsMap.get("hits");
            assertNotNull(hits);

            assertEquals(1.0, hits.get("max_score"));
            List<Map<String, Object>> listOfHits = (List<Map<String, Object>>) hits.get("hits");
            assertNotNull(listOfHits);
            assertEquals(2, listOfHits.size());

            Map<String, Object> innerHitDetails = listOfHits.get(0);
            assertEquals("3", innerHitDetails.get("_id"));
            assertEquals(1.0, innerHitDetails.get("_score"));

            innerHitDetails = listOfHits.get(1);
            assertEquals("4", innerHitDetails.get("_id"));
            assertTrue((double) innerHitDetails.get("_score") <= 1.0);
        } finally {
            wipeOfTestResources(INDEX_NAME, PIPELINE_NAME, modelId, null);
        }
    }

    private void assertDoc(Map<String, Object> sourceMap, String textFieldValue, Optional<String> level3ExpectedValue) {
        assertNotNull(sourceMap);
        assertTrue(sourceMap.containsKey(LEVEL_1_FIELD));
        Map<String, Object> nestedPassages = (Map<String, Object>) sourceMap.get(LEVEL_1_FIELD);
        assertTrue(nestedPassages.containsKey(LEVEL_2_FIELD));
        Map<String, Object> level2 = (Map<String, Object>) nestedPassages.get(LEVEL_2_FIELD);
        assertEquals(textFieldValue, level2.get(LEVEL_3_FIELD_TEXT));
        Map<String, Object> level3 = (Map<String, Object>) level2.get(LEVEL_3_FIELD_CONTAINER);
        List<Double> embeddings = (List<Double>) level3.get(LEVEL_3_FIELD_EMBEDDING);
        assertEquals(768, embeddings.size());
        for (Double embedding : embeddings) {
            assertTrue(embedding >= 0.0 && embedding <= 1.0);
        }
        if (level3ExpectedValue.isPresent()) {
            assertEquals(level3ExpectedValue.get(), level3.get("level_4_text_field"));
        }
    }

    public void testTextEmbeddingProcessor_withBatchSizeInProcessor() throws Exception {
        String modelId = null;
        try {
            modelId = uploadTextEmbeddingModel();
            loadModel(modelId);
            URL pipelineURLPath = classLoader.getResource("processor/PipelineConfigurationWithBatchSize.json");
            Objects.requireNonNull(pipelineURLPath);
            String requestBody = Files.readString(Path.of(pipelineURLPath.toURI()));
            createPipelineProcessor(requestBody, PIPELINE_NAME, modelId, null);
            createTextEmbeddingIndex();
            int docCount = 5;
            ingestBatchDocumentWithBulk("batch_", docCount, Collections.emptySet(), Collections.emptySet());
            assertEquals(5, getDocCount(INDEX_NAME));

            for (int i = 0; i < docCount; ++i) {
                String template = List.of(INGEST_DOC1, INGEST_DOC2).get(i % 2);
                String payload = String.format(LOCALE, template, "success");
                ingestDocument(payload, String.valueOf(i + 1));
            }

            for (int i = 0; i < docCount; ++i) {
                assertEquals(
                    getDocById(INDEX_NAME, String.valueOf(i + 1)).get("_source"),
                    getDocById(INDEX_NAME, "batch_" + (i + 1)).get("_source")
                );

            }
        } finally {
            wipeOfTestResources(INDEX_NAME, PIPELINE_NAME, modelId, null);
        }
    }

    public void testTextEmbeddingProcessor_withFailureAndSkip() throws Exception {
        String modelId = null;
        try {
            modelId = uploadTextEmbeddingModel();
            loadModel(modelId);
            URL pipelineURLPath = classLoader.getResource("processor/PipelineConfigurationWithBatchSize.json");
            Objects.requireNonNull(pipelineURLPath);
            String requestBody = Files.readString(Path.of(pipelineURLPath.toURI()));
            createPipelineProcessor(requestBody, PIPELINE_NAME, modelId, null);
            createTextEmbeddingIndex();
            int docCount = 5;
            ingestBatchDocumentWithBulk("batch_", docCount, Set.of(0), Set.of(1));
            assertEquals(3, getDocCount(INDEX_NAME));

            for (int i = 2; i < docCount; ++i) {
                String template = List.of(INGEST_DOC1, INGEST_DOC2).get(i % 2);
                String payload = String.format(LOCALE, template, "success");
                ingestDocument(payload, String.valueOf(i + 1));
            }

            for (int i = 2; i < docCount; ++i) {
                assertEquals(
                    getDocById(INDEX_NAME, String.valueOf(i + 1)).get("_source"),
                    getDocById(INDEX_NAME, "batch_" + (i + 1)).get("_source")
                );

            }
        } finally {
            wipeOfTestResources(INDEX_NAME, PIPELINE_NAME, modelId, null);
        }
    }

    private String uploadTextEmbeddingModel() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        return registerModelGroupAndUploadModel(requestBody);
    }

    private String uploadAsymmetricEmbeddingModel() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadAsymmetricModelRequestBody.json").toURI()));
        return registerModelGroupAndUploadModel(requestBody);
    }

    private void createTextEmbeddingIndex() throws Exception {
        createIndexWithConfiguration(
            INDEX_NAME,
            Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
            PIPELINE_NAME
        );
    }

    public void testAsymmetricTextEmbeddingProcessor() throws Exception {
        String modelId = null;
        try {
            modelId = uploadAsymmetricEmbeddingModel();
            loadModel(modelId);
            createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING, 2);
            createTextEmbeddingIndex();
            ingestDocument(INGEST_DOC5, null);
            assertEquals(1, getDocCount(INDEX_NAME));
        } finally {
            wipeOfTestResources(INDEX_NAME, PIPELINE_NAME, modelId, null);
        }
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

    private void ingestBatchDocumentWithBulk(String idPrefix, int docCount, Set<Integer> failedIds, Set<Integer> droppedIds)
        throws Exception {
        StringBuilder payloadBuilder = new StringBuilder();
        for (int i = 0; i < docCount; ++i) {
            String docTemplate = List.of(INGEST_DOC1, INGEST_DOC2).get(i % 2);
            if (failedIds.contains(i)) {
                docTemplate = String.format(LOCALE, docTemplate, "fail");
            } else if (droppedIds.contains(i)) {
                docTemplate = String.format(LOCALE, docTemplate, "drop");
            } else {
                docTemplate = String.format(LOCALE, docTemplate, "success");
            }
            String doc = docTemplate.replace("\n", "");
            final String id = idPrefix + (i + 1);
            String item = BULK_ITEM_TEMPLATE.replace("{{index}}", INDEX_NAME).replace("{{id}}", id).replace("{{doc}}", doc);
            payloadBuilder.append(item).append("\n");
        }
        final String payload = payloadBuilder.toString();
        Map<String, String> params = new HashMap<>();
        params.put("refresh", "true");
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
        assertEquals(!failedIds.isEmpty(), map.get("errors"));
        assertEquals(docCount, ((List) map.get("items")).size());

        int failedDocCount = 0;
        for (Object item : ((List) map.get("items"))) {
            Map<String, Map<String, Object>> itemMap = (Map<String, Map<String, Object>>) item;
            if (itemMap.get("index").get("error") != null) {
                failedDocCount++;
            }
        }
        assertEquals(failedIds.size(), failedDocCount);
    }
}
