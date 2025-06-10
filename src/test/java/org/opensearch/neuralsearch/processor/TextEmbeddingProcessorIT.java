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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.lucene.search.join.ScoreMode;
import org.junit.Before;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.info.InfoStatName;

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
    protected static final String TEXT_FIELD_VALUE_4 = "def";
    protected static final String TEXT_FIELD_VALUE_5 = "joker";

    private final String INGEST_DOC1 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc1.json").toURI()));
    private final String INGEST_DOC2 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc2.json").toURI()));
    private final String INGEST_DOC3 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc3.json").toURI()));
    private final String INGEST_DOC4 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc4.json").toURI()));
    private final String INGEST_DOC5 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc5.json").toURI()));
    private final String UPDATE_DOC1 = Files.readString(Path.of(classLoader.getResource("processor/update_doc1.json").toURI()));
    private final String UPDATE_DOC2 = Files.readString(Path.of(classLoader.getResource("processor/update_doc2.json").toURI()));
    private final String UPDATE_DOC3 = Files.readString(Path.of(classLoader.getResource("processor/update_doc3.json").toURI()));
    private final String UPDATE_DOC4 = Files.readString(Path.of(classLoader.getResource("processor/update_doc4.json").toURI()));
    private final String UPDATE_DOC5 = Files.readString(Path.of(classLoader.getResource("processor/update_doc5.json").toURI()));

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
        String modelId = uploadTextEmbeddingModel();
        loadModel(modelId);
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING);
        createIndexWithPipeline(INDEX_NAME, "IndexMappings.json", PIPELINE_NAME);
        ingestDocument(INDEX_NAME, INGEST_DOC1);
        assertEquals(1, getDocCount(INDEX_NAME));
    }

    public void testTextEmbeddingProcessorWithSkipExisting() throws Exception {
        String modelId = uploadTextEmbeddingModel();
        loadModel(modelId);
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING_WITH_SKIP_EXISTING);
        createIndexWithPipeline(INDEX_NAME, "IndexMappings.json", PIPELINE_NAME);
        ingestDocument(INDEX_NAME, INGEST_DOC1, "1");
        updateDocument(INDEX_NAME, UPDATE_DOC1, "1");
        assertEquals(1, getDocCount(INDEX_NAME));
        assertEquals(2, getDocById(INDEX_NAME, "1").get("_version"));
    }

    public void testTextEmbeddingProcessor_batch() throws Exception {
        String modelId = uploadTextEmbeddingModel();
        loadModel(modelId);
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING, 2);
        createIndexWithPipeline(INDEX_NAME, "IndexMappings.json", PIPELINE_NAME);
        ingestBatchDocumentWithBulk(
            INDEX_NAME,
            "batch_",
            2,
            Collections.emptySet(),
            Collections.emptySet(),
            List.of(INGEST_DOC1, INGEST_DOC2),
            BULK_ITEM_TEMPLATE
        );
        assertEquals(2, getDocCount(INDEX_NAME));

        ingestDocument(INDEX_NAME, String.format(LOCALE, INGEST_DOC1, "success"), "1");
        ingestDocument(INDEX_NAME, String.format(LOCALE, INGEST_DOC2, "success"), "2");

        assertEquals(getDocById(INDEX_NAME, "1").get("_source"), getDocById(INDEX_NAME, "batch_1").get("_source"));
        assertEquals(getDocById(INDEX_NAME, "2").get("_source"), getDocById(INDEX_NAME, "batch_2").get("_source"));
    }

    public void testTextEmbeddingProcessor_WithSkipExisting_batchUpdate() throws Exception {
        String modelId = uploadTextEmbeddingModel();
        loadModel(modelId);
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING_WITH_SKIP_EXISTING, 2);
        createIndexWithPipeline(INDEX_NAME, "IndexMappings.json", PIPELINE_NAME);
        ingestBatchDocumentWithBulk(
            INDEX_NAME,
            "batch_",
            2,
            Collections.emptySet(),
            Collections.emptySet(),
            List.of(INGEST_DOC1, INGEST_DOC2),
            BULK_ITEM_TEMPLATE
        );
        ingestBatchDocumentWithBulk(
            INDEX_NAME,
            "batch_",
            2,
            Collections.emptySet(),
            Collections.emptySet(),
            List.of(UPDATE_DOC1, UPDATE_DOC2),
            BULK_ITEM_TEMPLATE
        );

        assertEquals(2, getDocCount(INDEX_NAME));

        ingestDocument(INDEX_NAME, String.format(LOCALE, INGEST_DOC1, "success"), "1");
        ingestDocument(INDEX_NAME, String.format(LOCALE, INGEST_DOC2, "success"), "2");
        updateDocument(INDEX_NAME, String.format(LOCALE, UPDATE_DOC1, "success"), "1");
        updateDocument(INDEX_NAME, String.format(LOCALE, UPDATE_DOC2, "success"), "2");

        assertEquals(getDocById(INDEX_NAME, "1").get("_source"), getDocById(INDEX_NAME, "batch_1").get("_source"));
        assertEquals(getDocById(INDEX_NAME, "2").get("_source"), getDocById(INDEX_NAME, "batch_2").get("_source"));
    }

    public void testNestedFieldMapping_whenDocumentsIngested_thenSuccessful() throws Exception {
        String modelId = uploadTextEmbeddingModel();
        loadModel(modelId);
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING_WITH_NESTED_FIELDS_MAPPING);
        createIndexWithPipeline(INDEX_NAME, "IndexMappings.json", PIPELINE_NAME);
        ingestDocument(INDEX_NAME, INGEST_DOC3, "3");
        ingestDocument(INDEX_NAME, INGEST_DOC4, "4");

        assertDoc((Map<String, Object>) getDocById(INDEX_NAME, "3").get("_source"), TEXT_FIELD_VALUE_1, Optional.of(TEXT_FIELD_VALUE_3));
        assertDoc((Map<String, Object>) getDocById(INDEX_NAME, "4").get("_source"), TEXT_FIELD_VALUE_2, Optional.empty());

        NeuralQueryBuilder neuralQueryBuilderQuery = NeuralQueryBuilder.builder()
            .fieldName(LEVEL_1_FIELD + "." + LEVEL_2_FIELD + "." + LEVEL_3_FIELD_CONTAINER + "." + LEVEL_3_FIELD_EMBEDDING)
            .queryText(QUERY_TEXT)
            .modelId(modelId)
            .k(10)
            .build();

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
    }

    public void testNestedFieldMapping_whenDocumentsIngested_WithSkipExisting_thenSuccessful() throws Exception {
        String modelId = uploadTextEmbeddingModel();
        loadModel(modelId);
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING_WITH_NESTED_FIELDS_MAPPING_WITH_SKIP_EXISTING);
        createIndexWithPipeline(INDEX_NAME, "IndexMappings.json", PIPELINE_NAME);
        ingestDocument(INDEX_NAME, INGEST_DOC3, "3");
        updateDocument(INDEX_NAME, UPDATE_DOC3, "3");
        ingestDocument(INDEX_NAME, INGEST_DOC4, "4");
        updateDocument(INDEX_NAME, UPDATE_DOC4, "4");

        assertDoc((Map<String, Object>) getDocById(INDEX_NAME, "3").get("_source"), TEXT_FIELD_VALUE_1, Optional.of(TEXT_FIELD_VALUE_4));
        assertDoc((Map<String, Object>) getDocById(INDEX_NAME, "4").get("_source"), TEXT_FIELD_VALUE_5, Optional.empty());

        NeuralQueryBuilder neuralQueryBuilderQuery = NeuralQueryBuilder.builder()
            .fieldName(LEVEL_1_FIELD + "." + LEVEL_2_FIELD + "." + LEVEL_3_FIELD_CONTAINER + "." + LEVEL_3_FIELD_EMBEDDING)
            .queryText(QUERY_TEXT)
            .modelId(modelId)
            .k(10)
            .build();

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
    }

    public void testNestedFieldMapping_whenDocumentInListIngestedAndUpdated_WithSkipExisting_thenSuccessful() throws Exception {
        String modelId = uploadTextEmbeddingModel();
        loadModel(modelId);
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING_WITH_NESTED_FIELDS_MAPPING);
        createIndexWithPipeline(INDEX_NAME, "IndexMappings.json", PIPELINE_NAME);
        ingestDocument(INDEX_NAME, INGEST_DOC5, "5");
        updateDocument(INDEX_NAME, UPDATE_DOC5, "5");

        assertDocWithLevel2AsList((Map<String, Object>) getDocById(INDEX_NAME, "5").get("_source"));

        NeuralQueryBuilder neuralQueryBuilderQuery = NeuralQueryBuilder.builder()
            .fieldName(LEVEL_1_FIELD + "." + LEVEL_2_FIELD + "." + LEVEL_3_FIELD_CONTAINER + "." + LEVEL_3_FIELD_EMBEDDING)
            .queryText(QUERY_TEXT)
            .modelId(modelId)
            .k(10)
            .build();

        QueryBuilder queryNestedLowerLevel = QueryBuilders.nestedQuery(
            LEVEL_1_FIELD + "." + LEVEL_2_FIELD,
            neuralQueryBuilderQuery,
            ScoreMode.Total
        );
        QueryBuilder queryNestedHighLevel = QueryBuilders.nestedQuery(LEVEL_1_FIELD, queryNestedLowerLevel, ScoreMode.Total);

        Map<String, Object> searchResponseAsMap = search(INDEX_NAME, queryNestedHighLevel, 2);
        assertNotNull(searchResponseAsMap);

        assertEquals(1, getHitCount(searchResponseAsMap));

        Map<String, Object> innerHitDetails = getFirstInnerHit(searchResponseAsMap);
        assertEquals("5", innerHitDetails.get("_id"));
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

    private void assertDocWithLevel2AsList(Map<String, Object> sourceMap) {
        assertNotNull(sourceMap);
        assertTrue(sourceMap.containsKey(LEVEL_1_FIELD));
        assertTrue(sourceMap.get(LEVEL_1_FIELD) instanceof List);
        List<Map<String, Object>> nestedPassages = (List<Map<String, Object>>) sourceMap.get(LEVEL_1_FIELD);
        nestedPassages.forEach(nestedPassage -> {
            assertTrue(nestedPassage.containsKey(LEVEL_2_FIELD));
            Map<String, Object> level2 = (Map<String, Object>) nestedPassage.get(LEVEL_2_FIELD);
            Map<String, Object> level3 = (Map<String, Object>) level2.get(LEVEL_3_FIELD_CONTAINER);
            List<Double> embeddings = (List<Double>) level3.get(LEVEL_3_FIELD_EMBEDDING);
            assertEquals(768, embeddings.size());
            for (Double embedding : embeddings) {
                assertTrue(embedding >= 0.0 && embedding <= 1.0);
            }
        });
    }

    public void testTextEmbeddingProcessor_updateWithSkipExisting_withBatchSizeInProcessor() throws Exception {
        String modelId = uploadTextEmbeddingModel();
        loadModel(modelId);
        URL pipelineURLPath = classLoader.getResource("processor/PipelineConfigurationWithBatchSizeWithSkipExisting.json");
        Objects.requireNonNull(pipelineURLPath);
        String requestBody = Files.readString(Path.of(pipelineURLPath.toURI()));
        createPipelineProcessor(requestBody, PIPELINE_NAME, modelId, null);
        createIndexWithPipeline(INDEX_NAME, "IndexMappings.json", PIPELINE_NAME);
        int docCount = 5;
        ingestBatchDocumentWithBulk(
            INDEX_NAME,
            "batch_",
            docCount,
            Collections.emptySet(),
            Collections.emptySet(),
            List.of(INGEST_DOC1, INGEST_DOC2),
            BULK_ITEM_TEMPLATE
        );
        ingestBatchDocumentWithBulk(
            INDEX_NAME,
            "batch_",
            docCount,
            Collections.emptySet(),
            Collections.emptySet(),
            List.of(UPDATE_DOC1, UPDATE_DOC2),
            BULK_ITEM_TEMPLATE
        );

        assertEquals(5, getDocCount(INDEX_NAME));

        for (int i = 0; i < docCount; ++i) {
            String template = List.of(INGEST_DOC1, INGEST_DOC2).get(i % 2);
            String payload = String.format(LOCALE, template, "success");
            ingestDocument(INDEX_NAME, payload, String.valueOf(i + 1));
        }

        for (int i = 0; i < docCount; ++i) {
            String template = List.of(UPDATE_DOC1, UPDATE_DOC2).get(i % 2);
            String payload = String.format(LOCALE, template, "success");
            updateDocument(INDEX_NAME, payload, String.valueOf(i + 1));
        }

        for (int i = 0; i < docCount; ++i) {
            assertEquals(
                getDocById(INDEX_NAME, String.valueOf(i + 1)).get("_source"),
                getDocById(INDEX_NAME, "batch_" + (i + 1)).get("_source")
            );

        }
    }

    public void testTextEmbeddingProcessor_withBatchSizeInProcessor() throws Exception {
        String modelId = uploadTextEmbeddingModel();
        loadModel(modelId);
        URL pipelineURLPath = classLoader.getResource("processor/PipelineConfigurationWithBatchSize.json");
        Objects.requireNonNull(pipelineURLPath);
        String requestBody = Files.readString(Path.of(pipelineURLPath.toURI()));
        createPipelineProcessor(requestBody, PIPELINE_NAME, modelId, null);
        createIndexWithPipeline(INDEX_NAME, "IndexMappings.json", PIPELINE_NAME);
        int docCount = 5;
        ingestBatchDocumentWithBulk(
            INDEX_NAME,
            "batch_",
            docCount,
            Collections.emptySet(),
            Collections.emptySet(),
            List.of(INGEST_DOC1, INGEST_DOC2),
            BULK_ITEM_TEMPLATE
        );
        assertEquals(5, getDocCount(INDEX_NAME));

        for (int i = 0; i < docCount; ++i) {
            String template = List.of(INGEST_DOC1, INGEST_DOC2).get(i % 2);
            String payload = String.format(LOCALE, template, "success");
            ingestDocument(INDEX_NAME, payload, String.valueOf(i + 1));
        }

        for (int i = 0; i < docCount; ++i) {
            assertEquals(
                getDocById(INDEX_NAME, String.valueOf(i + 1)).get("_source"),
                getDocById(INDEX_NAME, "batch_" + (i + 1)).get("_source")
            );

        }
    }

    public void testTextEmbeddingProcessor_withFailureAndSkip() throws Exception {
        String modelId = uploadTextEmbeddingModel();
        loadModel(modelId);
        URL pipelineURLPath = classLoader.getResource("processor/PipelineConfigurationWithBatchSize.json");
        Objects.requireNonNull(pipelineURLPath);
        String requestBody = Files.readString(Path.of(pipelineURLPath.toURI()));
        createPipelineProcessor(requestBody, PIPELINE_NAME, modelId, null);
        createIndexWithPipeline(INDEX_NAME, "IndexMappings.json", PIPELINE_NAME);
        int docCount = 5;
        ingestBatchDocumentWithBulk(
            INDEX_NAME,
            "batch_",
            docCount,
            Set.of(0),
            Set.of(1),
            List.of(INGEST_DOC1, INGEST_DOC2),
            BULK_ITEM_TEMPLATE
        );
        assertEquals(3, getDocCount(INDEX_NAME));

        for (int i = 2; i < docCount; ++i) {
            String template = List.of(INGEST_DOC1, INGEST_DOC2).get(i % 2);
            String payload = String.format(LOCALE, template, "success");
            ingestDocument(INDEX_NAME, payload, String.valueOf(i + 1));
        }

        for (int i = 2; i < docCount; ++i) {
            assertEquals(
                getDocById(INDEX_NAME, String.valueOf(i + 1)).get("_source"),
                getDocById(INDEX_NAME, "batch_" + (i + 1)).get("_source")
            );

        }
    }

    @SuppressWarnings("unchecked")
    public void testNestedFieldMapping_whenDocumentInListIngested_thenSuccessful() throws Exception {
        String modelId = uploadTextEmbeddingModel();
        loadModel(modelId);
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING_WITH_NESTED_FIELDS_MAPPING);
        createIndexWithPipeline(INDEX_NAME, "IndexMappings.json", PIPELINE_NAME);
        ingestDocument(INDEX_NAME, INGEST_DOC5, "5");

        assertDocWithLevel2AsList((Map<String, Object>) getDocById(INDEX_NAME, "5").get("_source"));

        NeuralQueryBuilder neuralQueryBuilderQuery = NeuralQueryBuilder.builder()
            .fieldName(LEVEL_1_FIELD + "." + LEVEL_2_FIELD + "." + LEVEL_3_FIELD_CONTAINER + "." + LEVEL_3_FIELD_EMBEDDING)
            .queryText(QUERY_TEXT)
            .modelId(modelId)
            .k(10)
            .build();

        QueryBuilder queryNestedLowerLevel = QueryBuilders.nestedQuery(
            LEVEL_1_FIELD + "." + LEVEL_2_FIELD,
            neuralQueryBuilderQuery,
            ScoreMode.Total
        );
        QueryBuilder queryNestedHighLevel = QueryBuilders.nestedQuery(LEVEL_1_FIELD, queryNestedLowerLevel, ScoreMode.Total);

        Map<String, Object> searchResponseAsMap = search(INDEX_NAME, queryNestedHighLevel, 2);
        assertNotNull(searchResponseAsMap);

        assertEquals(1, getHitCount(searchResponseAsMap));

        Map<String, Object> innerHitDetails = getFirstInnerHit(searchResponseAsMap);
        assertEquals("5", innerHitDetails.get("_id"));
    }

    private String uploadTextEmbeddingModel() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        return registerModelGroupAndUploadModel(requestBody);
    }

    public void testTextEmbeddingProcessorWithReindexOperation() throws Exception {
        // create a simple index and indexing data into this index.
        String fromIndexName = "test-reindex-from";
        createIndexWithConfiguration(fromIndexName, "{ \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0 } }", null);
        ingestDocument(fromIndexName, "{ \"text\": \"hello world\" }");
        // create text embedding index for reindex
        String modelId = uploadTextEmbeddingModel();
        loadModel(modelId);
        String toIndexName = "test-reindex-to";
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING);
        createIndexWithPipeline(toIndexName, "IndexMappings.json", PIPELINE_NAME);
        reindex(fromIndexName, toIndexName);
        assertEquals(1, getDocCount(toIndexName));
    }

    public void testTextEmbeddingProcessor_processorStats_successful() throws Exception {
        updateClusterSettings("plugins.neural_search.stats_enabled", true);
        String modelId = uploadTextEmbeddingModel();
        loadModel(modelId);
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING_WITH_SKIP_EXISTING);
        createIndexWithPipeline(INDEX_NAME, "IndexMappings.json", PIPELINE_NAME);
        ingestDocument(INDEX_NAME, INGEST_DOC1, "1");
        updateDocument(INDEX_NAME, UPDATE_DOC1, "1");
        assertEquals(1, getDocCount(INDEX_NAME));
        assertEquals(2, getDocById(INDEX_NAME, "1").get("_version"));
        // Get stats
        String responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
        Map<String, Object> stats = parseInfoStatsResponse(responseBody);
        Map<String, Object> allNodesStats = parseAggregatedNodeStatsResponse(responseBody);

        // Parse json to get stats
        assertEquals(2, getNestedValue(allNodesStats, EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS));
        assertEquals(2, getNestedValue(allNodesStats, EventStatName.SKIP_EXISTING_EXECUTIONS));

        assertEquals(1, getNestedValue(stats, InfoStatName.TEXT_EMBEDDING_PROCESSORS));
        assertEquals(1, getNestedValue(stats, InfoStatName.SKIP_EXISTING_PROCESSORS));
        // Reset stats
        updateClusterSettings("plugins.neural_search.stats_enabled", false);
    }

    public void testTextEmbeddingProcessor_batch_processorStats_successful() throws Exception {
        updateClusterSettings("plugins.neural_search.stats_enabled", true);
        String modelId = uploadTextEmbeddingModel();
        loadModel(modelId);
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING_WITH_SKIP_EXISTING);
        createIndexWithPipeline(INDEX_NAME, "IndexMappings.json", PIPELINE_NAME);
        ingestBatchDocumentWithBulk(
            INDEX_NAME,
            "batch_",
            2,
            Collections.emptySet(),
            Collections.emptySet(),
            List.of(INGEST_DOC1, INGEST_DOC2),
            BULK_ITEM_TEMPLATE
        );
        assertEquals(2, getDocCount(INDEX_NAME));

        // Get stats
        String responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
        Map<String, Object> stats = parseInfoStatsResponse(responseBody);
        Map<String, Object> allNodesStats = parseAggregatedNodeStatsResponse(responseBody);

        // Parse json to get stats
        assertEquals(2, getNestedValue(allNodesStats, EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS));
        assertEquals(2, getNestedValue(allNodesStats, EventStatName.SKIP_EXISTING_EXECUTIONS));

        assertEquals(1, getNestedValue(stats, InfoStatName.TEXT_EMBEDDING_PROCESSORS));
        assertEquals(1, getNestedValue(stats, InfoStatName.SKIP_EXISTING_PROCESSORS));
        updateClusterSettings("plugins.neural_search.stats_enabled", false);
    }

}
