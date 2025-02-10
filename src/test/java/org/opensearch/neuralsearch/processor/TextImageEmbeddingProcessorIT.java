/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Before;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

/**
 * Testing text_and_image_embedding ingest processor. We can only test text in integ tests, none of pre-built models
 * supports both text and image.
 */
public class TextImageEmbeddingProcessorIT extends BaseNeuralSearchIT {

    private static final String INDEX_NAME = "text_image_embedding_index";
    private static final String PIPELINE_NAME = "ingest-pipeline";
    private static final String INGEST_DOCUMENT = "{\n"
        + "  \"title\": \"This is a good day\",\n"
        + "  \"description\": \"daily logging\",\n"
        + "  \"passage_text\": \"A very nice day today\",\n"
        + "  \"favorites\": {\n"
        + "    \"game\": \"overwatch\",\n"
        + "    \"movie\": null\n"
        + "  }\n"
        + "}\n";

    private static final String INGEST_DOCUMENT_UNMAPPED_FIELDS = "{\n"
        + "  \"title\": \"This is a good day\",\n"
        + "  \"description\": \"daily logging\",\n"
        + "  \"some_random_field\": \"Today is a sunny weather\"\n"
        + "}\n";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    public void testEmbeddingProcessor_whenIngestingDocumentWithOrWithoutSourceMatchingMapping_thenSuccessful() throws Exception {
        String modelId = uploadModel();
        loadModel(modelId);
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_IMAGE_EMBEDDING);
        createIndexWithPipeline(INDEX_NAME, "IndexMappings.json", PIPELINE_NAME);
        // verify doc with mapping
        ingestDocument(INDEX_NAME, INGEST_DOCUMENT);
        assertEquals(1, getDocCount(INDEX_NAME));
        // verify doc without mapping
        ingestDocument(INDEX_NAME, INGEST_DOCUMENT_UNMAPPED_FIELDS);
        assertEquals(2, getDocCount(INDEX_NAME));
    }

    private String uploadModel() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        return registerModelGroupAndUploadModel(requestBody);
    }

    public void testEmbeddingProcessor_whenReindexingDocument_thenSuccessful() throws Exception {
        // create a simple index and indexing data into this index.
        String fromIndexName = "test-reindex-from";
        createIndexWithConfiguration(fromIndexName, "{ \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0 } }", null);
        ingestDocument(fromIndexName, "{ \"text\": \"hello world\" }");
        String modelId = uploadModel();
        loadModel(modelId);
        String toIndexName = "test-reindex-to";
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_IMAGE_EMBEDDING);
        createIndexWithPipeline(toIndexName, "IndexMappings.json", PIPELINE_NAME);
        reindex(fromIndexName, toIndexName);
        assertEquals(1, getDocCount(toIndexName));
    }
}
