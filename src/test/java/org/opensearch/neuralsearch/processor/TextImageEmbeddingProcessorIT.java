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

    private static final String INDEX_NAME_1 = "text_image_embedding_index-1";
    private static final String INDEX_NAME_2 = "text_image_embedding_index-2";
    private static final String FROM_INDEX_NAME = "test-reindex-from";
    private static final String PIPELINE_NAME_1 = "text_image_embedding_ingest_pipeline-1";
    private static final String PIPELINE_NAME_2 = "text_image_embedding_ingest_pipeline-2";
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
        String modelId = null;
        try {
            modelId = uploadModel();
            loadModel(modelId);
            createPipelineProcessor(modelId, PIPELINE_NAME_1, ProcessorType.TEXT_IMAGE_EMBEDDING);
            createIndexWithPipeline(INDEX_NAME_1, "IndexMappings.json", PIPELINE_NAME_1);
            // verify doc with mapping
            ingestDocument(INDEX_NAME_1, INGEST_DOCUMENT);
            assertEquals(1, getDocCount(INDEX_NAME_1));
            // verify doc without mapping
            ingestDocument(INDEX_NAME_1, INGEST_DOCUMENT_UNMAPPED_FIELDS);
            assertEquals(2, getDocCount(INDEX_NAME_1));
        } finally {
            wipeOfTestResources(INDEX_NAME_1, PIPELINE_NAME_1, modelId, null);
        }
    }

    private String uploadModel() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        return registerModelGroupAndUploadModel(requestBody);
    }

    public void testEmbeddingProcessor_whenReindexingDocument_thenSuccessful() throws Exception {
        // create a simple index and indexing data into this index.
        createIndexWithConfiguration(FROM_INDEX_NAME, "{ \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0 } }", null);
        ingestDocument(FROM_INDEX_NAME, "{ \"text\": \"hello world\" }");
        String modelId = null;
        try {
            modelId = uploadModel();
            loadModel(modelId);
            createPipelineProcessor(modelId, PIPELINE_NAME_2, ProcessorType.TEXT_IMAGE_EMBEDDING);
            createIndexWithPipeline(INDEX_NAME_2, "IndexMappings.json", PIPELINE_NAME_2);
            reindex(FROM_INDEX_NAME, INDEX_NAME_2);
            assertEquals(1, getDocCount(INDEX_NAME_2));
        } finally {
            wipeOfTestResources(INDEX_NAME_2, PIPELINE_NAME_2, modelId, null);
        }
    }
}
