/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Before;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

public class TextEmbeddingProcessorIT extends BaseNeuralSearchIT {

    private static final String INDEX_NAME = "text_embedding_index";

    private static final String PIPELINE_NAME = "pipeline-hybrid";

    private static final String TEXT_EMBEDDING_DOCUMENT = "{\n"
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
            createIndexWithPipeline(INDEX_NAME, "IndexMappings.json", PIPELINE_NAME);
            String result = ingestDocument(INDEX_NAME, TEXT_EMBEDDING_DOCUMENT);
            assertEquals("created", result);
            assertEquals(1, getDocCount(INDEX_NAME));
        } finally {
            wipeOfTestResources(INDEX_NAME, PIPELINE_NAME, modelId, null);
        }
    }

    public void testTextEmbeddingProcessorWithReindexOperation() throws Exception {
        // create a simple index and indexing data into this index.
        String fromIndexName = "test-reindex-from";
        createIndexWithConfiguration(fromIndexName, "{ \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0 } }", null);
        String result = ingestDocument(fromIndexName, "{ \"text\": \"hello world\" }");
        assertEquals("created", result);
        // create text embedding index for reindex
        String modelId = null;
        try {
            modelId = uploadTextEmbeddingModel();
            loadModel(modelId);
            String toIndexName = "test-reindex-to";
            createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING);
            createIndexWithPipeline(toIndexName, "IndexMappings.json", PIPELINE_NAME);
            reindex(fromIndexName, toIndexName);
            assertEquals(1, getDocCount(toIndexName));
        } finally {
            wipeOfTestResources(fromIndexName, PIPELINE_NAME, modelId, null);
        }
    }

    private String uploadTextEmbeddingModel() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        return registerModelGroupAndUploadModel(requestBody);
    }

}
