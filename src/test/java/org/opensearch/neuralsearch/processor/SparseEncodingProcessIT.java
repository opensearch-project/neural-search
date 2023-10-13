/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.junit.Before;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

public class SparseEncodingProcessIT extends BaseNeuralSearchIT {

    private static final String INDEX_NAME = "sparse_encoding_index";

    private static final String PIPELINE_NAME = "pipeline-sparse-encoding";

    private static final String INGEST_DOCUMENT = "{\n"
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

    public void testSparseEncodingProcessor() throws Exception {
        String modelId = null;
        try {
            modelId = prepareSparseEncodingModel();
            createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.SPARSE_ENCODING);
            createIndexWithPipeline(INDEX_NAME, "SparseEncodingIndexMappings.json", PIPELINE_NAME);
            String result = ingestDocument(INDEX_NAME, INGEST_DOCUMENT);
            assertEquals("created", result);
            assertEquals(1, getDocCount(INDEX_NAME));
        } finally {
            wipeOfTestResources(INDEX_NAME, PIPELINE_NAME, modelId, null);
        }
    }

    public void testSparseEncodingProcessorWithReindex() throws Exception {
        // create a simple index and indexing data into this index.
        String fromIndexName = "test-reindex-from";
        createIndexWithConfiguration(fromIndexName, "{ \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0 } }", null);
        String result = ingestDocument(fromIndexName, "{ \"text\": \"hello world\" }");
        assertEquals("created", result);
        // create text embedding index for reindex
        String modelId = null;
        try {
            modelId = prepareSparseEncodingModel();
            String toIndexName = "test-reindex-to";
            createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.SPARSE_ENCODING);
            createIndexWithPipeline(toIndexName, "SparseEncodingIndexMappings.json", PIPELINE_NAME);
            reindex(fromIndexName, toIndexName);
            assertEquals(1, getDocCount(toIndexName));
        } finally {
            wipeOfTestResources(fromIndexName, PIPELINE_NAME, modelId, null);
        }
    }

}
