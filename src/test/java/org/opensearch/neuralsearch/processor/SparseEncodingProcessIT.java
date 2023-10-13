/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import lombok.SneakyThrows;

import org.junit.After;
import org.opensearch.neuralsearch.common.BaseSparseEncodingIT;

public class SparseEncodingProcessIT extends BaseSparseEncodingIT {

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

    @After
    @SneakyThrows
    public void tearDown() {
        super.tearDown();
        /* this is required to minimize chance of model not being deployed due to open memory CB,
         * this happens in case we leave model from previous test case. We use new model for every test, and old model
         * can be undeployed and deleted to free resources after each test case execution.
         */
        findDeployedModels().forEach(this::deleteModel);
    }

    public void testSparseEncodingProcessor() throws Exception {
        String modelId = prepareModel();
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.SPARSE_ENCODING);
        createIndexWithPipeline(INDEX_NAME, "SparseEncodingIndexMappings.json", PIPELINE_NAME);
        String result = ingestDocument(INDEX_NAME, INGEST_DOCUMENT);
        assertEquals("created", result);
        assertEquals(1, getDocCount(INDEX_NAME));
    }

    public void testSparseEncodingProcessorWithReindex() throws Exception {
        // create a simple index and indexing data into this index.
        String fromIndexName = "test-reindex-from";
        createIndexWithConfiguration(fromIndexName, "{ \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0 } }", null);
        String result = ingestDocument(fromIndexName, "{ \"text\": \"hello world\" }");
        assertEquals("created", result);
        // create text embedding index for reindex
        String modelId = prepareModel();
        String toIndexName = "test-reindex-to";
        String pipelineName = "pipeline-text-sparse-encoding";
        createPipelineProcessor(modelId, pipelineName);
        createIndexWithPipeline(toIndexName, "SparseEncodingIndexMappings.json", pipelineName);
        reindex(fromIndexName, toIndexName);
        assertEquals(1, getDocCount(toIndexName));
    }


}
