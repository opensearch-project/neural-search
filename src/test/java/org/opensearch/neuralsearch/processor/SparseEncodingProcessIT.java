/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.Map;

import org.junit.Before;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;

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
            ingestDocument(INDEX_NAME, INGEST_DOCUMENT);
            assertEquals(1, getDocCount(INDEX_NAME));

            NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder();
            neuralSparseQueryBuilder.fieldName("title_sparse");
            neuralSparseQueryBuilder.queryTokensSupplier(() -> Map.of("good", 1.0f, "a", 2.0f));
            Map<String, Object> searchResponse = search(INDEX_NAME, neuralSparseQueryBuilder, 2);
            assertFalse(searchResponse.isEmpty());
            double maxScore = (Double) ((Map) searchResponse.get("hits")).get("max_score");
            assertEquals(4.4433594, maxScore, 1e-3);
        } finally {
            wipeOfTestResources(INDEX_NAME, PIPELINE_NAME, modelId, null);
        }
    }

    public void testSparseEncodingProcessorWithPrune() throws Exception {
        String modelId = null;
        try {
            modelId = prepareSparseEncodingModel();
            createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.SPARSE_ENCODING_PRUNE);
            createIndexWithPipeline(INDEX_NAME, "SparseEncodingIndexMappings.json", PIPELINE_NAME);
            ingestDocument(INDEX_NAME, INGEST_DOCUMENT);
            assertEquals(1, getDocCount(INDEX_NAME));

            NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder();
            neuralSparseQueryBuilder.fieldName("title_sparse");
            neuralSparseQueryBuilder.queryTokensSupplier(() -> Map.of("good", 1.0f, "a", 2.0f));
            Map<String, Object> searchResponse = search(INDEX_NAME, neuralSparseQueryBuilder, 2);
            assertFalse(searchResponse.isEmpty());
            double maxScore = (Double) ((Map) searchResponse.get("hits")).get("max_score");
            assertEquals(3.640625, maxScore, 1e-3);
        } finally {
            wipeOfTestResources(INDEX_NAME, PIPELINE_NAME, modelId, null);
        }
    }

    public void testSparseEncodingProcessorWithReindex() throws Exception {
        // create a simple index and indexing data into this index.
        String fromIndexName = "test-reindex-from";
        createIndexWithConfiguration(fromIndexName, "{ \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0 } }", null);
        ingestDocument(fromIndexName, "{ \"text\": \"hello world\" }");
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
