/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.ArrayList;
import java.util.Map;

import org.junit.Before;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.info.InfoStatName;

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
    }

    public void testSparseEncodingProcessorWithPrune() throws Exception {
        String modelId = prepareSparseEncodingModel();
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
    }

    public void testSparseEncodingProcessorWithReindex() throws Exception {
        // create a simple index and indexing data into this index.
        String fromIndexName = "test-reindex-from";
        createIndexWithConfiguration(fromIndexName, "{ \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0 } }", null);
        ingestDocument(fromIndexName, "{ \"text\": \"hello world\" }");
        // create text embedding index for reindex
        String modelId = prepareSparseEncodingModel();
        String toIndexName = "test-reindex-to";
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.SPARSE_ENCODING);
        createIndexWithPipeline(toIndexName, "SparseEncodingIndexMappings.json", PIPELINE_NAME);
        reindex(fromIndexName, toIndexName);
        assertEquals(1, getDocCount(toIndexName));
    }

    public void testSparseEncodingProcessorWithSkipExistingUpdateWithNoChange() throws Exception {
        String modelId = null;
        modelId = prepareSparseEncodingModel();
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.SPARSE_ENCODING_WITH_SKIP_EXISTING);
        createIndexWithPipeline(INDEX_NAME, "SparseEncodingIndexMappings.json", PIPELINE_NAME);
        ingestDocument(INDEX_NAME, INGEST_DOCUMENT, "1");
        updateDocument(INDEX_NAME, INGEST_DOCUMENT, "1");
        assertEquals(1, getDocCount(INDEX_NAME));
        assertEquals(2, getDocById(INDEX_NAME, "1").get("_version"));

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder();
        neuralSparseQueryBuilder.fieldName("title_sparse");
        neuralSparseQueryBuilder.queryTokensSupplier(() -> Map.of("good", 1.0f, "a", 2.0f));
        Map<String, Object> searchResponse = search(INDEX_NAME, neuralSparseQueryBuilder, 2);
        assertFalse(searchResponse.isEmpty());
        double maxScore = (Double) ((Map) searchResponse.get("hits")).get("max_score");
        assertEquals(4.4433594, maxScore, 1e-3);
    }

    public void testSparseEncodingProcessorWithSkipExistingUpdateWithChange() throws Exception {
        String modelId = null;
        modelId = prepareSparseEncodingModel();
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.SPARSE_ENCODING_WITH_SKIP_EXISTING);
        createIndexWithPipeline(INDEX_NAME, "SparseEncodingIndexMappings.json", PIPELINE_NAME);
        ingestDocument(INDEX_NAME, INGEST_DOCUMENT.replace("\"This is a good day\"", "\"This is a bad day\""), "1");
        updateDocument(INDEX_NAME, INGEST_DOCUMENT, "1");
        assertEquals(1, getDocCount(INDEX_NAME));
        assertEquals(2, getDocById(INDEX_NAME, "1").get("_version"));

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder();
        neuralSparseQueryBuilder.fieldName("title_sparse");
        neuralSparseQueryBuilder.queryTokensSupplier(() -> Map.of("good", 1.0f, "a", 2.0f));
        Map<String, Object> searchResponse = search(INDEX_NAME, neuralSparseQueryBuilder, 2);
        assertFalse(searchResponse.isEmpty());
        double maxScore = (Double) ((Map) searchResponse.get("hits")).get("max_score");
        assertEquals(4.4433594, maxScore, 1e-3);
    }

    public void testSparseEncodingProcessor_withSkipExisting_statsEnabled() throws Exception {
        enableStats();

        String modelId = null;
        modelId = prepareSparseEncodingModel();
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.SPARSE_ENCODING_WITH_SKIP_EXISTING);
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

        // Get stats
        String responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
        Map<String, Object> stats = parseInfoStatsResponse(responseBody);
        Map<String, Object> allNodesStats = parseAggregatedNodeStatsResponse(responseBody);

        assertEquals(1, getNestedValue(allNodesStats, EventStatName.SPARSE_ENCODING_PROCESSOR_EXECUTIONS));
        assertEquals(1, getNestedValue(allNodesStats, EventStatName.SKIP_EXISTING_EXECUTIONS));
        assertEquals(1, getNestedValue(stats, InfoStatName.SPARSE_ENCODING_PROCESSORS));
        assertEquals(1, getNestedValue(stats, InfoStatName.SKIP_EXISTING_PROCESSORS));

        disableStats();
    }
}
