/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import lombok.extern.log4j.Log4j2;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.stats.common.StatSnapshot;
import org.opensearch.neuralsearch.stats.state.StateStatName;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.state.StateStatType;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j2
public class RestNeuralStatsHandlerIT extends BaseNeuralSearchIT {
    private static final String INDEX_NAME = "stats_index";
    private static final String INDEX_NAME_2 = "stats_index_2";
    private static final String INDEX_NAME_3 = "stats_index_3";

    private static final String INGEST_PIPELINE_NAME = "ingest-pipeline-1";
    private static final String INGEST_PIPELINE_NAME_2 = "ingest-pipeline-2";
    private static final String INGEST_PIPELINE_NAME_3 = "ingest-pipeline-3";
    private static final String SEARCH_PIPELINE_NAME = "search-pipeline-1";
    private final String INGEST_DOC1 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc1.json").toURI()));
    private final String INGEST_DOC2 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc2.json").toURI()));
    private final String INGEST_DOC3 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc3.json").toURI()));
    private final String INGEST_DOC4 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc4.json").toURI()));
    private final String INGEST_DOC5 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc5.json").toURI()));

    public RestNeuralStatsHandlerIT() throws IOException, URISyntaxException {}

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();

        // Only enable stats for this IT to prevent collisions
        updateClusterSettings("plugins.neural_search.stats_enabled", true);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        updateClusterSettings("plugins.neural_search.stats_enabled", false);
    }

    public void test_textEmbedding() throws Exception {
        // Setup processors
        String modelId = uploadTextEmbeddingModel();
        loadModel(modelId);
        createPipelineProcessor(modelId, INGEST_PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING);
        createIndexWithPipeline(INDEX_NAME, "IndexMappings.json", INGEST_PIPELINE_NAME);

        // Ingest documents
        ingestDocument(INDEX_NAME, INGEST_DOC1);
        ingestDocument(INDEX_NAME, INGEST_DOC2);
        ingestDocument(INDEX_NAME, INGEST_DOC3);
        assertEquals(3, getDocCount(INDEX_NAME));

        // Get stats request
        Response response;
        String responseBody;
        Map<String, Object> stats;
        List<Map<String, Object>> nodesStats;

        response = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
        responseBody = EntityUtils.toString(response.getEntity());
        stats = parseStatsResponse(responseBody);
        nodesStats = parseNodeStatsResponse(responseBody);

        // Parse json to get stats
        assertEquals(3, getNestedValue(nodesStats.getFirst(), EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS));
        assertEquals(1, getNestedValue(stats, StateStatName.TEXT_EMBEDDING_PROCESSORS));

        // Reset stats
        updateClusterSettings("plugins.neural_search.stats_enabled", false);
        updateClusterSettings("plugins.neural_search.stats_enabled", true);

        // State stats should persist, event stats should be reset
        response = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
        responseBody = EntityUtils.toString(response.getEntity());
        stats = parseStatsResponse(responseBody);
        nodesStats = parseNodeStatsResponse(responseBody);
        assertEquals(0, getNestedValue(nodesStats.getFirst(), EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS));
        assertEquals(1, getNestedValue(stats, StateStatName.TEXT_EMBEDDING_PROCESSORS));
    }

    public void test_statsFiltering() throws Exception {
        Response response = executeNeuralStatRequest(new ArrayList<>(), Arrays.asList(StateStatName.TEXT_EMBEDDING_PROCESSORS.getName()));

        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> stats = parseStatsResponse(responseBody);
        List<Map<String, Object>> nodesStats = parseNodeStatsResponse(responseBody);

        //
        assertNull(getNestedValue(nodesStats.getFirst(), EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS.getFullPath()));
        assertNotNull(getNestedValue(stats, StateStatName.TEXT_EMBEDDING_PROCESSORS.getFullPath()));
    }

    public void test_flatten() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put(RestNeuralStatsHandler.FLATTEN_PARAM, "true");

        Response response = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>(), params);

        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> stats = parseStatsResponse(responseBody);
        List<Map<String, Object>> nodesStats = parseNodeStatsResponse(responseBody);

        assertNotNull(nodesStats.getFirst().get(EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS.getFullPath()));
        assertNotNull(stats.get(StateStatName.TEXT_EMBEDDING_PROCESSORS.getFullPath()));
    }

    public void test_includeMetadata() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put(RestNeuralStatsHandler.INCLUDE_METADATA_PARAM, "true");

        Response response = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>(), params);
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> stats = parseStatsResponse(responseBody);

        Object clusterVersionStatMetadata = getNestedValue(stats, StateStatName.CLUSTER_VERSION.getFullPath());

        // Path value should be JSON object (convertible to map)
        // If metadata wasn't included, path value would be the raw value
        assertTrue(getNestedValue(stats, StateStatName.CLUSTER_VERSION.getFullPath()) instanceof Map);

        String statType = ((Map<String, String>) clusterVersionStatMetadata).get(StatSnapshot.STAT_TYPE_FIELD);
        String valueWithMetadata = ((Map<String, String>) clusterVersionStatMetadata).get(StatSnapshot.VALUE_FIELD);

        // Stat type metadata should match
        assertEquals(StateStatType.SETTABLE.getName(), statType);

        // Fetch Without metadata
        params.put(RestNeuralStatsHandler.INCLUDE_METADATA_PARAM, "false");

        response = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>(), params);
        responseBody = EntityUtils.toString(response.getEntity());
        stats = parseStatsResponse(responseBody);

        // Path value should be the settable value
        String valueWithoutMetadata = (String) getNestedValue(stats, StateStatName.CLUSTER_VERSION.getFullPath());

        // Value with and without metadta should be the same
        assertEquals(valueWithMetadata, valueWithoutMetadata);
    }

    protected String uploadTextEmbeddingModel() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        return registerModelGroupAndUploadModel(requestBody);
    }
}
