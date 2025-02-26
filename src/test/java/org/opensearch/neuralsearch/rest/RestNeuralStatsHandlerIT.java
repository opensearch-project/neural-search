/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import lombok.extern.log4j.Log4j2;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.client.Request;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.plugin.NeuralSearch;
import org.opensearch.neuralsearch.stats.state.StateStatName;
import org.opensearch.neuralsearch.stats.events.EventStatName;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Log4j2
public class RestNeuralStatsHandlerIT extends BaseNeuralSearchIT {
    private static final String INDEX_NAME = "stats_index";
    private static final String INDEX_NAME_2 = "stats_index_2";
    private static final String INDEX_NAME_3 = "stats_index_3";

    private static final String INGEST_PIPELINE_NAME = "ingest-pipeline-1";
    private static final String INGEST_PIPELINE_NAME_2 = "ingest-pipeline-2";
    private static final String INGEST_PIPELINE_NAME_3 = "ingest-pipeline-3";
    private static final String SEARCH_PIPELINE_NAME = "search-pipeline-1";
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

    private static final String NORMALIZATION_SEARCH_PIPELINE = "normalization-search-pipeline";

    private static final String OUTPUT_FIELD = "body_chunk";

    private static final String INTERMEDIATE_FIELD = "body_chunk_intermediate";

    private static final String FIXED_TOKEN_LENGTH_PIPELINE_WITH_STANDARD_TOKENIZER_NAME =
        "pipeline-text-chunking-fixed-token-length-standard-tokenizer";

    private static final String FIXED_TOKEN_LENGTH_PIPELINE_WITH_LETTER_TOKENIZER_NAME =
        "pipeline-text-chunking-fixed-token-length-letter-tokenizer";

    private static final String FIXED_TOKEN_LENGTH_PIPELINE_WITH_LOWERCASE_TOKENIZER_NAME =
        "pipeline-text-chunking-fixed-token-length-lowercase-tokenizer";

    private static final String DELIMITER_PIPELINE_NAME = "pipeline-text-chunking-delimiter";

    private static final String CASCADE_PIPELINE_NAME = "pipeline-text-chunking-cascade";

    private static final String TEST_DOCUMENT = "processor/chunker/TextChunkingTestDocument.json";

    private static final String TEST_LONG_DOCUMENT = "processor/chunker/TextChunkingTestLongDocument.json";

    private static final Map<String, String> PIPELINE_CONFIGS_BY_NAME = Map.of(
        FIXED_TOKEN_LENGTH_PIPELINE_WITH_STANDARD_TOKENIZER_NAME,
        "processor/chunker/PipelineForFixedTokenLengthChunkerWithStandardTokenizer.json",
        FIXED_TOKEN_LENGTH_PIPELINE_WITH_LETTER_TOKENIZER_NAME,
        "processor/chunker/PipelineForFixedTokenLengthChunkerWithLetterTokenizer.json",
        FIXED_TOKEN_LENGTH_PIPELINE_WITH_LOWERCASE_TOKENIZER_NAME,
        "processor/chunker/PipelineForFixedTokenLengthChunkerWithLowercaseTokenizer.json",
        DELIMITER_PIPELINE_NAME,
        "processor/chunker/PipelineForDelimiterChunker.json",
        CASCADE_PIPELINE_NAME,
        "processor/chunker/PipelineForCascadedChunker.json"
    );

    public RestNeuralStatsHandlerIT() throws IOException, URISyntaxException {}

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    public void test_textEmbedding() throws Exception {
        String modelId = uploadTextEmbeddingModel();
        loadModel(modelId);
        createPipelineProcessor(modelId, INGEST_PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING);
        createIndexWithPipeline(INDEX_NAME, "IndexMappings.json", INGEST_PIPELINE_NAME);

        ingestDocument(INDEX_NAME, INGEST_DOC1);
        ingestDocument(INDEX_NAME, INGEST_DOC2);
        ingestDocument(INDEX_NAME, INGEST_DOC3);
        assertEquals(3, getDocCount(INDEX_NAME));

        Response response = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> stats = parseStatsResponse(responseBody);
        List<Map<String, Object>> nodesStats = parseNodeStatsResponse(responseBody);

        log.info(nodesStats);
        assertEquals(3, getNestedValue(nodesStats.getFirst(), EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS));
        assertEquals(1, getNestedValue(stats, StateStatName.TEXT_EMBEDDING_PROCESSORS));

    }

    public void test_statsFiltering() throws Exception {
        Response response = executeNeuralStatRequest(new ArrayList<>(), Arrays.asList(StateStatName.TEXT_EMBEDDING_PROCESSORS.getName()));

        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> stats = parseStatsResponse(responseBody);
        List<Map<String, Object>> nodesStats = parseNodeStatsResponse(responseBody);

        //
        assertNull(getNestedValue(nodesStats.getFirst(), EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS.getFullPath()));
        assertEquals(0, getNestedValue(stats, StateStatName.TEXT_EMBEDDING_PROCESSORS.getFullPath()));

    }

    protected String uploadTextEmbeddingModel() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        return registerModelGroupAndUploadModel(requestBody);
    }

    protected Response executeNeuralStatRequest(List<String> nodeIds, List<String> stats) throws IOException {
        String nodePrefix = "";
        if (!nodeIds.isEmpty()) {
            nodePrefix = "/" + String.join(",", nodeIds);
        }

        String statsSuffix = "";
        if (!stats.isEmpty()) {
            statsSuffix = "/" + String.join(",", stats);
        }

        Request request = new Request("GET", NeuralSearch.NEURAL_BASE_URI + nodePrefix + "/stats" + statsSuffix);

        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return response;
    }

    protected Map<String, Object> parseStatsResponse(String responseBody) throws IOException {
        Map<String, Object> responseMap = createParser(MediaTypeRegistry.getDefaultMediaType().xContent(), responseBody).map();
        return responseMap;
    }

    protected List<Map<String, Object>> parseNodeStatsResponse(String responseBody) throws IOException {
        @SuppressWarnings("unchecked")
        Map<String, Object> responseMap = (Map<String, Object>) createParser(
            MediaTypeRegistry.getDefaultMediaType().xContent(),
            responseBody
        ).map().get("nodes");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodeResponses = responseMap.keySet()
            .stream()
            .map(key -> (Map<String, Object>) responseMap.get(key))
            .collect(Collectors.toList());

        return nodeResponses;
    }

    public Object getNestedValue(Map<String, Object> map, String path) {
        String[] keys = path.split("\\.");
        return getNestedValueHelper(map, keys, 0);
    }

    public Object getNestedValue(Map<String, Object> map, EventStatName eventStatName) {
        return getNestedValue(map, eventStatName.getFullPath());
    }

    public Object getNestedValue(Map<String, Object> map, StateStatName stateStatName) {
        return getNestedValue(map, stateStatName.getFullPath());
    }

    private Object getNestedValueHelper(Map<String, Object> map, String[] keys, int depth) {
        log.info(map);
        if (map == null) {
            return null;
        }

        Object value = map.get(keys[depth]);

        if (depth == keys.length - 1) {
            return value;
        }

        if (value instanceof Map) {
            Map<String, Object> nestedMap = (Map<String, Object>) value;
            return getNestedValueHelper(nestedMap, keys, depth + 1);
        }

        return null;
    }
}
