/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.SneakyThrows;
import org.junit.Before;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.info.InfoStatName;

public class TextChunkingProcessorIT extends BaseNeuralSearchIT {
    private static final String INDEX_NAME = "text_chunking_test_index";
    private static final String INDEX_NAME2 = "text_chunking_test_index_2nd";
    private static final String INDEX_NAME3 = "text_chunking_test_index_3rd";

    private static final String OUTPUT_FIELD = "body_chunk";

    private static final String INTERMEDIATE_FIELD = "body_chunk_intermediate";

    private static final String FIXED_TOKEN_LENGTH_PIPELINE_WITH_STANDARD_TOKENIZER_NAME =
        "pipeline-text-chunking-fixed-token-length-standard-tokenizer";

    private static final String FIXED_TOKEN_LENGTH_PIPELINE_WITH_LETTER_TOKENIZER_NAME =
        "pipeline-text-chunking-fixed-token-length-letter-tokenizer";

    private static final String FIXED_TOKEN_LENGTH_PIPELINE_WITH_LOWERCASE_TOKENIZER_NAME =
        "pipeline-text-chunking-fixed-token-length-lowercase-tokenizer";

    private static final String DELIMITER_PIPELINE_NAME = "pipeline-text-chunking-delimiter";

    private static final String FIXED_CHAR_LENGTH_PIPELINE_NAME = "pipeline-text-chunking-fixed-char-length";

    private static final String CASCADE_PIPELINE_V1_NAME = "pipeline-text-chunking-cascade-v1";

    private static final String CASCADE_PIPELINE_V2_NAME = "pipeline-text-chunking-cascade-v2";

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
        FIXED_CHAR_LENGTH_PIPELINE_NAME,
        "processor/chunker/PipelineForFixedCharLengthChunker.json",
        CASCADE_PIPELINE_V1_NAME,
        "processor/chunker/PipelineForCascadedChunkerWithDelimiterAndFixedTokenLength.json",
        CASCADE_PIPELINE_V2_NAME,
        "processor/chunker/PipelineForCascadedChunkerWithDelimiterAndFixedCharLength.json"
    );

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    @SneakyThrows
    public void testTextChunkingProcessor_withFixedTokenLengthAlgorithmStandardTokenizer_thenSucceed() {
        createPipelineProcessor(FIXED_TOKEN_LENGTH_PIPELINE_WITH_STANDARD_TOKENIZER_NAME);
        createTextChunkingIndex(INDEX_NAME, FIXED_TOKEN_LENGTH_PIPELINE_WITH_STANDARD_TOKENIZER_NAME);

        String document = getDocumentFromFilePath(TEST_DOCUMENT);
        ingestDocument(INDEX_NAME, document);

        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
        expectedPassages.add("standard tokenizer in OpenSearch.");
        validateIndexIngestResults(INDEX_NAME, OUTPUT_FIELD, expectedPassages);
    }

    @SneakyThrows
    public void testTextChunkingProcessor_withFixedTokenLengthAlgorithmLetterTokenizer_thenSucceed() {
        createPipelineProcessor(FIXED_TOKEN_LENGTH_PIPELINE_WITH_LETTER_TOKENIZER_NAME);
        createTextChunkingIndex(INDEX_NAME, FIXED_TOKEN_LENGTH_PIPELINE_WITH_LETTER_TOKENIZER_NAME);

        String document = getDocumentFromFilePath(TEST_DOCUMENT);
        ingestDocument(INDEX_NAME, document);

        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by standard ");
        expectedPassages.add("tokenizer in OpenSearch.");
        validateIndexIngestResults(INDEX_NAME, OUTPUT_FIELD, expectedPassages);
    }

    @SneakyThrows
    public void testTextChunkingProcessor_withFixedTokenLengthAlgorithmLowercaseTokenizer_thenSucceed() {
        createPipelineProcessor(FIXED_TOKEN_LENGTH_PIPELINE_WITH_LOWERCASE_TOKENIZER_NAME);
        createTextChunkingIndex(INDEX_NAME, FIXED_TOKEN_LENGTH_PIPELINE_WITH_LOWERCASE_TOKENIZER_NAME);

        String document = getDocumentFromFilePath(TEST_DOCUMENT);
        ingestDocument(INDEX_NAME, document);

        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by standard ");
        expectedPassages.add("tokenizer in OpenSearch.");
        validateIndexIngestResults(INDEX_NAME, OUTPUT_FIELD, expectedPassages);
    }

    @SneakyThrows
    public void testTextChunkingProcessor_withFixedTokenLengthAlgorithmStandardTokenizer_whenExceedMaxTokenCount_thenFail() {
        createPipelineProcessor(FIXED_TOKEN_LENGTH_PIPELINE_WITH_STANDARD_TOKENIZER_NAME);
        createTextChunkingIndex(INDEX_NAME, FIXED_TOKEN_LENGTH_PIPELINE_WITH_STANDARD_TOKENIZER_NAME);
        Exception exception = assertThrows(Exception.class, () -> {
            String document = getDocumentFromFilePath(TEST_LONG_DOCUMENT);
            ingestDocument(INDEX_NAME, document);
        });
        // max_token_count is 100 by index settings
        assert (exception.getMessage()
            .contains("The number of tokens produced by calling _analyze has exceeded the allowed maximum of [100]."));
        assertEquals(0, getDocCount(INDEX_NAME));
    }

    @SneakyThrows
    public void testTextChunkingProcessor_withDelimiterAlgorithm_successful() {
        createPipelineProcessor(DELIMITER_PIPELINE_NAME);
        createTextChunkingIndex(INDEX_NAME, DELIMITER_PIPELINE_NAME);

        String document = getDocumentFromFilePath(TEST_DOCUMENT);
        ingestDocument(INDEX_NAME, document);

        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked.");
        expectedPassages.add(" The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.");
        validateIndexIngestResults(INDEX_NAME, OUTPUT_FIELD, expectedPassages);
    }

    @SneakyThrows
    public void testTextChunkingProcessor_withFixedCharLengthAlgorithm_thenSucceed() {
        createPipelineProcessor(FIXED_CHAR_LENGTH_PIPELINE_NAME);
        createTextChunkingIndex(INDEX_NAME, FIXED_CHAR_LENGTH_PIPELINE_NAME);

        String document = getDocumentFromFilePath(TEST_DOCUMENT);
        ingestDocument(INDEX_NAME, document);

        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The doc");
        expectedPassages.add("d. The document contains a single paragraph, two s");
        expectedPassages.add("aph, two sentences and 24 tokens by standard token");
        expectedPassages.add("dard tokenizer in OpenSearch.");
        validateIndexIngestResults(INDEX_NAME, OUTPUT_FIELD, expectedPassages);
    }

    @SneakyThrows
    public void testTextChunkingProcessor_withCascadePipelineV1ApplyingDelimiterAndFixedTokenLength_successful() {
        createPipelineProcessor(CASCADE_PIPELINE_V1_NAME);
        createTextChunkingIndex(INDEX_NAME, CASCADE_PIPELINE_V1_NAME);

        String document = getDocumentFromFilePath(TEST_DOCUMENT);
        ingestDocument(INDEX_NAME, document);

        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked.");
        expectedPassages.add(" The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.");
        validateIndexIngestResults(INDEX_NAME, INTERMEDIATE_FIELD, expectedPassages);

        expectedPassages.clear();
        expectedPassages.add("This is an example document to be chunked.");
        expectedPassages.add(" The document contains a single paragraph, two sentences and 24 ");
        expectedPassages.add("tokens by standard tokenizer in OpenSearch.");
        validateIndexIngestResults(INDEX_NAME, OUTPUT_FIELD, expectedPassages);
    }

    @SneakyThrows
    public void testTextChunkingProcessor_withCascadePipelineV2ApplyingDelimiterAndFixedCharLength_successful() {
        createPipelineProcessor(CASCADE_PIPELINE_V2_NAME);
        createTextChunkingIndex(INDEX_NAME, CASCADE_PIPELINE_V2_NAME);

        String document = getDocumentFromFilePath(TEST_DOCUMENT);
        ingestDocument(INDEX_NAME, document);

        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked.");
        expectedPassages.add(" The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.");
        validateIndexIngestResults(INDEX_NAME, INTERMEDIATE_FIELD, expectedPassages);

        expectedPassages.clear();
        expectedPassages.add("This is an example document to");
        expectedPassages.add(" be chunked.");
        expectedPassages.add(" The document contains a singl");
        expectedPassages.add("e paragraph, two sentences and");
        expectedPassages.add(" 24 tokens by standard tokeniz");
        expectedPassages.add("er in OpenSearch.");
        validateIndexIngestResults(INDEX_NAME, OUTPUT_FIELD, expectedPassages);
    }

    public void testTextChunkingProcessor_withFixedTokenLengthAlgorithmStandardTokenizer_whenReindexingDocument_thenSuccessful()
        throws Exception {
        String fromIndexName = "test-reindex-from";
        createIndexWithConfiguration(fromIndexName, "{ \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0 } }", null);
        String document = getDocumentFromFilePath(TEST_DOCUMENT);
        ingestDocument(fromIndexName, document);

        createPipelineProcessor(FIXED_TOKEN_LENGTH_PIPELINE_WITH_STANDARD_TOKENIZER_NAME);
        createTextChunkingIndex(INDEX_NAME, FIXED_TOKEN_LENGTH_PIPELINE_WITH_STANDARD_TOKENIZER_NAME);
        reindex(fromIndexName, INDEX_NAME);
        assertEquals(1, getDocCount(INDEX_NAME));
    }

    @SneakyThrows
    public void testTextChunkingProcessor_processorStats_successful() {
        enableStats();
        createPipelineProcessor(FIXED_TOKEN_LENGTH_PIPELINE_WITH_STANDARD_TOKENIZER_NAME);
        createTextChunkingIndex(INDEX_NAME, FIXED_TOKEN_LENGTH_PIPELINE_WITH_STANDARD_TOKENIZER_NAME);

        // Creating an extra fixed token length pipeline
        createPipelineProcessor(FIXED_TOKEN_LENGTH_PIPELINE_WITH_LOWERCASE_TOKENIZER_NAME);

        createPipelineProcessor(DELIMITER_PIPELINE_NAME);
        createTextChunkingIndex(INDEX_NAME2, DELIMITER_PIPELINE_NAME);

        createPipelineProcessor(FIXED_CHAR_LENGTH_PIPELINE_NAME);
        createTextChunkingIndex(INDEX_NAME3, FIXED_CHAR_LENGTH_PIPELINE_NAME);

        String document = getDocumentFromFilePath(TEST_DOCUMENT);
        ingestDocument(INDEX_NAME, document);
        ingestDocument(INDEX_NAME, document);

        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
        expectedPassages.add("standard tokenizer in OpenSearch.");
        validateIndexIngestResultsWithMultipleDocs(INDEX_NAME, OUTPUT_FIELD, expectedPassages, 2);

        ingestDocument(INDEX_NAME2, document);
        ingestDocument(INDEX_NAME2, document);
        ingestDocument(INDEX_NAME2, document);

        expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked.");
        expectedPassages.add(" The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.");
        validateIndexIngestResultsWithMultipleDocs(INDEX_NAME2, OUTPUT_FIELD, expectedPassages, 3);

        ingestDocument(INDEX_NAME3, document);
        ingestDocument(INDEX_NAME3, document);
        ingestDocument(INDEX_NAME3, document);
        ingestDocument(INDEX_NAME3, document);

        expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The doc");
        expectedPassages.add("d. The document contains a single paragraph, two s");
        expectedPassages.add("aph, two sentences and 24 tokens by standard token");
        expectedPassages.add("dard tokenizer in OpenSearch.");
        validateIndexIngestResultsWithMultipleDocs(INDEX_NAME3, OUTPUT_FIELD, expectedPassages, 4);

        // Get stats
        String responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
        Map<String, Object> stats = parseInfoStatsResponse(responseBody);
        Map<String, Object> allNodesStats = parseAggregatedNodeStatsResponse(responseBody);

        // Parse json to get stats
        assertEquals(9, getNestedValue(allNodesStats, EventStatName.TEXT_CHUNKING_PROCESSOR_EXECUTIONS));
        assertEquals(3, getNestedValue(allNodesStats, EventStatName.TEXT_CHUNKING_DELIMITER_EXECUTIONS));
        assertEquals(2, getNestedValue(allNodesStats, EventStatName.TEXT_CHUNKING_FIXED_TOKEN_LENGTH_EXECUTIONS));
        assertEquals(4, getNestedValue(allNodesStats, EventStatName.TEXT_CHUNKING_FIXED_CHAR_LENGTH_EXECUTIONS));

        assertEquals(4, getNestedValue(stats, InfoStatName.TEXT_CHUNKING_PROCESSORS));
        assertEquals(1, getNestedValue(stats, InfoStatName.TEXT_CHUNKING_DELIMITER_PROCESSORS));
        assertEquals(2, getNestedValue(stats, InfoStatName.TEXT_CHUNKING_FIXED_TOKEN_LENGTH_PROCESSORS));
        assertEquals(1, getNestedValue(stats, InfoStatName.TEXT_CHUNKING_FIXED_CHAR_LENGTH_PROCESSORS));

        // Reset stats
        disableStats();
    }

    private void validateIndexIngestResultsWithMultipleDocs(String indexName, String fieldName, Object expected, int docCount) {
        assertEquals(docCount, getDocCount(indexName));
        MatchAllQueryBuilder query = new MatchAllQueryBuilder();
        Map<String, Object> searchResults = search(indexName, query, 10);
        assertNotNull(searchResults);
        Map<String, Object> document = getFirstInnerHit(searchResults);
        assertNotNull(document);
        Object documentSource = document.get("_source");
        assert (documentSource instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> documentSourceMap = (Map<String, Object>) documentSource;
        assert (documentSourceMap).containsKey(fieldName);
        Object ingestOutputs = documentSourceMap.get(fieldName);
        assertEquals(expected, ingestOutputs);
    }

    private void validateIndexIngestResults(String indexName, String fieldName, Object expected) {
        validateIndexIngestResultsWithMultipleDocs(indexName, fieldName, expected, 1);
    }

    private void createPipelineProcessor(String pipelineName) throws Exception {
        URL pipelineURLPath = classLoader.getResource(PIPELINE_CONFIGS_BY_NAME.get(pipelineName));
        Objects.requireNonNull(pipelineURLPath);
        String requestBody = Files.readString(Path.of(pipelineURLPath.toURI()));
        createPipelineProcessor(requestBody, pipelineName, "", null);
    }

    private void createTextChunkingIndex(String indexName, String pipelineName) throws Exception {
        URL indexSettingsURLPath = classLoader.getResource("processor/chunker/TextChunkingIndexSettings.json");
        Objects.requireNonNull(indexSettingsURLPath);
        createIndexWithConfiguration(indexName, Files.readString(Path.of(indexSettingsURLPath.toURI())), pipelineName);
    }

    private String getDocumentFromFilePath(String filePath) throws Exception {
        URL documentURLPath = classLoader.getResource(filePath);
        Objects.requireNonNull(documentURLPath);
        return Files.readString(Path.of(documentURLPath.toURI()));
    }
}
