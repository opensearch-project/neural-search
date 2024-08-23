/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.junit.Before;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

public class TextChunkingProcessorIT extends BaseNeuralSearchIT {
    private static final String INDEX_NAME = "text_chunking_test_index";

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

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    @SneakyThrows
    public void testTextChunkingProcessor_withFixedTokenLengthAlgorithmStandardTokenizer_thenSucceed() {
        try {
            createPipelineProcessor(FIXED_TOKEN_LENGTH_PIPELINE_WITH_STANDARD_TOKENIZER_NAME);
            createTextChunkingIndex(INDEX_NAME, FIXED_TOKEN_LENGTH_PIPELINE_WITH_STANDARD_TOKENIZER_NAME);
            ingestDocument(TEST_DOCUMENT);

            List<String> expectedPassages = new ArrayList<>();
            expectedPassages.add("This is an example document to be chunked. The document ");
            expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
            expectedPassages.add("standard tokenizer in OpenSearch.");
            validateIndexIngestResults(INDEX_NAME, OUTPUT_FIELD, expectedPassages);
        } finally {
            wipeOfTestResources(INDEX_NAME, FIXED_TOKEN_LENGTH_PIPELINE_WITH_STANDARD_TOKENIZER_NAME, null, null);
        }
    }

    @SneakyThrows
    public void testTextChunkingProcessor_withFixedTokenLengthAlgorithmLetterTokenizer_thenSucceed() {
        try {
            createPipelineProcessor(FIXED_TOKEN_LENGTH_PIPELINE_WITH_LETTER_TOKENIZER_NAME);
            createTextChunkingIndex(INDEX_NAME, FIXED_TOKEN_LENGTH_PIPELINE_WITH_LETTER_TOKENIZER_NAME);
            ingestDocument(TEST_DOCUMENT);

            List<String> expectedPassages = new ArrayList<>();
            expectedPassages.add("This is an example document to be chunked. The document ");
            expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by standard ");
            expectedPassages.add("tokenizer in OpenSearch.");
            validateIndexIngestResults(INDEX_NAME, OUTPUT_FIELD, expectedPassages);
        } finally {
            wipeOfTestResources(INDEX_NAME, FIXED_TOKEN_LENGTH_PIPELINE_WITH_LETTER_TOKENIZER_NAME, null, null);
        }
    }

    @SneakyThrows
    public void testTextChunkingProcessor_withFixedTokenLengthAlgorithmLowercaseTokenizer_thenSucceed() {
        try {
            createPipelineProcessor(FIXED_TOKEN_LENGTH_PIPELINE_WITH_LOWERCASE_TOKENIZER_NAME);
            createTextChunkingIndex(INDEX_NAME, FIXED_TOKEN_LENGTH_PIPELINE_WITH_LOWERCASE_TOKENIZER_NAME);
            ingestDocument(TEST_DOCUMENT);

            List<String> expectedPassages = new ArrayList<>();
            expectedPassages.add("This is an example document to be chunked. The document ");
            expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by standard ");
            expectedPassages.add("tokenizer in OpenSearch.");
            validateIndexIngestResults(INDEX_NAME, OUTPUT_FIELD, expectedPassages);
        } finally {
            wipeOfTestResources(INDEX_NAME, FIXED_TOKEN_LENGTH_PIPELINE_WITH_LOWERCASE_TOKENIZER_NAME, null, null);
        }
    }

    @SneakyThrows
    public void testTextChunkingProcessor_withFixedTokenLengthAlgorithmStandardTokenizer_whenExceedMaxTokenCount_thenFail() {
        try {
            createPipelineProcessor(FIXED_TOKEN_LENGTH_PIPELINE_WITH_STANDARD_TOKENIZER_NAME);
            createTextChunkingIndex(INDEX_NAME, FIXED_TOKEN_LENGTH_PIPELINE_WITH_STANDARD_TOKENIZER_NAME);
            Exception exception = assertThrows(Exception.class, () -> ingestDocument(TEST_LONG_DOCUMENT));
            // max_token_count is 100 by index settings
            assert (exception.getMessage()
                .contains("The number of tokens produced by calling _analyze has exceeded the allowed maximum of [100]."));
            assertEquals(0, getDocCount(INDEX_NAME));
        } finally {
            wipeOfTestResources(INDEX_NAME, FIXED_TOKEN_LENGTH_PIPELINE_WITH_STANDARD_TOKENIZER_NAME, null, null);
        }
    }

    @SneakyThrows
    public void testTextChunkingProcessor_withDelimiterAlgorithm_successful() {
        try {
            createPipelineProcessor(DELIMITER_PIPELINE_NAME);
            createTextChunkingIndex(INDEX_NAME, DELIMITER_PIPELINE_NAME);
            ingestDocument(TEST_DOCUMENT);

            List<String> expectedPassages = new ArrayList<>();
            expectedPassages.add("This is an example document to be chunked.");
            expectedPassages.add(
                " The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
            );
            validateIndexIngestResults(INDEX_NAME, OUTPUT_FIELD, expectedPassages);
        } finally {
            wipeOfTestResources(INDEX_NAME, DELIMITER_PIPELINE_NAME, null, null);
        }
    }

    @SneakyThrows
    public void testTextChunkingProcessor_withCascadePipeline_successful() {
        try {
            createPipelineProcessor(CASCADE_PIPELINE_NAME);
            createTextChunkingIndex(INDEX_NAME, CASCADE_PIPELINE_NAME);
            ingestDocument(TEST_DOCUMENT);

            List<String> expectedPassages = new ArrayList<>();
            expectedPassages.add("This is an example document to be chunked.");
            expectedPassages.add(" The document contains a single paragraph, two sentences and 24 ");
            expectedPassages.add("tokens by standard tokenizer in OpenSearch.");
            validateIndexIngestResults(INDEX_NAME, OUTPUT_FIELD, expectedPassages);

            expectedPassages.clear();
            expectedPassages.add("This is an example document to be chunked.");
            expectedPassages.add(
                " The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
            );
            validateIndexIngestResults(INDEX_NAME, INTERMEDIATE_FIELD, expectedPassages);
        } finally {
            wipeOfTestResources(INDEX_NAME, CASCADE_PIPELINE_NAME, null, null);
        }
    }

    private void validateIndexIngestResults(String indexName, String fieldName, Object expected) {
        assertEquals(1, getDocCount(indexName));
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

    private void ingestDocument(String documentPath) throws Exception {
        URL documentURLPath = classLoader.getResource(documentPath);
        Objects.requireNonNull(documentURLPath);
        String document = Files.readString(Path.of(documentURLPath.toURI()));
        Response response = makeRequest(
            client(),
            "POST",
            INDEX_NAME + "/_doc?refresh",
            null,
            toHttpEntity(document),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        Map<String, Object> map = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(response.getEntity()),
            false
        );
        assertEquals("created", map.get("result"));
    }
}
