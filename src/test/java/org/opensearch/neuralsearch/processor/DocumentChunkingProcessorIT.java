/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import com.google.common.collect.ImmutableList;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.TestUtils.DEFAULT_USER_AGENT;

public class DocumentChunkingProcessorIT extends BaseNeuralSearchIT {
    private static final String INDEX_NAME = "document_chunking_index";

    private static final String OUTPUT_FIELD = "body_chunk";

    private static final String INTERMEDIATE_FIELD = "body_chunk_intermediate";

    private static final String FIXED_TOKEN_LENGTH_PIPELINE_NAME = "pipeline-document-chunking-fixed-token-length";

    private static final String DELIMITER_PIPELINE_NAME = "pipeline-document-chunking-delimiter";

    private static final String CASCADE_PIPELINE_NAME = "pipeline-document-chunking-cascade";

    private static final String TEST_DOCUMENT = "processor/chunker/DocumentChunkingTestDocument.json";

    private static final String TEST_LONG_DOCUMENT = "processor/chunker/DocumentChunkingTestLongDocument.json";

    private static final Map<String, String> PIPELINE_CONFIGS_BY_NAME = Map.of(
        FIXED_TOKEN_LENGTH_PIPELINE_NAME,
        "processor/chunker/PipelineForFixedTokenLengthChunker.json",
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

    public void testDocumentChunkingProcessor_withFixedTokenLength_successful() throws Exception {
        try {
            createPipelineProcessor(FIXED_TOKEN_LENGTH_PIPELINE_NAME);
            createDocumentChunkingIndex(FIXED_TOKEN_LENGTH_PIPELINE_NAME);
            ingestDocument(TEST_DOCUMENT);

            List<String> expectedPassages = new ArrayList<>();
            expectedPassages.add("This is an example document to be chunked The document");
            expectedPassages.add("The document contains a single paragraph two sentences and 24");
            expectedPassages.add("and 24 tokens by standard tokenizer in OpenSearch");
            validateIndexIngestResults(INDEX_NAME, OUTPUT_FIELD, expectedPassages);
        } finally {
            wipeOfTestResources(INDEX_NAME, FIXED_TOKEN_LENGTH_PIPELINE_NAME, null, null);
        }
    }

    public void testDocumentChunkingProcessor_withFixedTokenLength_fail() throws Exception {
        try {
            createPipelineProcessor(FIXED_TOKEN_LENGTH_PIPELINE_NAME);
            createDocumentChunkingIndex(FIXED_TOKEN_LENGTH_PIPELINE_NAME);
            Exception exception = assertThrows(Exception.class, () -> ingestDocument(TEST_LONG_DOCUMENT));
            // max_token_count is 100 by index settings
            assert (exception.getMessage()
                .contains("The number of tokens produced by calling _analyze has exceeded the allowed maximum of [100]."));
            assertEquals(0, getDocCount(INDEX_NAME));
        } finally {
            wipeOfTestResources(INDEX_NAME, FIXED_TOKEN_LENGTH_PIPELINE_NAME, null, null);
        }
    }

    public void testDocumentChunkingProcessor_withDelimiter_successful() throws Exception {
        try {
            createPipelineProcessor(DELIMITER_PIPELINE_NAME);
            createDocumentChunkingIndex(DELIMITER_PIPELINE_NAME);
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

    public void testDocumentChunkingProcessor_withCascade_successful() throws Exception {
        try {
            createPipelineProcessor(CASCADE_PIPELINE_NAME);
            createDocumentChunkingIndex(CASCADE_PIPELINE_NAME);
            ingestDocument(TEST_DOCUMENT);

            List<String> expectedPassages = new ArrayList<>();
            // " ", "." and "," will not be included in fixed token length output
            expectedPassages.add("This is an example document to be chunked");
            expectedPassages.add("The document contains a single paragraph two sentences and 24");
            expectedPassages.add("and 24 tokens by standard tokenizer in OpenSearch");
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

    private void createPipelineProcessor(final String pipelineName) throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource(PIPELINE_CONFIGS_BY_NAME.get(pipelineName)).toURI()));
        Response pipelineCreateResponse = makeRequest(
            client(),
            "PUT",
            "/_ingest/pipeline/" + pipelineName,
            null,
            toHttpEntity(String.format(LOCALE, requestBody)),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> node = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(pipelineCreateResponse.getEntity()),
            false
        );
        assertEquals("true", node.get("acknowledged").toString());
    }

    private void createDocumentChunkingIndex(String pipelineName) throws Exception {
        createIndexWithConfiguration(
            INDEX_NAME,
            Files.readString(Path.of(classLoader.getResource("processor/chunker/DocumentChunkingIndexSettings.json").toURI())),
            pipelineName
        );
    }

    private void ingestDocument(String documentPath) throws Exception {
        String ingestDocument = Files.readString(Path.of(classLoader.getResource(documentPath).toURI()));
        Response response = makeRequest(
            client(),
            "POST",
            INDEX_NAME + "/_doc?refresh",
            null,
            toHttpEntity(ingestDocument),
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
