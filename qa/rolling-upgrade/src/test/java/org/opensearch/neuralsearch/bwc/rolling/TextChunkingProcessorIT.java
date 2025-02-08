/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.opensearch.index.query.MatchAllQueryBuilder;
import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;

public class TextChunkingProcessorIT extends AbstractRollingUpgradeTestCase {

    private static final String PIPELINE_NAME = "pipeline-text-chunking";
    private static final String INPUT_FIELD = "body";
    private static final String OUTPUT_FIELD = "body_chunk";
    private static final String TEST_INDEX_SETTING_PATH = "processor/ChunkingIndexSettings.json";
    private static final int NUM_DOCS_PER_ROUND = 1;
    private static final String TEST_INGEST_TEXT =
        "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.";

    List<String> expectedPassages = List.of(
        "This is an example document to be chunked. The document ",
        "contains a single paragraph, two sentences and 24 tokens by ",
        "standard tokenizer in OpenSearch."
    );

    // Test rolling-upgrade text chunking processor
    // Create Text Chunking Processor, Ingestion Pipeline and add document
    // Validate process, pipeline and document count in rolling-upgrade scenario
    public void testTextChunkingProcessor_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        super.ingestPipelineName = PIPELINE_NAME;

        switch (getClusterType()) {
            case OLD:
                createPipelineForTextChunkingProcessor(PIPELINE_NAME);
                createChunkingIndex(indexName);
                addDocument(indexName, "0", INPUT_FIELD, TEST_INGEST_TEXT, null, null);
                break;
            case MIXED:
                int totalDocsCountMixed;
                if (isFirstMixedRound()) {
                    totalDocsCountMixed = NUM_DOCS_PER_ROUND;
                    validateTestIndex(indexName, OUTPUT_FIELD, totalDocsCountMixed, expectedPassages);
                    addDocument(indexName, "1", INPUT_FIELD, TEST_INGEST_TEXT, null, null);
                } else {
                    totalDocsCountMixed = 2 * NUM_DOCS_PER_ROUND;
                    validateTestIndex(indexName, OUTPUT_FIELD, totalDocsCountMixed, expectedPassages);
                }
                break;
            case UPGRADED:
                int totalDocsCountUpgraded = 3 * NUM_DOCS_PER_ROUND;
                addDocument(indexName, "2", INPUT_FIELD, TEST_INGEST_TEXT, null, null);
                validateTestIndex(indexName, OUTPUT_FIELD, totalDocsCountUpgraded, expectedPassages);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }
    }

    private void createChunkingIndex(String indexName) throws Exception {
        URL documentURLPath = classLoader.getResource(TEST_INDEX_SETTING_PATH);
        Objects.requireNonNull(documentURLPath);
        String indexSetting = Files.readString(Path.of(documentURLPath.toURI()));
        createIndexWithConfiguration(indexName, indexSetting, PIPELINE_NAME);
    }

    private void validateTestIndex(String indexName, String fieldName, int documentCount, Object expected) {
        int docCount = getDocCount(indexName);
        assertEquals(documentCount, docCount);
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
}
