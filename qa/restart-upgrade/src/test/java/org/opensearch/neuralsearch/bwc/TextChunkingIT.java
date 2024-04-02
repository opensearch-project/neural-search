/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.opensearch.index.query.MatchAllQueryBuilder;
import static org.opensearch.neuralsearch.TestUtils.NODES_BWC_CLUSTER;

public class TextChunkingIT extends AbstractRestartUpgradeRestTestCase {

    private static final String PIPELINE_NAME = "pipeline-text-chunking";
    private static final String INPUT_FIELD = "body";
    private static final String OUTPUT_FIELD = "body_chunk";
    private static final String DOCUMENT_PATH = "processor/ChunkingTestDocument.json";
    List<String> expectedPassages = List.of(
        "This is an example document to be chunked. The document ",
        "contains a single paragraph, two sentences and 24 tokens by ",
        "standard tokenizer in OpenSearch."
    );

    // Test rolling-upgrade text chunking processor
    // Create Text Chunking Processor, Ingestion Pipeline and add document
    // Validate process, pipeline and document count in restart-upgrade scenario
    public void testTextChunkingProcessor_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        URL documentURLPath = classLoader.getResource(DOCUMENT_PATH);
        Objects.requireNonNull(documentURLPath);
        String document = Files.readString(Path.of(documentURLPath.toURI()));
        String indexName = getIndexNameForTest();
        if (isRunningAgainstOldCluster()) {
            createPipelineForTextChunkingProcessor(PIPELINE_NAME);
            createChunkingIndex();
            addDocument(indexName, "0", INPUT_FIELD, document, null, null);
        } else {
            try {
                addDocument(indexName, "1", INPUT_FIELD, document, null, null);
                validateTestIndex(indexName, OUTPUT_FIELD, expectedPassages);
            } finally {
                wipeOfTestResources(indexName, PIPELINE_NAME, null, null);
            }
        }
    }

    private void createChunkingIndex() throws Exception {
        createIndexWithConfiguration(getIndexNameForTest(), "{}", PIPELINE_NAME);
    }

    private void validateTestIndex(String indexName, String fieldName, Object expected) {
        int docCount = getDocCount(indexName);
        assertEquals(2, docCount);
        MatchAllQueryBuilder query = new MatchAllQueryBuilder();
        Map<String, Object> searchResults = search(getIndexNameForTest(), query, 10);
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
