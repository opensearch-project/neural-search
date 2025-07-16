/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.dto;

import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.neuralsearch.mapper.dto.ChunkingConfig;
import org.opensearch.neuralsearch.processor.TextChunkingProcessorTests;
import org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.mapper.dto.ChunkingConfig.ALGORITHM_FIELD;

public class SemanticFieldInfoTests extends OpenSearchTestCase {

    public void testGetFullPathForChunksInDoc_whenChunkingEnabled_thenReturnPath() {
        final SemanticFieldInfo semanticFieldInfo = createDummySemanticFieldInfo();
        semanticFieldInfo.setChunkingEnabled(true);
        assertEquals("root.path_semantic_info.chunks", semanticFieldInfo.getFullPathForChunksInDoc());
    }

    public void testGetFullPathForChunksInDoc_whenChunkingDisabled_thenException() {
        final SemanticFieldInfo semanticFieldInfo = createDummySemanticFieldInfo();
        final IllegalStateException exception = expectThrows(IllegalStateException.class, semanticFieldInfo::getFullPathForChunksInDoc);
        final String expectedError =
            "Should not try to get full path to chunks for the semantic field at root.path when the chunking is not enabled.";
        assertEquals(expectedError, exception.getMessage());
    }

    public void testGetFullPathForEmbeddingInDoc_whenChunkingEnabled_thenReturnPath() {
        final SemanticFieldInfo semanticFieldInfo = createDummySemanticFieldInfo();
        semanticFieldInfo.setChunkingEnabled(true);
        assertEquals("root.path_semantic_info.chunks.0.embedding", semanticFieldInfo.getFullPathForEmbeddingInDoc(0));
    }

    public void testGetFullPathForEmbeddingInDoc_whenChunkingDisabled_thenReturnPath() {
        final SemanticFieldInfo semanticFieldInfo = createDummySemanticFieldInfo();
        assertEquals("root.path_semantic_info.embedding", semanticFieldInfo.getFullPathForEmbeddingInDoc(0));
    }

    public void testGetFullPathForEmbeddingInDoc_whenChunkingDisabledWithInvalidIndex_thenException() {
        final SemanticFieldInfo semanticFieldInfo = createDummySemanticFieldInfo();
        final IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> semanticFieldInfo.getFullPathForEmbeddingInDoc(1)
        );
        final String expectedError =
            "Should not try to get the full path for the embedding with index 1 when the chunking is not enabled for the semantic field at root.path.";
        assertEquals(expectedError, exception.getMessage());
    }

    public void testGetFullPathForModelInfoInDoc() {
        final SemanticFieldInfo semanticFieldInfo = createDummySemanticFieldInfo();
        assertEquals("root.path_semantic_info.model", semanticFieldInfo.getFullPathForModelInfoInDoc());
    }

    private SemanticFieldInfo createDummySemanticFieldInfo() {
        return SemanticFieldInfo.builder()
            .value("testValue")
            .modelId("model123")
            .semanticFieldFullPathInMapping("root.path")
            .semanticInfoFullPathInDoc("root.path_semantic_info")
            .chunks(List.of("chunk1", "chunk2"))
            .build();
    }

    public void testSetChunkingConfig_whenValidConfig_thenSuccess() {
        AnalysisRegistry analysisRegistry = TextChunkingProcessorTests.getAnalysisRegistry();
        ChunkingConfig chunkingConfig = ChunkingConfig.builder()
            .enabled(true)
            .configs(List.of(Map.of(ALGORITHM_FIELD, FixedTokenLengthChunker.ALGORITHM_NAME)))
            .build();
        SemanticFieldInfo semanticFieldInfo = SemanticFieldInfo.builder().build();
        semanticFieldInfo.setChunkingConfig(chunkingConfig, analysisRegistry);

        assertTrue(semanticFieldInfo.getChunkingEnabled());
        assertEquals(1, semanticFieldInfo.getChunkers().size());
        assertTrue(semanticFieldInfo.getChunkers().getFirst() instanceof FixedTokenLengthChunker);
    }
}
