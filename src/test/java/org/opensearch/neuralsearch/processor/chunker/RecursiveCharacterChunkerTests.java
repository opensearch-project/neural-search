/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.processor.chunker.Chunker.MAX_CHUNK_LIMIT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.CHUNK_STRING_COUNT_FIELD;

public class RecursiveCharacterChunkerTests extends OpenSearchTestCase {

    private final Map<String, Object> runtimeParameters = Map.of(MAX_CHUNK_LIMIT_FIELD, 100, CHUNK_STRING_COUNT_FIELD, 1);

    public void testCreate_withInvalidDelimitersType_thenFail() {
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> new RecursiveCharacterChunker(Map.of("delimiters", "notAList"))
        );
        assertEquals("Parameter [delimiters] must be of type List<String>", exception.getMessage());
    }

    public void testChunk_whenWithinLimit_thenChunksSplitProperly() {
        RecursiveCharacterChunker chunker = new RecursiveCharacterChunker(Map.of("delimiters", List.of(". ", " ", ""), "chunk_size", 5));
        String content = "This is a test. This is another.";
        List<String> chunkResult = chunker.chunk(content, runtimeParameters);
        for (String chunk : chunkResult) {
            assertTrue(chunk.length() <= 5);
        }
    }

    public void testChunk_whenContentShorterThanLimit_thenReturnsWhole() {
        RecursiveCharacterChunker chunker = new RecursiveCharacterChunker(Map.of("chunk_size", 50));
        String content = "Short text.";
        List<String> result = chunker.chunk(content, runtimeParameters);
        assertEquals(1, result.size());
        assertEquals("Short text.", result.get(0));
    }

    public void testChunk_whenNoDelimitersProvided_thenSplitByCharacters() {
        RecursiveCharacterChunker chunker = new RecursiveCharacterChunker(Map.of("chunk_size", 1));
        String content = "abc";
        List<String> result = chunker.chunk(content, runtimeParameters);
        assertEquals(List.of("a", "b", "c"), result);
    }

    public void testChunk_whenExceedsMaxChunkLimit_thenLastChunksAreConcatenated() {
        RecursiveCharacterChunker chunker = new RecursiveCharacterChunker(Map.of("delimiters", List.of(". ", " ", ""), "chunk_size", 5));
        String content = "a b c d e f g h";
        int runtimeMaxChunkLimit = 3;
        List<String> result = chunker.chunk(content, Map.of(MAX_CHUNK_LIMIT_FIELD, runtimeMaxChunkLimit, CHUNK_STRING_COUNT_FIELD, 1));
        assertTrue(result.size() <= runtimeMaxChunkLimit);
    }

    public void testChunk_withNestedDelimiters_thenRespectsOrder() {
        RecursiveCharacterChunker chunker = new RecursiveCharacterChunker(Map.of("delimiters", List.of(". ", " ", ""), "chunk_size", 5));
        String content = "abc def. ghi jkl.";
        List<String> result = chunker.chunk(content, runtimeParameters);
        logger.info(result.toString());
        for (String chunk : result) {
            assertTrue(chunk.length() <= 5);
        }
    }
}
