/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.neuralsearch.processor.chunker.DelimiterChunker.DELIMITER_FIELD;

public class DelimiterChunkerTests extends OpenSearchTestCase {

    public void testChunkerWithDelimiterFieldNotString() {
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> new DelimiterChunker(Map.of(DELIMITER_FIELD, List.of("")))
        );
        Assert.assertEquals(
            "Chunker parameter [" + DELIMITER_FIELD + "] cannot be cast to [" + String.class.getName() + "]",
            exception.getMessage()
        );
    }

    public void testChunkerWithDelimiterFieldNoString() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> new DelimiterChunker(Map.of(DELIMITER_FIELD, "")));
        Assert.assertEquals("Chunker parameter: " + DELIMITER_FIELD + " should not be empty.", exception.getMessage());
    }

    public void testChunker() {
        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, "\n"));
        String content = "a\nb\nc\nd";
        List<String> chunkResult = chunker.chunk(content);
        assertEquals(List.of("a\n", "b\n", "c\n", "d"), chunkResult);
    }

    public void testChunkerWithDefaultDelimiter() {
        // default delimiter is \n\n
        DelimiterChunker chunker = new DelimiterChunker(Map.of());
        String content = "a.b\n\nc.d";
        List<String> chunkResult = chunker.chunk(content);
        assertEquals(List.of("a.b\n\n", "c.d"), chunkResult);
    }

    public void testChunkerWithDelimiterEnd() {
        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, "\n"));
        String content = "a\nb\nc\nd\n";
        List<String> chunkResult = chunker.chunk(content);
        assertEquals(List.of("a\n", "b\n", "c\n", "d\n"), chunkResult);
    }

    public void testChunkerWithOnlyDelimiter() {
        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, "\n"));
        String content = "\n";
        List<String> chunkResult = chunker.chunk(content);
        assertEquals(List.of("\n"), chunkResult);
    }

    public void testChunkerWithAllDelimiters() {
        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, "\n"));
        String content = "\n\n\n";
        List<String> chunkResult = chunker.chunk(content);
        assertEquals(List.of("\n", "\n", "\n"), chunkResult);
    }

    public void testChunkerWithDifferentDelimiters() {
        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, "."));
        String content = "a.b.cc.d.";
        List<String> chunkResult = chunker.chunk(content);
        assertEquals(List.of("a.", "b.", "cc.", "d."), chunkResult);
    }

    public void testChunkerWithStringDelimiter() {
        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, "\n\n"));
        String content = "\n\na\n\n\n";
        List<String> chunkResult = chunker.chunk(content);
        assertEquals(List.of("\n\n", "a\n\n", "\n"), chunkResult);
    }
}
