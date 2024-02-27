/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import org.junit.Assert;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.opensearch.neuralsearch.processor.chunker.DelimiterChunker.DELIMITER_FIELD;

public class DelimiterChunkerTests extends OpenSearchTestCase {

    public void testChunkerWithNoDelimiterField() {
        DelimiterChunker chunker = new DelimiterChunker();
        String content = "a\nb\nc\nd";
        Map<String, Object> inputParameters = Map.of("", "");
        Exception exception = assertThrows(IllegalArgumentException.class, () -> chunker.validateParameters(inputParameters));
        Assert.assertEquals("You must contain field:" + DELIMITER_FIELD + " in your parameter.", exception.getMessage());
    }

    public void testChunkerWithDelimiterFieldNotString() {
        DelimiterChunker chunker = new DelimiterChunker();
        String content = "a\nb\nc\nd";
        Map<String, Object> inputParameters = Map.of(DELIMITER_FIELD, List.of(""));
        Exception exception = assertThrows(IllegalArgumentException.class, () -> chunker.validateParameters(inputParameters));
        Assert.assertEquals("delimiter parameters: " + List.of("") + " must be string.", exception.getMessage());
    }

    public void testChunkerWithDelimiterFieldNoString() {
        DelimiterChunker chunker = new DelimiterChunker();
        String content = "a\nb\nc\nd";
        Map<String, Object> inputParameters = Map.of(DELIMITER_FIELD, "");
        Exception exception = assertThrows(IllegalArgumentException.class, () -> chunker.validateParameters(inputParameters));
        Assert.assertEquals("delimiter parameters should not be empty.", exception.getMessage());
    }

    public void testChunker() {
        DelimiterChunker chunker = new DelimiterChunker();
        String content = "a\nb\nc\nd";
        Map<String, Object> inputParameters = Map.of(DELIMITER_FIELD, "\n");
        List<String> chunkResult = chunker.chunk(content, inputParameters);
        assertEquals(List.of("a\n", "b\n", "c\n", "d"), chunkResult);
    }

    public void testChunkerWithDelimiterEnd() {
        DelimiterChunker chunker = new DelimiterChunker();
        String content = "a\nb\nc\nd\n";
        Map<String, Object> inputParameters = Map.of(DELIMITER_FIELD, "\n");
        List<String> chunkResult = chunker.chunk(content, inputParameters);
        assertEquals(List.of("a\n", "b\n", "c\n", "d\n"), chunkResult);
    }

    public void testChunkerWithOnlyDelimiter() {
        DelimiterChunker chunker = new DelimiterChunker();
        String content = "\n";
        Map<String, Object> inputParameters = Map.of(DELIMITER_FIELD, "\n");
        List<String> chunkResult = chunker.chunk(content, inputParameters);
        assertEquals(List.of("\n"), chunkResult);
    }

    public void testChunkerWithAllDelimiters() {
        DelimiterChunker chunker = new DelimiterChunker();
        String content = "\n\n\n";
        Map<String, Object> inputParameters = Map.of(DELIMITER_FIELD, "\n");
        List<String> chunkResult = chunker.chunk(content, inputParameters);
        assertEquals(List.of("\n", "\n", "\n"), chunkResult);
    }

    public void testChunkerWithDifferentDelimiters() {
        DelimiterChunker chunker = new DelimiterChunker();
        String content = "a.b.cc.d.";
        Map<String, Object> inputParameters = Map.of(DELIMITER_FIELD, ".");
        List<String> chunkResult = chunker.chunk(content, inputParameters);
        assertEquals(List.of("a.", "b.", "cc.", "d."), chunkResult);
    }

    public void testChunkerWithStringDelimiter() {
        DelimiterChunker chunker = new DelimiterChunker();
        String content = "\n\na\n\n\n";
        Map<String, Object> inputParameters = Map.of(DELIMITER_FIELD, "\n\n");
        List<String> chunkResult = chunker.chunk(content, inputParameters);
        assertEquals(List.of("\n\n", "a\n\n", "\n"), chunkResult);
    }

}