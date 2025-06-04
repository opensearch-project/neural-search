/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.util;

import org.opensearch.neuralsearch.processor.chunker.Chunker;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.CHUNK_STRING_COUNT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.MAX_CHUNK_LIMIT_FIELD;

public class ChunkUtilsTests extends OpenSearchTestCase {

    public void testChunkString_whenEmptyContent_thenEmptyString() {
        final Chunker chunker = mock(Chunker.class);
        final Map<String, Object> runtimeParameters = new HashMap<>();
        List<String> results = ChunkUtils.chunkString(chunker, "", runtimeParameters);

        assertTrue("Should return an empty string when the content is empty", results.isEmpty());
    }

    public void testChunkString_whenNormalContent_thenReturnResultsAndUpdateRuntimeParameters() {
        final Chunker chunker = mock(Chunker.class);
        final String content = "test";
        final Map<String, Object> runtimeParameters = new HashMap<>();
        runtimeParameters.put(CHUNK_STRING_COUNT_FIELD, 1);
        runtimeParameters.put(MAX_CHUNK_LIMIT_FIELD, 10);
        when(chunker.chunk(content, runtimeParameters)).thenReturn(Arrays.stream(content.split("")).toList());
        List<String> results = ChunkUtils.chunkString(chunker, content, runtimeParameters);

        List<String> expectedResults = List.of("t", "e", "s", "t");
        assertEquals(expectedResults, results);
        assertEquals(0, runtimeParameters.get(CHUNK_STRING_COUNT_FIELD));
        assertEquals(6, runtimeParameters.get(MAX_CHUNK_LIMIT_FIELD));
    }

    public void testChunkList_whenNormalContentWithoutMaxLimit_thenReturnResults() {
        final Chunker chunker = mock(Chunker.class);
        final List<String> contents = List.of("test", "test");
        final Map<String, Object> runtimeParameters = new HashMap<>();
        runtimeParameters.put(CHUNK_STRING_COUNT_FIELD, 2);
        runtimeParameters.put(MAX_CHUNK_LIMIT_FIELD, -1);
        when(chunker.chunk("test", runtimeParameters)).thenReturn(Arrays.stream("test".split("")).toList());
        List<String> results = ChunkUtils.chunkList(chunker, contents, runtimeParameters);

        List<String> expectedResults = List.of("t", "e", "s", "t", "t", "e", "s", "t");
        assertEquals(expectedResults, results);
        assertEquals(0, runtimeParameters.get(CHUNK_STRING_COUNT_FIELD));
        assertEquals(-1, runtimeParameters.get(MAX_CHUNK_LIMIT_FIELD));
    }
}
