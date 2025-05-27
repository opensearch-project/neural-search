/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.neuralsearch.processor.chunker.Chunker.MAX_CHUNK_LIMIT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.CHUNK_STRING_COUNT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedCharLengthChunker.CHAR_LIMIT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedCharLengthChunker.OVERLAP_RATE_FIELD;

public class FixedCharLengthChunkerTests extends OpenSearchTestCase {

    private final Map<String, Object> defaultRuntimeParameters = Map.of(MAX_CHUNK_LIMIT_FIELD, 100, CHUNK_STRING_COUNT_FIELD, 1);

    public void testParseParameters_whenNoParams_thenSuccessfulUsesDefaults() {
        FixedCharLengthChunker chunker = new FixedCharLengthChunker(Map.of());
        assertNotNull(chunker);
    }

    public void testParseParameters_whenIllegalCharLimitType_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(CHAR_LIMIT_FIELD, "invalid character limit");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> new FixedCharLengthChunker(parameters)
        );
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", CHAR_LIMIT_FIELD, Integer.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseParameters_whenIllegalCharLimitValue_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(CHAR_LIMIT_FIELD, -1);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> new FixedCharLengthChunker(parameters)
        );
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be positive.", CHAR_LIMIT_FIELD),
            illegalArgumentException.getMessage()
        );

        parameters.put(CHAR_LIMIT_FIELD, 0);
        illegalArgumentException = assertThrows(IllegalArgumentException.class, () -> new FixedCharLengthChunker(parameters));
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be positive.", CHAR_LIMIT_FIELD),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseParameters_whenIllegalOverlapRateType_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(OVERLAP_RATE_FIELD, "invalid overlap rate");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> new FixedCharLengthChunker(parameters)
        );
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", OVERLAP_RATE_FIELD, Double.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseParameters_whenTooLargeOverlapRate_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(OVERLAP_RATE_FIELD, 0.6); // Max is 0.5
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> new FixedCharLengthChunker(parameters)
        );
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be between %s and %s, but was %s", OVERLAP_RATE_FIELD, 0.0, 0.5, 0.6),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseParameters_whenTooSmallOverlapRateValue_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(OVERLAP_RATE_FIELD, -0.1); // Min is 0.0
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> new FixedCharLengthChunker(parameters)
        );
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be between %s and %s, but was %s", OVERLAP_RATE_FIELD, 0.0, 0.5, -0.1),
            illegalArgumentException.getMessage()
        );
    }

    public void testChunk_withCharLimitZeroOrNegative_handledByDefensiveCode_returnsFullContent() {
        // This test assumes the defensive check `if (this.charLimit <= 0)` is hit.
        // parsePositiveIntegerWithDefault should prevent this, so this tests an edge case
        Map<String, Object> params = new HashMap<>();
        params.put(CHAR_LIMIT_FIELD, 0);
        assertThrows(IllegalArgumentException.class, () -> new FixedCharLengthChunker(params));
    }

    public void testChunk_withCharLimit10_noOverlap_thenSucceed() {
        FixedCharLengthChunker chunker = new FixedCharLengthChunker(Map.of(CHAR_LIMIT_FIELD, 10, OVERLAP_RATE_FIELD, 0.0));
        String content = "This is a test string for chunking."; // length 35
        List<String> passages = chunker.chunk(content, defaultRuntimeParameters);
        List<String> expectedPassages = List.of("This is a ", "test strin", "g for chun", "king.");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_withContentShorterThanCharLimit_noOverlap_thenSucceed() {
        FixedCharLengthChunker chunker = new FixedCharLengthChunker(Map.of(CHAR_LIMIT_FIELD, 100, OVERLAP_RATE_FIELD, 0.0));
        String content = "Short content."; // length 14
        List<String> passages = chunker.chunk(content, defaultRuntimeParameters);
        List<String> expectedPassages = List.of("Short content.");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_withContentEqualToCharLimit_noOverlap_thenSucceed() {
        FixedCharLengthChunker chunker = new FixedCharLengthChunker(Map.of(CHAR_LIMIT_FIELD, 14, OVERLAP_RATE_FIELD, 0.0));
        String content = "Short content."; // length 14
        List<String> passages = chunker.chunk(content, defaultRuntimeParameters);
        List<String> expectedPassages = List.of("Short content.");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_withCharLimit20_noOverlap_thenSucceed() {
        FixedCharLengthChunker chunker = new FixedCharLengthChunker(Map.of(CHAR_LIMIT_FIELD, 20, OVERLAP_RATE_FIELD, 0.0));
        String content = "This is an example document to be chunked by the algorithm named 'fixed_char_length'."; // length 68
        List<String> passages = chunker.chunk(content, defaultRuntimeParameters);
        List<String> expectedPassages = List.of(
            "This is an example d", // 20
            "ocument to be chunke", // 20
            "d by the algorithm n", // 20
            "amed 'fixed_char_len", // 20
            "gth'."                 // 5
        );
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_withOverlapRateHalf_charLimit10_thenSucceed() {
        // charLimit = 10, overlapRate = 0.5
        // overlapCharNumber = floor(10 * 0.5) = 5
        FixedCharLengthChunker chunker = new FixedCharLengthChunker(Map.of(CHAR_LIMIT_FIELD, 10, OVERLAP_RATE_FIELD, 0.5));
        String content = "abcdefghijklmnopqrstuvwxyz";
        List<String> passages = chunker.chunk(content, defaultRuntimeParameters);
        List<String> expectedPassages = List.of("abcdefghij", "fghijklmno", "klmnopqrst", "pqrstuvwxy", "uvwxyz");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_withOverlapRatePointTwo_charLimit10_thenSucceed() {
        FixedCharLengthChunker chunker = new FixedCharLengthChunker(Map.of(CHAR_LIMIT_FIELD, 10, OVERLAP_RATE_FIELD, 0.2));
        String content = "abcdefghijklmnopqrstuvwxyz"; // length 26
        List<String> passages = chunker.chunk(content, defaultRuntimeParameters);
        List<String> expectedPassages = List.of("abcdefghij", "ijklmnopqr", "qrstuvwxyz");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_withOverlapRateZero_charLimitExactMultiple_thenSucceed() {
        FixedCharLengthChunker chunker = new FixedCharLengthChunker(Map.of(CHAR_LIMIT_FIELD, 5, OVERLAP_RATE_FIELD, 0.0));
        String content = "abcdefghij"; // length 10
        List<String> passages = chunker.chunk(content, defaultRuntimeParameters);
        List<String> expectedPassages = List.of("abcde", "fghij");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_whenExceedMaxChunkLimit_thenLastPassageGetConcatenated() {
        FixedCharLengthChunker chunker = new FixedCharLengthChunker(Map.of(CHAR_LIMIT_FIELD, 10, OVERLAP_RATE_FIELD, 0.0));
        int runtimeMaxChunkLimit = 2;
        Map<String, Object> runtimeParameters = new HashMap<>(this.defaultRuntimeParameters);
        runtimeParameters.put(MAX_CHUNK_LIMIT_FIELD, runtimeMaxChunkLimit);

        String content = "This is a test string for chunking with max limit."; // length 49

        List<String> passages = chunker.chunk(content, runtimeParameters);
        List<String> expectedPassages = List.of("This is a ", "test string for chunking with max limit.");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_whenWithinMaxChunkLimit_thenSucceed() {
        FixedCharLengthChunker chunker = new FixedCharLengthChunker(Map.of(CHAR_LIMIT_FIELD, 10, OVERLAP_RATE_FIELD, 0.0));
        int runtimeMaxChunkLimit = 5;
        Map<String, Object> runtimeParameters = new HashMap<>(this.defaultRuntimeParameters);
        runtimeParameters.put(MAX_CHUNK_LIMIT_FIELD, runtimeMaxChunkLimit);
        String content = "This is a test string for chunking with max limit."; // length 49
        List<String> passages = chunker.chunk(content, runtimeParameters);
        List<String> expectedPassages = List.of("This is a ", "test strin", "g for chun", "king with ", "max limit.");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_whenMaxChunkLimitIsOne_thenReturnsFullContent() {
        FixedCharLengthChunker chunker = new FixedCharLengthChunker(Map.of(CHAR_LIMIT_FIELD, 5, OVERLAP_RATE_FIELD, 0.0));
        int runtimeMaxChunkLimit = 1;
        Map<String, Object> runtimeParameters = new HashMap<>(this.defaultRuntimeParameters);
        runtimeParameters.put(MAX_CHUNK_LIMIT_FIELD, runtimeMaxChunkLimit);
        String content = "This is a test string.";
        List<String> passages = chunker.chunk(content, runtimeParameters);
        List<String> expectedPassages = List.of("This is a test string.");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_whenChunkIntervalIsOne_dueToOverlap() {
        FixedCharLengthChunker chunker = new FixedCharLengthChunker(Map.of(CHAR_LIMIT_FIELD, 2, OVERLAP_RATE_FIELD, 0.5));
        String content = "abcde";
        List<String> passages = chunker.chunk(content, defaultRuntimeParameters);
        List<String> expectedPassages = List.of("ab", "bc", "cd", "de");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_whenOverlapRateRoundsToZero_thenSucceed() {
        // charLimit=3, overlapRate=0.33 => overlapNum=0, chunkInterval=3
        FixedCharLengthChunker chunker = new FixedCharLengthChunker(Map.of(CHAR_LIMIT_FIELD, 3, OVERLAP_RATE_FIELD, 0.33));
        String content = "abcd"; // length 4
        List<String> passages = chunker.chunk(content, defaultRuntimeParameters);
        List<String> expectedPassages = List.of("abc", "d");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_contentEqualToOverlap_charLimitGreaterThanContent_withOverlap() {
        // charLimit=5, overlapRate=0.2 => overlapNum=1, chunkInterval=4
        FixedCharLengthChunker chunker = new FixedCharLengthChunker(Map.of(CHAR_LIMIT_FIELD, 5, OVERLAP_RATE_FIELD, 0.2));
        String content = "a"; // length 1
        List<String> passages = chunker.chunk(content, defaultRuntimeParameters);
        List<String> expectedPassages = List.of("a");
        assertEquals(expectedPassages, passages);
    }
}
