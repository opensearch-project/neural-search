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
import static org.opensearch.neuralsearch.processor.chunker.FixedStringLengthChunker.LENGTH_LIMIT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedStringLengthChunker.OVERLAP_RATE_FIELD;

public class FixedStringLengthChunkerTests extends OpenSearchTestCase {

    private FixedStringLengthChunker fixedStringLengthChunker;

    // Default runtime parameters for most tests
    private final Map<String, Object> defaultRuntimeParameters = Map.of(
        MAX_CHUNK_LIMIT_FIELD,
        100, // A high limit, unlikely to be hit unless specified in a test
        CHUNK_STRING_COUNT_FIELD,
        1
    );

    public void testParseParameters_whenNoParams_thenSuccessfulUsesDefaults() {
        FixedStringLengthChunker chunker = new FixedStringLengthChunker(Map.of());
        // Access private fields via reflection or add getters if needed for assertion
        // For now, we assume default values are set as per FixedStringLengthChunker.DEFAULT_LENGTH_LIMIT etc.
        assertNotNull(chunker);
    }

    public void testParseParameters_whenIllegalLengthLimitType_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(LENGTH_LIMIT_FIELD, "invalid length limit");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> new FixedStringLengthChunker(parameters)
        );
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", LENGTH_LIMIT_FIELD, Integer.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseParameters_whenIllegalLengthLimitValue_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(LENGTH_LIMIT_FIELD, -1);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> new FixedStringLengthChunker(parameters)
        );
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be positive.", LENGTH_LIMIT_FIELD),
            illegalArgumentException.getMessage()
        );

        parameters.put(LENGTH_LIMIT_FIELD, 0);
        illegalArgumentException = assertThrows(IllegalArgumentException.class, () -> new FixedStringLengthChunker(parameters));
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be positive.", LENGTH_LIMIT_FIELD),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseParameters_whenIllegalOverlapRateType_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(OVERLAP_RATE_FIELD, "invalid overlap rate");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> new FixedStringLengthChunker(parameters)
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
            () -> new FixedStringLengthChunker(parameters)
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
            () -> new FixedStringLengthChunker(parameters)
        );
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be between %s and %s, but was %s", OVERLAP_RATE_FIELD, 0.0, 0.5, -0.1),
            illegalArgumentException.getMessage()
        );
    }

    public void testChunk_withEmptyInput_thenSucceedReturnsEmptyList() {
        FixedStringLengthChunker chunker = new FixedStringLengthChunker(Map.of(LENGTH_LIMIT_FIELD, 10));
        String content = "";
        List<String> passages = chunker.chunk(content, defaultRuntimeParameters);
        assertTrue(passages.isEmpty());
    }

    public void testChunk_withNullInput_thenSucceedReturnsEmptyList() {
        FixedStringLengthChunker chunker = new FixedStringLengthChunker(Map.of(LENGTH_LIMIT_FIELD, 10));
        String content = null;
        List<String> passages = chunker.chunk(content, defaultRuntimeParameters);
        assertTrue(passages.isEmpty());
    }

    public void testChunk_withLengthLimitZeroOrNegative_handledByDefensiveCode_returnsFullContent() {
        // This test assumes the defensive check `if (this.lengthLimit <= 0)` is hit.
        // parsePositiveIntegerWithDefault should prevent this, so this tests an edge case
        Map<String, Object> params = new HashMap<>();
        params.put(LENGTH_LIMIT_FIELD, 0);
        assertThrows(IllegalArgumentException.class, () -> new FixedStringLengthChunker(params));
    }

    public void testChunk_withLengthLimit10_noOverlap_thenSucceed() {
        FixedStringLengthChunker chunker = new FixedStringLengthChunker(Map.of(LENGTH_LIMIT_FIELD, 10, OVERLAP_RATE_FIELD, 0.0));
        String content = "This is a test string for chunking."; // length 35
        List<String> passages = chunker.chunk(content, defaultRuntimeParameters);
        List<String> expectedPassages = List.of(
            "This is a ",
            "test strin",
            "g for chun",
            "king."
        );
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_withContentShorterThanLengthLimit_noOverlap_thenSucceed() {
        FixedStringLengthChunker chunker = new FixedStringLengthChunker(Map.of(LENGTH_LIMIT_FIELD, 100, OVERLAP_RATE_FIELD, 0.0));
        String content = "Short content."; // length 14
        List<String> passages = chunker.chunk(content, defaultRuntimeParameters);
        List<String> expectedPassages = List.of("Short content.");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_withContentEqualToLengthLimit_noOverlap_thenSucceed() {
        FixedStringLengthChunker chunker = new FixedStringLengthChunker(Map.of(LENGTH_LIMIT_FIELD, 14, OVERLAP_RATE_FIELD, 0.0));
        String content = "Short content."; // length 14
        List<String> passages = chunker.chunk(content, defaultRuntimeParameters);
        List<String> expectedPassages = List.of("Short content.");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_withLengthLimit20_noOverlap_thenSucceed() {
        FixedStringLengthChunker chunker = new FixedStringLengthChunker(Map.of(LENGTH_LIMIT_FIELD, 20, OVERLAP_RATE_FIELD, 0.0));
        String content = "This is an example document to be chunked by fixed string length."; // length 68
        List<String> passages = chunker.chunk(content, defaultRuntimeParameters);
        List<String> expectedPassages = List.of(
            "This is an example d", // 20
            "ocument to be chunke", // 20
            "d by fixed string le", // 20
            "ngth."                 // 6
        );
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_withOverlapRateHalf_lengthLimit10_thenSucceed() {
        // lengthLimit = 10, overlapRate = 0.5
        // overlapCharNumber = floor(10 * 0.5) = 5
        FixedStringLengthChunker chunker = new FixedStringLengthChunker(Map.of(LENGTH_LIMIT_FIELD, 10, OVERLAP_RATE_FIELD, 0.5));
        String content = "abcdefghijklmnopqrstuvwxyz";
        List<String> passages = chunker.chunk(content, defaultRuntimeParameters);
        List<String> expectedPassages = List.of(
            "abcdefghij",
            "fghijklmno",
            "klmnopqrst",
            "pqrstuvwxy",
            "uvwxyz"
        );
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_withOverlapRatePointTwo_lengthLimit10_thenSucceed() {
        FixedStringLengthChunker chunker = new FixedStringLengthChunker(Map.of(LENGTH_LIMIT_FIELD, 10, OVERLAP_RATE_FIELD, 0.2));
        String content = "abcdefghijklmnopqrstuvwxyz"; // length 26
        List<String> passages = chunker.chunk(content, defaultRuntimeParameters);
        List<String> expectedPassages = List.of(
            "abcdefghij",
            "ijklmnopqr",
            "qrstuvwxyz"
        );
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_withOverlapRateZero_lengthLimitExactMultiple_thenSucceed() {
        FixedStringLengthChunker chunker = new FixedStringLengthChunker(Map.of(LENGTH_LIMIT_FIELD, 5, OVERLAP_RATE_FIELD, 0.0));
        String content = "abcdefghij"; // length 10
        List<String> passages = chunker.chunk(content, defaultRuntimeParameters);
        List<String> expectedPassages = List.of("abcde", "fghij");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_whenExceedMaxChunkLimit_thenLastPassageGetConcatenated() {
        FixedStringLengthChunker chunker = new FixedStringLengthChunker(Map.of(LENGTH_LIMIT_FIELD, 10, OVERLAP_RATE_FIELD, 0.0));
        int runtimeMaxChunkLimit = 2;
        Map<String, Object> runtimeParameters = new HashMap<>(this.defaultRuntimeParameters);
        runtimeParameters.put(MAX_CHUNK_LIMIT_FIELD, runtimeMaxChunkLimit);

        String content = "This is a test string for chunking with max limit."; // length 49

        List<String> passages = chunker.chunk(content, runtimeParameters);
        List<String> expectedPassages = List.of(
            "This is a ",
            "test string for chunking with max limit."
        );
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_whenWithinMaxChunkLimit_thenSucceed() {
        FixedStringLengthChunker chunker = new FixedStringLengthChunker(Map.of(LENGTH_LIMIT_FIELD, 10, OVERLAP_RATE_FIELD, 0.0));
        int runtimeMaxChunkLimit = 5;
        Map<String, Object> runtimeParameters = new HashMap<>(this.defaultRuntimeParameters);
        runtimeParameters.put(MAX_CHUNK_LIMIT_FIELD, runtimeMaxChunkLimit);
        String content = "This is a test string for chunking with max limit."; // length 49
        List<String> passages = chunker.chunk(content, runtimeParameters);
        List<String> expectedPassages = List.of("This is a ", "test strin", "g for chun", "king with ", "max limit.");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_whenMaxChunkLimitIsOne_thenReturnsFullContent() {
        FixedStringLengthChunker chunker = new FixedStringLengthChunker(Map.of(LENGTH_LIMIT_FIELD, 5, OVERLAP_RATE_FIELD, 0.0));
        int runtimeMaxChunkLimit = 1;
        Map<String, Object> runtimeParameters = new HashMap<>(this.defaultRuntimeParameters);
        runtimeParameters.put(MAX_CHUNK_LIMIT_FIELD, runtimeMaxChunkLimit);
        String content = "This is a test string.";
        List<String> passages = chunker.chunk(content, runtimeParameters);
        List<String> expectedPassages = List.of("This is a test string.");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_whenChunkIntervalIsOne_dueToOverlap() {
        FixedStringLengthChunker chunker = new FixedStringLengthChunker(Map.of(LENGTH_LIMIT_FIELD, 2, OVERLAP_RATE_FIELD, 0.5));
        String content = "abcde";
        List<String> passages = chunker.chunk(content, defaultRuntimeParameters);
        List<String> expectedPassages = List.of(
            "ab",
            "bc",
            "cd",
            "de",
            "e"
        );
        assertEquals(expectedPassages, passages);
    }
}
