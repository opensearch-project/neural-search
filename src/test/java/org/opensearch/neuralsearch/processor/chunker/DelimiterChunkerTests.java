/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import org.junit.Assert;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.neuralsearch.processor.chunker.Chunker.MAX_CHUNK_LIMIT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.CHUNK_STRING_COUNT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.DelimiterChunker.DELIMITER_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.DelimiterChunker.CHUNK_SIZE_FIELD;

public class DelimiterChunkerTests extends OpenSearchTestCase {

    private final Map<String, Object> runtimeParameters = Map.of(MAX_CHUNK_LIMIT_FIELD, 100, CHUNK_STRING_COUNT_FIELD, 1);

    public void testCreate_withDelimiterFieldInvalidType_thenFail() {
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> new DelimiterChunker(Map.of(DELIMITER_FIELD, List.of("")))
        );
        Assert.assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", DELIMITER_FIELD, String.class.getName()),
            exception.getMessage()
        );
    }

    public void testCreate_withDelimiterFieldEmptyString_thenFail() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> new DelimiterChunker(Map.of(DELIMITER_FIELD, "")));
        Assert.assertEquals(String.format(Locale.ROOT, "Parameter [%s] should not be empty.", DELIMITER_FIELD), exception.getMessage());
    }

    public void testCreate_withChunkSizeInvalidType_thenFail() {
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> new DelimiterChunker(Map.of(CHUNK_SIZE_FIELD, "not-an-integer"))
        );
        Assert.assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", CHUNK_SIZE_FIELD, Integer.class.getName()),
            exception.getMessage()
        );
    }

    public void testCreate_withValidChunkSize_thenSucceed() {
        DelimiterChunker chunker = new DelimiterChunker(Map.of(CHUNK_SIZE_FIELD, 100, DELIMITER_FIELD, "\n"));
        assertNotNull(chunker);
    }

    public void testChunk_withNewlineDelimiter_thenSucceed() {
        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, "\n"));
        String content = "a\nb\nc\nd";
        List<String> chunkResult = chunker.chunk(content, runtimeParameters);
        assertEquals(List.of("a\n", "b\n", "c\n", "d"), chunkResult);
    }

    public void testChunk_withDefaultDelimiter_thenSucceed() {
        // default delimiter is \n\n
        DelimiterChunker chunker = new DelimiterChunker(Map.of());
        String content = "a.b\n\nc.d";
        List<String> chunkResult = chunker.chunk(content, runtimeParameters);
        assertEquals(List.of("a.b\n\n", "c.d"), chunkResult);
    }

    public void testChunk_withOnlyDelimiterContent_thenSucceed() {
        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, "\n"));
        String content = "\n";
        List<String> chunkResult = chunker.chunk(content, runtimeParameters);
        assertEquals(List.of("\n"), chunkResult);
    }

    public void testChunk_WithAllDelimiterContent_thenSucceed() {
        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, "\n"));
        String content = "\n\n\n";
        List<String> chunkResult = chunker.chunk(content, runtimeParameters);
        assertEquals(List.of("\n", "\n", "\n"), chunkResult);
    }

    public void testChunk_WithPeriodDelimiters_thenSucceed() {
        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, "."));
        String content = "a.b.cc.d.";
        List<String> chunkResult = chunker.chunk(content, runtimeParameters);
        assertEquals(List.of("a.", "b.", "cc.", "d."), chunkResult);
    }

    public void testChunk_WithPeriodDelimitersAndChunkSize_thenSucceed() {
        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, ".", CHUNK_SIZE_FIELD, 3));
        String content = "aa.bb.cc c.dd.";
        List<String> chunkResult = chunker.chunk(content, runtimeParameters);
        assertEquals(List.of("aa.", "bb.", "cc ", "c.", "dd."), chunkResult);
    }

    public void testChunk_withDoubleNewlineDelimiter_thenSucceed() {
        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, "\n\n"));
        String content = "\n\na\n\n\n";
        List<String> chunkResult = chunker.chunk(content, runtimeParameters);
        assertEquals(List.of("\n\n", "a\n\n", "\n"), chunkResult);
    }

    public void testChunk_whenWithinMaxChunkLimit_thenSucceed() {
        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, "\n\n"));
        String content = "\n\na\n\n\n";
        int runtimeMaxChunkLimit = 3;
        List<String> chunkResult = chunker.chunk(content, Map.of(CHUNK_STRING_COUNT_FIELD, 1, MAX_CHUNK_LIMIT_FIELD, runtimeMaxChunkLimit));
        assertEquals(List.of("\n\n", "a\n\n", "\n"), chunkResult);
    }

    public void testChunk_whenExceedMaxChunkLimit_thenLastPassageGetConcatenated() {
        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, "\n\n"));
        String content = "\n\na\n\n\n";
        int runtimeMaxChunkLimit = 2;
        List<String> passages = chunker.chunk(content, Map.of(CHUNK_STRING_COUNT_FIELD, 1, MAX_CHUNK_LIMIT_FIELD, runtimeMaxChunkLimit));
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("\n\n");
        expectedPassages.add("a\n\n\n");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_whenExceedMaxChunkLimit_withTwoStringsTobeChunked_thenLastPassageGetConcatenated() {
        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, "\n\n"));
        String content = "\n\na\n\n\n";
        int runtimeMaxChunkLimit = 2, chunkStringCount = 2;
        List<String> passages = chunker.chunk(
            content,
            Map.of(MAX_CHUNK_LIMIT_FIELD, runtimeMaxChunkLimit, CHUNK_STRING_COUNT_FIELD, chunkStringCount)
        );
        List<String> expectedPassages = List.of("\n\na\n\n\n");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_whenOnlyDelimitersPresent_withChunkSizeTooSmall_thenEachDelimiterIsOwnChunk() {
        String content = "\n\n\n\n";
        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, "\n\n", CHUNK_SIZE_FIELD, 1));
        List<String> chunkResult = chunker.chunk(content, runtimeParameters);
        assertEquals(List.of("\n\n", "\n\n"), chunkResult);
    }

    public void testChunk_whenOnlyDelimiterPresent_withChunkSizeLargerThanContent_thenSingleChunkReturned() {
        String content = "\n\n";
        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, "\n\n", CHUNK_SIZE_FIELD, 3));
        List<String> chunkResult = chunker.chunk(content, runtimeParameters);
        assertEquals(List.of("\n\n"), chunkResult);
    }

    public void testChunk_whenNewlineDelimiterUsed_andChunkSizeIsLimited_thenSplitAtNewlineAndChunkSize() {
        String content = """
            OpenSearch is a community-driven project.
            It consists of a search engine and visualization tools.
            Contributions are welcome!
            """;

        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, "\n", CHUNK_SIZE_FIELD, 50));
        List<String> chunkResult = chunker.chunk(content, runtimeParameters);

        assertEquals(
            List.of(
                "OpenSearch is a community-driven project.\n",
                "It consists of a search engine and visualization t",
                "ools.\n",
                "Contributions are welcome!\n"
            ),
            chunkResult
        );
    }

    public void testChunk_whenSpaceDelimiterUsed_andChunkSizeIsSmall_thenSplitAtSpacesRespectingSize() {
        String content = "This is a sample text that includes several words.";
        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, " ", CHUNK_SIZE_FIELD, 10));
        List<String> chunkResult = chunker.chunk(content, runtimeParameters);

        assertEquals(List.of("This ", "is ", "a ", "sample ", "text ", "that ", "includes ", "several ", "words."), chunkResult);
    }

    public void testChunk_whenParagraphDelimiterUsed_andParagraphExceedsChunkSize_thenSplitWithinParagraphBySize() {
        String content = """
            Paragraph one is short.

            Paragraph two is significantly longer and contains multiple sentences. \
            This should be split properly if it exceeds the specified chunk size.

            Third para.
            """;

        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, "\n\n", CHUNK_SIZE_FIELD, 60));
        List<String> chunkResult = chunker.chunk(content, runtimeParameters);

        assertEquals(
            List.of(
                "Paragraph one is short.\n\n",
                "Paragraph two is significantly longer and contains multiple ",
                "sentences. This should be split properly if it exceeds the s",
                "pecified chunk size.\n\n",
                "Third para.\n"
            ),
            chunkResult
        );
    }

    public void testChunk_whenExceedMaxChunkLimit_withDelimiterSegments_thenRemainingContentAppendedToLastChunk() {
        String content = "one. two. three. four. five six.";

        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, ".", CHUNK_SIZE_FIELD, 6));

        int runtimeMaxChunkLimit = 2, chunkStringCount = 1;

        List<String> result = chunker.chunk(
            content,
            Map.of(MAX_CHUNK_LIMIT_FIELD, runtimeMaxChunkLimit, CHUNK_STRING_COUNT_FIELD, chunkStringCount)
        );

        assertEquals(List.of("one.", " two. three. four. five six."), result);
    }

    public void testChunk_whenExceedMaxChunkLimit_withChunkSizeSplit_thenRemainingContentAppendedToLastChunk() {
        String content = "one, two. three. four. five six.";

        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, ".", CHUNK_SIZE_FIELD, 6));

        int runtimeMaxChunkLimit = 3, chunkStringCount = 1;

        List<String> result = chunker.chunk(
            content,
            Map.of(MAX_CHUNK_LIMIT_FIELD, runtimeMaxChunkLimit, CHUNK_STRING_COUNT_FIELD, chunkStringCount)
        );

        assertEquals(List.of("one, t", "wo.", " three. four. five six."), result);
    }

    public void testChunk_WhenChunkIsSubdividedByChunkSize_AndMaxLimitIsHit_ShouldMergeRemainder() {
        String content = "abcd.efgh";

        DelimiterChunker chunker = new DelimiterChunker(Map.of(DELIMITER_FIELD, ".", CHUNK_SIZE_FIELD, 2));

        int runtimeMaxChunkLimit = 2, chunkStringCount = 1;

        List<String> result = chunker.chunk(
            content,
            Map.of(MAX_CHUNK_LIMIT_FIELD, runtimeMaxChunkLimit, CHUNK_STRING_COUNT_FIELD, chunkStringCount)
        );

        assertEquals(List.of("ab", "cd.efgh"), result);
    }
}
