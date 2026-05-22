/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.utils;

import java.util.List;
import java.util.Map;

import org.opensearch.OpenSearchException;
import org.opensearch.test.OpenSearchTestCase;

public class HighlightTagApplierTests extends OpenSearchTestCase {

    public void testNullHighlightsReturnsNull() {
        assertNull(HighlightTagApplier.applyTags("some text", null, "<em>", "</em>"));
    }

    public void testEmptyHighlightsReturnsNull() {
        assertNull(HighlightTagApplier.applyTags("some text", List.of(), "<em>", "</em>"));
    }

    public void testSingleSpan() {
        String result = HighlightTagApplier.applyTags("alpha beta gamma", List.of(Map.of("start", 6, "end", 10)), "<em>", "</em>");
        assertEquals("alpha <em>beta</em> gamma", result);
    }

    public void testMultipleSpans() {
        String result = HighlightTagApplier.applyTags(
            "alpha beta gamma delta",
            List.of(Map.of("start", 0, "end", 5), Map.of("start", 11, "end", 16)),
            "<em>",
            "</em>"
        );
        assertEquals("<em>alpha</em> beta <em>gamma</em> delta", result);
    }

    public void testAdjacentSpans() {
        String result = HighlightTagApplier.applyTags(
            "abcdef",
            List.of(Map.of("start", 0, "end", 3), Map.of("start", 3, "end", 6)),
            "<b>",
            "</b>"
        );
        assertEquals("<b>abc</b><b>def</b>", result);
    }

    public void testSpanAtEnd() {
        String result = HighlightTagApplier.applyTags("hello world", List.of(Map.of("start", 6, "end", 11)), "<em>", "</em>");
        assertEquals("hello <em>world</em>", result);
    }

    public void testSpanAtStart() {
        String result = HighlightTagApplier.applyTags("hello world", List.of(Map.of("start", 0, "end", 5)), "<em>", "</em>");
        assertEquals("<em>hello</em> world", result);
    }

    public void testEntireText() {
        String result = HighlightTagApplier.applyTags("hello", List.of(Map.of("start", 0, "end", 5)), "<em>", "</em>");
        assertEquals("<em>hello</em>", result);
    }

    public void testCustomTags() {
        String result = HighlightTagApplier.applyTags("hello world", List.of(Map.of("start", 0, "end", 5)), "<mark>", "</mark>");
        assertEquals("<mark>hello</mark> world", result);
    }

    public void testNonNumericStartThrowsException() {
        OpenSearchException ex = expectThrows(
            OpenSearchException.class,
            () -> HighlightTagApplier.applyTags("alpha beta", List.of(Map.of("start", "bad", "end", 5)), "<em>", "</em>")
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("start and end must be numeric"));
    }

    public void testNonNumericEndThrowsException() {
        OpenSearchException ex = expectThrows(
            OpenSearchException.class,
            () -> HighlightTagApplier.applyTags("alpha beta", List.of(Map.of("start", 0, "end", "bad")), "<em>", "</em>")
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("start and end must be numeric"));
    }

    public void testNegativeStartThrowsException() {
        OpenSearchException ex = expectThrows(
            OpenSearchException.class,
            () -> HighlightTagApplier.applyTags("alpha", List.of(Map.of("start", -1, "end", 3)), "<em>", "</em>")
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("Positions must satisfy"));
    }

    public void testEndBeyondTextLengthThrowsException() {
        OpenSearchException ex = expectThrows(
            OpenSearchException.class,
            () -> HighlightTagApplier.applyTags("alpha", List.of(Map.of("start", 0, "end", 100)), "<em>", "</em>")
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("Positions must satisfy"));
    }

    public void testStartEqualsEndThrowsException() {
        OpenSearchException ex = expectThrows(
            OpenSearchException.class,
            () -> HighlightTagApplier.applyTags("alpha", List.of(Map.of("start", 3, "end", 3)), "<em>", "</em>")
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("Positions must satisfy"));
    }

    public void testStartGreaterThanEndThrowsException() {
        OpenSearchException ex = expectThrows(
            OpenSearchException.class,
            () -> HighlightTagApplier.applyTags("alpha", List.of(Map.of("start", 4, "end", 2)), "<em>", "</em>")
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("Positions must satisfy"));
    }

    public void testUnsortedSpansThrowsException() {
        OpenSearchException ex = expectThrows(
            OpenSearchException.class,
            () -> HighlightTagApplier.applyTags(
                "alpha beta gamma",
                List.of(Map.of("start", 6, "end", 10), Map.of("start", 0, "end", 5)),
                "<em>",
                "</em>"
            )
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("not sorted"));
    }

    public void testDuplicateStartThrowsException() {
        OpenSearchException ex = expectThrows(
            OpenSearchException.class,
            () -> HighlightTagApplier.applyTags(
                "alpha beta gamma",
                List.of(Map.of("start", 0, "end", 5), Map.of("start", 0, "end", 10)),
                "<em>",
                "</em>"
            )
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("duplicate start position"));
    }

    public void testOverlappingSpansThrowsException() {
        OpenSearchException ex = expectThrows(
            OpenSearchException.class,
            () -> HighlightTagApplier.applyTags(
                "alpha beta gamma",
                List.of(Map.of("start", 0, "end", 8), Map.of("start", 6, "end", 15)),
                "<em>",
                "</em>"
            )
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("overlapping spans"));
    }
}
