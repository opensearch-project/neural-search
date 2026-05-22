/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.batch.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.TotalHits;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.test.OpenSearchTestCase;

public class HighlightResultApplierTests extends OpenSearchTestCase {

    private final HighlightResultApplier applier = new HighlightResultApplier();

    public void testAppliesSingleSpanWithDefaultEncoder() {
        SearchHit hit = hitWithSource(Map.of("body", "alpha beta gamma"));
        applier.applyBatchResults(
            List.of(hit),
            List.of(List.of(Map.of("start", 6, "end", 10))),
            List.of("body"),
            List.of("<em>"),
            List.of("</em>"),
            List.of(0),
            List.of("default")
        );
        assertEquals("alpha <em>beta</em> gamma", highlightedValue(hit, "body"));
    }

    public void testAppliesMultipleSpansInOrder() {
        SearchHit hit = hitWithSource(Map.of("body", "alpha beta gamma delta"));
        applier.applyBatchResults(
            List.of(hit),
            List.of(List.of(Map.of("start", 0, "end", 5), Map.of("start", 11, "end", 16))),
            List.of("body"),
            List.of("<em>"),
            List.of("</em>"),
            List.of(0),
            List.of("default")
        );
        assertEquals("<em>alpha</em> beta <em>gamma</em> delta", highlightedValue(hit, "body"));
    }

    public void testHtmlEncoderEscapesAngleBrackets() {
        SearchHit hit = hitWithSource(Map.of("body", "x <script> y match"));
        applier.applyBatchResults(
            List.of(hit),
            List.of(List.of(Map.of("start", 13, "end", 18))),
            List.of("body"),
            List.of("<em>"),
            List.of("</em>"),
            List.of(0),
            List.of("html")
        );
        String value = highlightedValue(hit, "body");
        assertTrue(value, value.contains("&lt;script&gt;"));
        assertTrue(value, value.contains("<em>match</em>"));
    }

    public void testNoSpansAndZeroNoMatchSizeEmitsNoHighlight() {
        SearchHit hit = hitWithSource(Map.of("body", "alpha beta"));
        applier.applyBatchResults(
            List.of(hit),
            List.of(List.of()),
            List.of("body"),
            List.of("<em>"),
            List.of("</em>"),
            List.of(0),
            List.of("default")
        );
        assertNull(hit.getHighlightFields().get("body"));
    }

    public void testNoSpansAndPositiveNoMatchSizeEmitsTrimmedSnippet() {
        SearchHit hit = hitWithSource(Map.of("body", "abcdefghij"));
        applier.applyBatchResults(
            List.of(hit),
            List.of(List.of()),
            List.of("body"),
            List.of("<em>"),
            List.of("</em>"),
            List.of(4),
            List.of("default")
        );
        assertEquals("abcd", highlightedValue(hit, "body"));
    }

    public void testInvalidSpansThrowException() {
        SearchHit hit = hitWithSource(Map.of("body", "alpha beta gamma"));
        expectThrows(
            Exception.class,
            () -> applier.applyBatchResults(
                List.of(hit),
                List.of(List.of(Map.of("start", "not-a-number", "end", 10), Map.of("start", 6, "end", 10))),
                List.of("body"),
                List.of("<em>"),
                List.of("</em>"),
                List.of(0),
                List.of("default")
            )
        );
    }

    public void testNestedFieldFallsBackToLeafName() {
        SearchHit hit = hitWithSource(Map.of("text", "alpha beta gamma"));
        applier.applyBatchResults(
            List.of(hit),
            List.of(List.of(Map.of("start", 6, "end", 10))),
            List.of("chunks.text"),
            List.of("<em>"),
            List.of("</em>"),
            List.of(0),
            List.of("default")
        );
        assertEquals("alpha <em>beta</em> gamma", highlightedValue(hit, "chunks.text"));
    }

    public void testBatchSizeMismatchThrows() {
        SearchHit hit = hitWithSource(Map.of("body", "alpha"));
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> applier.applyBatchResults(
                List.of(hit),
                List.of(List.of(), List.of()),
                List.of("body"),
                List.of("<em>"),
                List.of("</em>"),
                List.of(0),
                List.of("default")
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("Batch results size"));
    }

    public void testApplyWithIndicesHandlesSlice() {
        SearchHit hitA = hitWithSource(Map.of("body", "alpha beta gamma"));
        SearchHit hitB = hitWithSource(Map.of("body", "alpha beta gamma"));
        applier.applyBatchResultsWithIndices(
            List.of(hitA, hitB),
            List.of(List.of(Map.of("start", 6, "end", 10))),
            1,
            2,
            List.of("body", "body"),
            List.of("<em>", "<em>"),
            List.of("</em>", "</em>"),
            List.of(0, 0),
            List.of("default", "default")
        );
        assertNull(hitA.getHighlightFields().get("body"));
        assertEquals("alpha <em>beta</em> gamma", highlightedValue(hitB, "body"));
    }

    public void testNullSourceIsSkipped() {
        // Hit with no source — applier returns early without throwing
        SearchHit hit = new SearchHit(0, "_id", new HashMap<>(), new HashMap<>());
        applier.applyBatchResults(
            List.of(hit),
            List.of(List.of(Map.of("start", 0, "end", 5))),
            List.of("body"),
            List.of("<em>"),
            List.of("</em>"),
            List.of(0),
            List.of("default")
        );
        assertTrue(hit.getHighlightFields().isEmpty());
    }

    public void testEmptySourceFieldIsSkipped() {
        // Hit has source but the requested field is missing entirely
        SearchHit hit = hitWithSource(Map.of("title", "no body field here"));
        applier.applyBatchResults(
            List.of(hit),
            List.of(List.of(Map.of("start", 0, "end", 5))),
            List.of("body"),
            List.of("<em>"),
            List.of("</em>"),
            List.of(0),
            List.of("default")
        );
        assertTrue(hit.getHighlightFields().isEmpty());
    }

    public void testHtmlEncoderWithNoMatchSizeEscapesSnippet() {
        SearchHit hit = hitWithSource(Map.of("body", "<b>bold</b> text"));
        applier.applyBatchResults(
            List.of(hit),
            List.of(List.of()),
            List.of("body"),
            List.of("<em>"),
            List.of("</em>"),
            List.of(6),
            List.of("html")
        );
        // first 6 chars of "<b>bold</b> text" is "<b>bol" which becomes "&lt;b&gt;bol"
        String value = highlightedValue(hit, "body");
        assertTrue(value, value.contains("&lt;"));
        assertTrue(value, value.contains("&gt;"));
    }

    public void testApplyBatchResultsPreservesExistingHighlightFields() {
        // Hit already has another highlight; applying should add ours without losing existing
        SearchHit hit = hitWithSource(Map.of("body", "alpha beta"));
        Map<String, HighlightField> existing = new HashMap<>();
        existing.put(
            "title",
            new HighlightField(
                "title",
                new org.opensearch.core.common.text.Text[] { new org.opensearch.core.common.text.Text("<em>title</em>") }
            )
        );
        hit.highlightFields(existing);

        applier.applyBatchResults(
            List.of(hit),
            List.of(List.of(Map.of("start", 0, "end", 5))),
            List.of("body"),
            List.of("<em>"),
            List.of("</em>"),
            List.of(0),
            List.of("default")
        );
        assertNotNull(hit.getHighlightFields().get("title"));
        assertEquals("<em>title</em>", hit.getHighlightFields().get("title").fragments()[0].string());
        assertEquals("<em>alpha</em> beta", highlightedValue(hit, "body"));
    }

    public void testNullHighlightsListIsSkipped() {
        // batchResults entry is null instead of empty list
        SearchHit hit = hitWithSource(Map.of("body", "alpha"));
        java.util.ArrayList<List<Map<String, Object>>> results = new java.util.ArrayList<>();
        results.add(null);
        applier.applyBatchResults(
            List.of(hit),
            results,
            List.of("body"),
            List.of("<em>"),
            List.of("</em>"),
            List.of(0),
            List.of("default")
        );
        // Null highlights → no spans → no_match_size=0 → no field written
        assertTrue(hit.getHighlightFields().isEmpty());
    }

    public void testInvalidSpanRangesThrowException() {
        SearchHit hit = hitWithSource(Map.of("body", "alpha beta"));
        expectThrows(
            Exception.class,
            () -> applier.applyBatchResults(
                List.of(hit),
                List.of(List.of(Map.of("start", 5, "end", 5))),
                List.of("body"),
                List.of("<em>"),
                List.of("</em>"),
                List.of(0),
                List.of("default")
            )
        );
    }

    public void testApplyWithIndicesSliceSizeMismatchThrows() {
        SearchHit a = hitWithSource(Map.of("body", "alpha"));
        SearchHit b = hitWithSource(Map.of("body", "beta"));
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> applier.applyBatchResultsWithIndices(
                List.of(a, b),
                List.of(List.of()),  // 1 result for slice of size 2
                0,
                2,
                List.of("body", "body"),
                List.of("<em>", "<em>"),
                List.of("</em>", "</em>"),
                List.of(0, 0),
                List.of("default", "default")
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("Batch results size"));
    }

    private static SearchHit hitWithSource(Map<String, Object> source) {
        SearchHit hit = new SearchHit(0, "_id", new HashMap<>(), new HashMap<>());
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            sb.append('"').append(entry.getKey()).append('"').append(':').append('"').append(entry.getValue()).append('"');
        }
        sb.append('}');
        BytesReference src = new BytesArray(sb.toString());
        hit.sourceRef(src);
        return hit;
    }

    private static String highlightedValue(SearchHit hit, String fieldName) {
        HighlightField field = hit.getHighlightFields().get(fieldName);
        assertNotNull("expected highlight field [" + fieldName + "]", field);
        assertEquals(1, field.fragments().length);
        return field.fragments()[0].string();
    }

    /** placeholder to silence unused-import for TotalHits if pulled in elsewhere */
    @SuppressWarnings("unused")
    private static final TotalHits UNUSED = new TotalHits(0, TotalHits.Relation.EQUAL_TO);
}
