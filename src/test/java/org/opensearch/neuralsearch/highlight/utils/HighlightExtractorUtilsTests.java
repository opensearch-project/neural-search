/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.utils;

import org.apache.lucene.search.Query;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HighlightExtractorUtilsTests extends OpenSearchTestCase {

    private FieldHighlightContext createFieldContext(String fieldName, FetchSubPhase.HitContext hitContext) {
        return new FieldHighlightContext(
            fieldName,
            mock(SearchHighlightContext.Field.class),
            mock(MappedFieldType.class),
            mock(FetchContext.class),
            hitContext,
            mock(Query.class),
            false,
            new HashMap<>()
        );
    }

    public void testGetFieldTextReturnsNullWhenHitContextIsNull() {
        FieldHighlightContext fieldContext = createFieldContext("test_field", null);
        assertNull(HighlightExtractorUtils.getFieldText(fieldContext));
    }

    public void testGetFieldTextReturnsNullWhenSourceLookupIsNull() {
        FetchSubPhase.HitContext hitContext = mock(FetchSubPhase.HitContext.class);
        when(hitContext.sourceLookup()).thenReturn(null);
        FieldHighlightContext fieldContext = createFieldContext("test_field", hitContext);
        assertNull(HighlightExtractorUtils.getFieldText(fieldContext));
    }

    public void testGetFieldTextReturnsNullWhenFieldMissing() {
        FetchSubPhase.HitContext hitContext = mock(FetchSubPhase.HitContext.class);
        SourceLookup sourceLookup = mock(SourceLookup.class);
        when(hitContext.sourceLookup()).thenReturn(sourceLookup);
        when(sourceLookup.extractValue("test_field", null)).thenReturn(null);

        FieldHighlightContext fieldContext = createFieldContext("test_field", hitContext);
        assertNull(HighlightExtractorUtils.getFieldText(fieldContext));
    }

    public void testGetFieldTextReturnsNullWhenFieldNotString() {
        FetchSubPhase.HitContext hitContext = mock(FetchSubPhase.HitContext.class);
        SourceLookup sourceLookup = mock(SourceLookup.class);
        when(hitContext.sourceLookup()).thenReturn(sourceLookup);
        when(sourceLookup.extractValue("test_field", null)).thenReturn(42);

        FieldHighlightContext fieldContext = createFieldContext("test_field", hitContext);
        assertNull(HighlightExtractorUtils.getFieldText(fieldContext));
    }

    public void testGetFieldTextReturnsNullWhenFieldEmpty() {
        FetchSubPhase.HitContext hitContext = mock(FetchSubPhase.HitContext.class);
        SourceLookup sourceLookup = mock(SourceLookup.class);
        when(hitContext.sourceLookup()).thenReturn(sourceLookup);
        when(sourceLookup.extractValue("test_field", null)).thenReturn("");

        FieldHighlightContext fieldContext = createFieldContext("test_field", hitContext);
        assertNull(HighlightExtractorUtils.getFieldText(fieldContext));
    }

    public void testGetFieldTextReturnsTextWhenFieldExists() {
        FetchSubPhase.HitContext hitContext = mock(FetchSubPhase.HitContext.class);
        SourceLookup sourceLookup = mock(SourceLookup.class);
        when(hitContext.sourceLookup()).thenReturn(sourceLookup);
        when(sourceLookup.extractValue("test_field", null)).thenReturn("some text content");

        FieldHighlightContext fieldContext = createFieldContext("test_field", hitContext);
        assertEquals("some text content", HighlightExtractorUtils.getFieldText(fieldContext));
    }

    // ------------------------- extractNestedPaths -------------------------

    public void testExtractNestedPathsFromList() {
        HighlightBuilder hl = new HighlightBuilder().options(optionsWith("nested_paths", java.util.Arrays.asList("chunks", "reviews")));
        assertEquals(java.util.Arrays.asList("chunks", "reviews"), HighlightExtractorUtils.extractNestedPaths(hl));
    }

    public void testExtractNestedPathsFromSingleString() {
        HighlightBuilder hl = new HighlightBuilder().options(optionsWith("nested_paths", "chunks"));
        assertEquals(java.util.Collections.singletonList("chunks"), HighlightExtractorUtils.extractNestedPaths(hl));
    }

    public void testExtractNestedPathsReturnsEmptyWhenMissing() {
        HighlightBuilder hl = new HighlightBuilder().options(new HashMap<>());
        assertTrue(HighlightExtractorUtils.extractNestedPaths(hl).isEmpty());
    }

    public void testExtractNestedPathsReturnsEmptyWhenOptionsNull() {
        HighlightBuilder hl = new HighlightBuilder();
        assertTrue(HighlightExtractorUtils.extractNestedPaths(hl).isEmpty());
    }

    public void testExtractNestedPathsSkipsNonStringEntries() {
        HighlightBuilder hl = new HighlightBuilder().options(
            optionsWith("nested_paths", java.util.Arrays.asList("chunks", 42, null, "reviews"))
        );
        assertEquals(java.util.Arrays.asList("chunks", "reviews"), HighlightExtractorUtils.extractNestedPaths(hl));
    }

    public void testExtractNestedPathsWithInvalidTypeReturnsEmpty() {
        HighlightBuilder hl = new HighlightBuilder().options(optionsWith("nested_paths", 42));
        assertTrue(HighlightExtractorUtils.extractNestedPaths(hl).isEmpty());
    }

    public void testExtractNestedPathsEmptyStringIsIgnored() {
        HighlightBuilder hl = new HighlightBuilder().options(optionsWith("nested_paths", ""));
        assertTrue(HighlightExtractorUtils.extractNestedPaths(hl).isEmpty());
    }

    // ------------------------- extractInnerHitsNames -------------------------

    public void testExtractInnerHitsNamesFromList() {
        HighlightBuilder hl = new HighlightBuilder().options(optionsWith("inner_hits_names", java.util.Arrays.asList("a", "b")));
        assertEquals(java.util.Arrays.asList("a", "b"), HighlightExtractorUtils.extractInnerHitsNames(hl));
    }

    public void testExtractInnerHitsNamesReturnsEmptyWhenMissing() {
        HighlightBuilder hl = new HighlightBuilder().options(new HashMap<>());
        assertTrue(HighlightExtractorUtils.extractInnerHitsNames(hl).isEmpty());
    }

    // ------------------------- matchNestedPathIndex -------------------------

    public void testMatchNestedPathIndexReturnsCorrectIndex() {
        java.util.List<String> paths = java.util.Arrays.asList("reviews", "qa");
        assertEquals(0, HighlightExtractorUtils.matchNestedPathIndex("reviews.text", paths));
        assertEquals(1, HighlightExtractorUtils.matchNestedPathIndex("qa.answer", paths));
    }

    public void testMatchNestedPathIndexReturnsMinusOneForTopLevelField() {
        assertEquals(-1, HighlightExtractorUtils.matchNestedPathIndex("title", java.util.Collections.singletonList("chunks")));
    }

    public void testMatchNestedPathIndexReturnsMinusOneForNullInputs() {
        assertEquals(-1, HighlightExtractorUtils.matchNestedPathIndex(null, java.util.Collections.singletonList("chunks")));
        assertEquals(-1, HighlightExtractorUtils.matchNestedPathIndex("chunks.text", null));
        assertEquals(-1, HighlightExtractorUtils.matchNestedPathIndex("chunks.text", java.util.Collections.emptyList()));
    }

    public void testMatchNestedPathIndexPrefersLongestMatch() {
        // Even though "chunks" technically matches, the deeper declaration should win
        java.util.List<String> paths = java.util.Arrays.asList("chunks", "chunks.sub");
        assertEquals(1, HighlightExtractorUtils.matchNestedPathIndex("chunks.sub.text", paths));
    }

    public void testMatchNestedPathSkipsEmptyPaths() {
        java.util.List<String> paths = java.util.Arrays.asList("", "chunks");
        assertEquals(1, HighlightExtractorUtils.matchNestedPathIndex("chunks.text", paths));
    }

    public void testMatchNestedPathDelegates() {
        java.util.List<String> paths = java.util.Arrays.asList("reviews", "qa");
        assertEquals("qa", HighlightExtractorUtils.matchNestedPath("qa.answer", paths));
        assertNull(HighlightExtractorUtils.matchNestedPath("title", paths));
    }

    // ------------------------- stripNestedPrefix -------------------------

    public void testStripNestedPrefixRemovesPrefix() {
        assertEquals("text", HighlightExtractorUtils.stripNestedPrefix("chunks.text", "chunks"));
    }

    public void testStripNestedPrefixReturnsAsIsWhenPrefixAbsent() {
        assertEquals("text", HighlightExtractorUtils.stripNestedPrefix("text", "chunks"));
    }

    public void testStripNestedPrefixReturnsAsIsWhenPathNull() {
        assertEquals("chunks.text", HighlightExtractorUtils.stripNestedPrefix("chunks.text", null));
    }

    public void testStripNestedPrefixReturnsAsIsWhenFieldNull() {
        assertNull(HighlightExtractorUtils.stripNestedPrefix(null, "chunks"));
    }

    // ------------------------- helper -------------------------

    private java.util.Map<String, Object> optionsWith(String key, Object value) {
        java.util.Map<String, Object> m = new HashMap<>();
        m.put(key, value);
        return m;
    }
}
