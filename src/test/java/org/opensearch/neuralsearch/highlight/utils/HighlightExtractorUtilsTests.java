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
}
