/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper.semanticfieldtypes;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.StringFieldType;
import org.opensearch.neuralsearch.mapper.SemanticFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.mapper.SemanticFieldMapperTestUtil.buildSemanticFieldMapperWithTextAsRawFieldType;

public class SemanticStringFieldTypeTests extends OpenSearchTestCase {
    @Mock
    private Mapper.TypeParser.ParserContext parserContext;
    @Mock
    private StringFieldType mockDelegateFieldType;
    private SemanticStringFieldType semanticStringFieldType;

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapperWithTextAsRawFieldType(parserContext);
        final StringFieldType stringFieldType = (StringFieldType) semanticFieldMapper.getDelegateFieldMapper().fieldType();

        when(mockDelegateFieldType.name()).thenReturn(stringFieldType.name());
        when(mockDelegateFieldType.isSearchable()).thenReturn(stringFieldType.isSearchable());
        when(mockDelegateFieldType.isStored()).thenReturn(stringFieldType.isStored());
        when(mockDelegateFieldType.hasDocValues()).thenReturn(stringFieldType.hasDocValues());
        when(mockDelegateFieldType.getTextSearchInfo()).thenReturn(stringFieldType.getTextSearchInfo());
        when(mockDelegateFieldType.meta()).thenReturn(stringFieldType.meta());

        semanticStringFieldType = new SemanticStringFieldType(
            mockDelegateFieldType,
            semanticFieldMapper.getMergeBuilder().getSemanticParameters()
        );
    }

    public void testSemanticStringFieldType_shouldDelegateWork() throws IOException {
        assertEquals(SemanticFieldMapper.CONTENT_TYPE, semanticStringFieldType.typeName());

        semanticStringFieldType.valueFetcher(any(), any(), any());
        verify(mockDelegateFieldType, times(1)).valueFetcher(any(), any(), any());

        semanticStringFieldType.prefixQuery(any(), any(), anyBoolean(), any());
        verify(mockDelegateFieldType, times(1)).prefixQuery(any(), any(), anyBoolean(), any());

        semanticStringFieldType.spanPrefixQuery(any(), any(), any());
        verify(mockDelegateFieldType, times(1)).spanPrefixQuery(any(), any(), any());

        semanticStringFieldType.intervals(any(), anyInt(), any(), any(), anyBoolean());
        verify(mockDelegateFieldType, times(1)).intervals(any(), anyInt(), any(), any(), anyBoolean());

        semanticStringFieldType.phraseQuery(any(), anyInt(), anyBoolean());
        verify(mockDelegateFieldType, times(1)).phraseQuery(any(), anyInt(), anyBoolean());

        semanticStringFieldType.multiPhraseQuery(any(), anyInt(), anyBoolean());
        verify(mockDelegateFieldType, times(1)).multiPhraseQuery(any(), anyInt(), anyBoolean());

        semanticStringFieldType.phrasePrefixQuery(any(), anyInt(), anyInt());
        verify(mockDelegateFieldType, times(1)).phrasePrefixQuery(any(), anyInt(), anyInt());

        semanticStringFieldType.fielddataBuilder(any(), any());
        verify(mockDelegateFieldType, times(1)).fielddataBuilder(any(), any());

        semanticStringFieldType.phraseQuery(any(), anyInt(), anyBoolean(), any());
        verify(mockDelegateFieldType, times(1)).phraseQuery(any(), anyInt(), anyBoolean(), any());

        semanticStringFieldType.multiPhraseQuery(any(), anyInt(), anyBoolean(), any());
        verify(mockDelegateFieldType, times(1)).multiPhraseQuery(any(), anyInt(), anyBoolean(), any());

        semanticStringFieldType.phrasePrefixQuery(any(), anyInt(), anyInt(), any());
        verify(mockDelegateFieldType, times(1)).phrasePrefixQuery(any(), anyInt(), anyInt(), any());

        semanticStringFieldType.termQuery(any(), any());
        verify(mockDelegateFieldType, times(1)).termQuery(any(), any());

        semanticStringFieldType.termQueryCaseInsensitive(any(), any());
        verify(mockDelegateFieldType, times(1)).termQuery(any(), any());

        semanticStringFieldType.fuzzyQuery(any(), any(), anyInt(), anyInt(), anyBoolean(), any(), any());
        verify(mockDelegateFieldType, times(1)).fuzzyQuery(any(), any(), anyInt(), anyInt(), anyBoolean(), any(), any());

        semanticStringFieldType.wildcardQuery(any(), any(), anyBoolean(), any());
        verify(mockDelegateFieldType, times(1)).wildcardQuery(any(), any(), anyBoolean(), any());

        semanticStringFieldType.regexpQuery(any(), anyInt(), anyInt(), anyInt(), any(), any());
        verify(mockDelegateFieldType, times(1)).regexpQuery(any(), anyInt(), anyInt(), anyInt(), any(), any());

        semanticStringFieldType.rangeQuery(any(), any(), anyBoolean(), anyBoolean(), any());
        verify(mockDelegateFieldType, times(1)).rangeQuery(any(), any(), anyBoolean(), anyBoolean(), any());

        semanticStringFieldType.termsQuery(any(), any());
        verify(mockDelegateFieldType, times(1)).termsQuery(any(), any());

        semanticStringFieldType.valueForDisplay(any());
        verify(mockDelegateFieldType, times(1)).valueForDisplay(any());

        semanticStringFieldType.indexedValueForSearch(any());
        verify(mockDelegateFieldType, times(1)).indexedValueForSearch(any());
    }
}
