/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper.semanticfieldtypes;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.index.mapper.BinaryFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.neuralsearch.mapper.SemanticFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.mapper.SemanticFieldMapperTestUtil.buildSemanticFieldMapper;

public class SemanticFieldTypeTests extends OpenSearchTestCase {
    @Mock
    private Mapper.TypeParser.ParserContext parserContext;
    @Mock
    private MappedFieldType mockDelegateFieldType;
    private SemanticFieldType semanticFieldType;

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(
            BinaryFieldMapper.CONTENT_TYPE,
            BinaryFieldMapper.PARSER,
            parserContext
        );
        final MappedFieldType mappedFieldType = semanticFieldMapper.getDelegateFieldMapper().fieldType();

        when(mockDelegateFieldType.name()).thenReturn(mappedFieldType.name());
        when(mockDelegateFieldType.isSearchable()).thenReturn(mappedFieldType.isSearchable());
        when(mockDelegateFieldType.isStored()).thenReturn(mappedFieldType.isStored());
        when(mockDelegateFieldType.hasDocValues()).thenReturn(mappedFieldType.hasDocValues());
        when(mockDelegateFieldType.getTextSearchInfo()).thenReturn(mappedFieldType.getTextSearchInfo());
        when(mockDelegateFieldType.meta()).thenReturn(mappedFieldType.meta());

        semanticFieldType = new SemanticFieldType(mockDelegateFieldType, semanticFieldMapper.getMergeBuilder().getSemanticParameters());
    }

    public void testSemanticFieldType_shouldDelegateWork() throws IOException {
        assertEquals(SemanticFieldMapper.CONTENT_TYPE, semanticFieldType.typeName());

        semanticFieldType.valueFetcher(any(), any(), any());
        verify(mockDelegateFieldType, times(1)).valueFetcher(any(), any(), any());

        semanticFieldType.termQuery(any(), any());
        verify(mockDelegateFieldType, times(1)).termQuery(any(), any());

        semanticFieldType.docValueFormat(any(), any());
        verify(mockDelegateFieldType, times(1)).docValueFormat(any(), any());

        semanticFieldType.valueForDisplay(any());
        verify(mockDelegateFieldType, times(1)).valueForDisplay(any());

        semanticFieldType.fielddataBuilder(any(), any());
        verify(mockDelegateFieldType, times(1)).fielddataBuilder(any(), any());
    }
}
