/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper.semanticfieldtypes;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.TokenCountFieldMapper;
import org.opensearch.neuralsearch.mapper.SemanticFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.neuralsearch.mapper.SemanticFieldMapperTestUtil.buildSemanticFieldMapper;
import static org.opensearch.neuralsearch.mapper.SemanticFieldMapperTestUtil.createFieldConfig;

public class SemanticNumberFieldTypeTests extends OpenSearchTestCase {
    @Mock
    private Mapper.TypeParser.ParserContext parserContext;
    @Mock
    private NumberFieldMapper.NumberFieldType mockDelegateFieldType;
    private SemanticNumberFieldType semanticNumberFieldType;

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        final Map<String, Object> fieldConfig = createFieldConfig(TokenCountFieldMapper.CONTENT_TYPE);
        fieldConfig.put("analyzer", "default");
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(
            fieldConfig,
            TokenCountFieldMapper.CONTENT_TYPE,
            TokenCountFieldMapper.PARSER,
            parserContext
        );

        final NumberFieldMapper.NumberFieldType numberFieldType = (NumberFieldMapper.NumberFieldType) semanticFieldMapper
            .getDelegateFieldMapper()
            .fieldType();

        Mockito.when(mockDelegateFieldType.name()).thenReturn(numberFieldType.name());
        Mockito.when(mockDelegateFieldType.numberType()).thenReturn(numberFieldType.numberType());
        Mockito.when(mockDelegateFieldType.isSearchable()).thenReturn(numberFieldType.isSearchable());
        Mockito.when(mockDelegateFieldType.isStored()).thenReturn(numberFieldType.isStored());
        Mockito.when(mockDelegateFieldType.hasDocValues()).thenReturn(numberFieldType.hasDocValues());
        Mockito.when(mockDelegateFieldType.coerce()).thenReturn(numberFieldType.coerce());
        Mockito.when(mockDelegateFieldType.nullValue()).thenReturn(numberFieldType.nullValue());
        Mockito.when(mockDelegateFieldType.meta()).thenReturn(numberFieldType.meta());

        semanticNumberFieldType = new SemanticNumberFieldType(
            mockDelegateFieldType,
            semanticFieldMapper.getMergeBuilder().getSemanticParameters()
        );
    }

    public void testSemanticNumberFieldType_shouldDelegateWork() {
        assertEquals(SemanticFieldMapper.CONTENT_TYPE, semanticNumberFieldType.typeName());

        semanticNumberFieldType.valueFetcher(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verify(mockDelegateFieldType, Mockito.times(1)).valueFetcher(Mockito.any(), Mockito.any(), Mockito.any());
    }
}
