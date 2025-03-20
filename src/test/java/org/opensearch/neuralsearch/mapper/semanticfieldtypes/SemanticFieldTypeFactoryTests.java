/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper.semanticfieldtypes;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.index.mapper.BinaryFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MatchOnlyTextFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.mapper.TokenCountFieldMapper;
import org.opensearch.index.mapper.WildcardFieldMapper;
import org.opensearch.neuralsearch.mapper.SemanticFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

import static org.opensearch.neuralsearch.mapper.SemanticFieldMapperTestUtil.buildSemanticFieldMapper;
import static org.opensearch.neuralsearch.mapper.SemanticFieldMapperTestUtil.buildSemanticFieldMapperWithTextAsRawFieldType;
import static org.opensearch.neuralsearch.mapper.SemanticFieldMapperTestUtil.createFieldConfig;

public class SemanticFieldTypeFactoryTests extends OpenSearchTestCase {
    private final SemanticFieldTypeFactory semanticFieldTypeFactory = SemanticFieldTypeFactory.getInstance();
    @Mock
    private Mapper.TypeParser.ParserContext parserContext;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    public void testCreateSemanticFieldType_whenBinaryAsRawFieldType_thenSemanticFieldType() {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(
            BinaryFieldMapper.CONTENT_TYPE,
            BinaryFieldMapper.PARSER,
            parserContext
        );

        final MappedFieldType fieldType = semanticFieldTypeFactory.createSemanticFieldType(
            semanticFieldMapper,
            BinaryFieldMapper.CONTENT_TYPE,
            semanticFieldMapper.getMergeBuilder().getSemanticParameters()
        );

        assertTrue(fieldType instanceof SemanticFieldType);
    }

    public void testCreateSemanticFieldType_whenTextAsRawFieldType_thenSemanticStringFieldType() {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapperWithTextAsRawFieldType(parserContext);

        final MappedFieldType fieldType = semanticFieldTypeFactory.createSemanticFieldType(
            semanticFieldMapper,
            TextFieldMapper.CONTENT_TYPE,
            semanticFieldMapper.getMergeBuilder().getSemanticParameters()
        );

        assertTrue(fieldType instanceof SemanticStringFieldType);
    }

    public void testCreateSemanticFieldType_whenMatchOnlyTextAsRawFieldType_thenSemanticStringFieldType() {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(
            MatchOnlyTextFieldMapper.CONTENT_TYPE,
            MatchOnlyTextFieldMapper.PARSER,
            parserContext
        );

        final MappedFieldType fieldType = semanticFieldTypeFactory.createSemanticFieldType(
            semanticFieldMapper,
            MatchOnlyTextFieldMapper.CONTENT_TYPE,
            semanticFieldMapper.getMergeBuilder().getSemanticParameters()
        );

        assertTrue(fieldType instanceof SemanticStringFieldType);
    }

    public void testCreateSemanticFieldType_whenKeywordAsRawFieldType_thenSemanticStringFieldType() {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(
            KeywordFieldMapper.CONTENT_TYPE,
            KeywordFieldMapper.PARSER,
            parserContext
        );

        final MappedFieldType fieldType = semanticFieldTypeFactory.createSemanticFieldType(
            semanticFieldMapper,
            KeywordFieldMapper.CONTENT_TYPE,
            semanticFieldMapper.getMergeBuilder().getSemanticParameters()
        );

        assertTrue(fieldType instanceof SemanticStringFieldType);
    }

    public void testCreateSemanticFieldType_whenWildcardAsRawFieldType_thenSemanticStringFieldType() {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(
            WildcardFieldMapper.CONTENT_TYPE,
            WildcardFieldMapper.PARSER,
            parserContext
        );

        final MappedFieldType fieldType = semanticFieldTypeFactory.createSemanticFieldType(
            semanticFieldMapper,
            WildcardFieldMapper.CONTENT_TYPE,
            semanticFieldMapper.getMergeBuilder().getSemanticParameters()
        );

        assertTrue(fieldType instanceof SemanticStringFieldType);
    }

    public void testCreateSemanticFieldType_whenTokenCountAsRawFieldType_thenSemanticNumberFieldType() {
        final Map<String, Object> fieldConfig = createFieldConfig(TokenCountFieldMapper.CONTENT_TYPE);
        fieldConfig.put("analyzer", "default");
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(
            fieldConfig,
            TokenCountFieldMapper.CONTENT_TYPE,
            TokenCountFieldMapper.PARSER,
            parserContext
        );

        final MappedFieldType fieldType = semanticFieldTypeFactory.createSemanticFieldType(
            semanticFieldMapper,
            TokenCountFieldMapper.CONTENT_TYPE,
            semanticFieldMapper.getMergeBuilder().getSemanticParameters()
        );

        assertTrue(fieldType instanceof SemanticNumberFieldType);
    }

    public void testCreateSemanticFieldType_whenUnsupportedRawFieldType_thenException() {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapperWithTextAsRawFieldType(parserContext);

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> semanticFieldTypeFactory.createSemanticFieldType(
                semanticFieldMapper,
                "unsupported",
                semanticFieldMapper.getMergeBuilder().getSemanticParameters()
            )
        );

        final String expectedErrorMessage = "Failed to create delegate field type for semantic field [testField]. "
            + "Unsupported raw field type: unsupported";
        assertEquals(expectedErrorMessage, exception.getMessage());
    }
}
