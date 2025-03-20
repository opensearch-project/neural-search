/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper;

import lombok.NonNull;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.BinaryFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MatchOnlyTextFieldMapper;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.mapper.WildcardFieldMapper;
import org.opensearch.neuralsearch.mapper.semanticfieldtypes.SemanticFieldTypeFactory;
import org.opensearch.neuralsearch.mapper.semanticfieldtypes.SemanticStringFieldType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.constants.MappingConstants.TYPE;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.MODEL_ID;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.RAW_FIELD_TYPE;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SEARCH_MODEL_ID;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SEMANTIC_INFO_FIELD_NAME;
import static org.opensearch.neuralsearch.mapper.SemanticFieldMapperTestUtil.buildSemanticFieldMapperWithTextAsRawFieldType;
import static org.opensearch.neuralsearch.mapper.SemanticFieldMapperTestUtil.mockParserContext;
import static org.opensearch.neuralsearch.mapper.SemanticFieldMapperTestUtil.TYPE_PARSER;
import static org.opensearch.neuralsearch.util.TestUtils.xContentBuilderToMap;

public class SemanticFieldMapperTests extends OpenSearchTestCase {

    @Mock
    private ParametrizedFieldMapper.TypeParser.ParserContext parserContext;
    @Mock
    private ParseContext parseContext;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    public void testTypeParser_parse_whenTextRawFieldType_thenDelegateTextFieldMapper() {
        final Map<String, Object> node = createFieldConfig(TextFieldMapper.CONTENT_TYPE);

        mockParserContext(TextFieldMapper.CONTENT_TYPE, TextFieldMapper.PARSER, parserContext);

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(SemanticFieldMapperTestUtil.fieldName, node, parserContext);

        assertBuilderParseParametersSuccessfully(TextFieldMapper.CONTENT_TYPE, builder);
        assertTrue(builder.delegateBuilder instanceof TextFieldMapper.Builder);
    }

    public void testTypeParser_parse_whenMatchOnlyTextRawFieldType_thenDelegateTextOnlyFieldMapper() {
        final Map<String, Object> node = createFieldConfig(MatchOnlyTextFieldMapper.CONTENT_TYPE);

        mockParserContext(MatchOnlyTextFieldMapper.CONTENT_TYPE, MatchOnlyTextFieldMapper.PARSER, parserContext);

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(SemanticFieldMapperTestUtil.fieldName, node, parserContext);

        assertBuilderParseParametersSuccessfully(MatchOnlyTextFieldMapper.CONTENT_TYPE, builder);
        assertTrue(builder.delegateBuilder instanceof MatchOnlyTextFieldMapper.Builder);
    }

    public void testTypeParser_parse_whenKeywordRawFieldType_thenDelegateKeywordFieldMapper() {
        final Map<String, Object> node = createFieldConfig(KeywordFieldMapper.CONTENT_TYPE);

        mockParserContext(KeywordFieldMapper.CONTENT_TYPE, KeywordFieldMapper.PARSER, parserContext);

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(SemanticFieldMapperTestUtil.fieldName, node, parserContext);

        assertBuilderParseParametersSuccessfully(KeywordFieldMapper.CONTENT_TYPE, builder);
        assertTrue(builder.delegateBuilder instanceof KeywordFieldMapper.Builder);
    }

    public void testTypeParser_parse_whenWildcardRawFieldType_thenDelegateWildcardFieldMapper() {
        final Map<String, Object> node = createFieldConfig(WildcardFieldMapper.CONTENT_TYPE);

        mockParserContext(WildcardFieldMapper.CONTENT_TYPE, WildcardFieldMapper.PARSER, parserContext);

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(SemanticFieldMapperTestUtil.fieldName, node, parserContext);

        assertBuilderParseParametersSuccessfully(WildcardFieldMapper.CONTENT_TYPE, builder);
        assertTrue(builder.delegateBuilder instanceof WildcardFieldMapper.Builder);
    }

    public void testTypeParser_parse_whenBinaryRawFieldType_thenDelegateBinaryFieldMapper() {
        final Map<String, Object> node = createFieldConfig(BinaryFieldMapper.CONTENT_TYPE);

        mockParserContext(BinaryFieldMapper.CONTENT_TYPE, BinaryFieldMapper.PARSER, parserContext);

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(SemanticFieldMapperTestUtil.fieldName, node, parserContext);

        assertBuilderParseParametersSuccessfully(BinaryFieldMapper.CONTENT_TYPE, builder);
        assertTrue(builder.delegateBuilder instanceof BinaryFieldMapper.Builder);
    }

    public void testTypeParser_parse_whenNoRawFieldType_thenDelegateTextFieldMapper() {
        final Map<String, Object> node = createFieldConfig(null);
        node.remove(RAW_FIELD_TYPE);

        mockParserContext(TextFieldMapper.CONTENT_TYPE, TextFieldMapper.PARSER, parserContext);

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(SemanticFieldMapperTestUtil.fieldName, node, parserContext);

        assertBuilderParseParametersSuccessfully(TextFieldMapper.CONTENT_TYPE, builder);
        assertTrue(builder.delegateBuilder instanceof TextFieldMapper.Builder);
    }

    public void testTypeParser_parse_whenNullRawFieldType_thenException() {
        final Map<String, Object> node = createFieldConfig(null);
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> TYPE_PARSER.parse(SemanticFieldMapperTestUtil.fieldName, node, parserContext)
        );
        final String expectedError = "raw_field_type: [null] is not supported. It should be one of";
        assertTrue(exception.getMessage().contains(expectedError));
    }

    public void testTypeParser_parse_whenUnsupportedRawFieldType_thenException() {
        final Map<String, Object> node = createFieldConfig("unsupported");
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> TYPE_PARSER.parse(SemanticFieldMapperTestUtil.fieldName, node, parserContext)
        );
        final String expectedError = "raw_field_type: [unsupported] is not supported. It should be one of";
        assertTrue(exception.getMessage().contains(expectedError));
    }

    private void assertBuilderParseParametersSuccessfully(
        @NonNull final String rawFieldType,
        @NonNull final SemanticFieldMapper.Builder builder
    ) {
        assertEquals(rawFieldType, builder.rawFieldType.getValue());
        assertEquals(SemanticFieldMapperTestUtil.modelId, builder.modelId.getValue());
        assertEquals(SemanticFieldMapperTestUtil.searchModelId, builder.searchModelId.getValue());
        assertEquals(SemanticFieldMapperTestUtil.semanticInfoFieldName, builder.semanticInfoFieldName.getValue());
    }

    private Map<String, Object> createFieldConfig(final String rawFieldType) {
        Map<String, Object> node = new HashMap<>();
        node.put(TYPE, SemanticFieldMapper.CONTENT_TYPE);
        node.put(RAW_FIELD_TYPE, rawFieldType);
        node.put(MODEL_ID, SemanticFieldMapperTestUtil.modelId);
        node.put(SEARCH_MODEL_ID, SemanticFieldMapperTestUtil.searchModelId);
        node.put(SEMANTIC_INFO_FIELD_NAME, SemanticFieldMapperTestUtil.semanticInfoFieldName);
        return node;
    }

    public void testBuilder_getParameters() {
        final SemanticFieldMapper.Builder builder = new SemanticFieldMapper.Builder(
            SemanticFieldMapperTestUtil.fieldName,
            SemanticFieldTypeFactory.getInstance()
        );
        assertEquals(4, builder.getParameters().size());
        List<String> actualParams = builder.getParameters().stream().map(a -> a.name).collect(Collectors.toList());
        List<String> expectedParams = Arrays.asList(MODEL_ID, SEARCH_MODEL_ID, RAW_FIELD_TYPE, SEMANTIC_INFO_FIELD_NAME);
        assertEquals(expectedParams, actualParams);
    }

    public void testBuilder_build_shouldBuildDelegateFieldMapper() {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapperWithTextAsRawFieldType(parserContext);

        assertTrue(semanticFieldMapper.getDelegateFieldMapper() instanceof TextFieldMapper);
        assertTrue(semanticFieldMapper.fieldType() instanceof SemanticStringFieldType);
    }

    public void testFieldMapper_getMergeBuilder_shouldSetDelegateMergeBuilder() {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapperWithTextAsRawFieldType(parserContext);
        final SemanticFieldMapper.Builder mergeBuilder = semanticFieldMapper.getMergeBuilder();
        assertTrue(mergeBuilder.delegateBuilder instanceof TextFieldMapper.Builder);
    }

    public void testFieldMapper_merge_shouldMergeDelegateFieldMapper() {
        final String newModelId = "newModelId";
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapperWithTextAsRawFieldType(parserContext);

        final Map<String, Object> updatedConfig = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        updatedConfig.put(MODEL_ID, newModelId);
        final SemanticFieldMapper semanticFieldMapperToMerge = buildSemanticFieldMapperWithTextAsRawFieldType(updatedConfig, parserContext);

        final SemanticFieldMapper mergedSemanticFieldMapper = (SemanticFieldMapper) semanticFieldMapper.merge(semanticFieldMapperToMerge);

        assertEquals(newModelId, mergedSemanticFieldMapper.getMergeBuilder().modelId.getValue());
        // A new delegate field mapper should be created after the merge and it's still a text field mapper
        assertNotEquals(semanticFieldMapper.getDelegateFieldMapper(), mergedSemanticFieldMapper.getDelegateFieldMapper());
        assertTrue(mergedSemanticFieldMapper.getDelegateFieldMapper() instanceof TextFieldMapper);
    }

    public void testFieldMapper_merge_whenConflictWithSemanticParameters_thenException() {
        final String newSemanticInfoFieldName = "newSemanticInfoFieldName";
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapperWithTextAsRawFieldType(parserContext);

        final Map<String, Object> updatedConfig = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        updatedConfig.put(SEMANTIC_INFO_FIELD_NAME, newSemanticInfoFieldName);
        when(parserContext.typeParser(KeywordFieldMapper.CONTENT_TYPE)).thenReturn(KeywordFieldMapper.PARSER);
        final SemanticFieldMapper semanticFieldMapperToMerge = buildSemanticFieldMapperWithTextAsRawFieldType(updatedConfig, parserContext);

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> semanticFieldMapper.merge(semanticFieldMapperToMerge)
        );

        final String expectedError = "Mapper for [testField] conflicts with existing mapper:\n"
            + "\tCannot update parameter [semantic_info_field_name] from [semanticInfoFieldName] to [newSemanticInfoFieldName]";
        assertEquals(expectedError, exception.getMessage());
    }

    public void testFieldMapper_merge_whenConflictWithDelegateMapperParameters_thenException() {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapperWithTextAsRawFieldType(parserContext);

        final Map<String, Object> updatedConfig = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        updatedConfig.put(RAW_FIELD_TYPE, KeywordFieldMapper.CONTENT_TYPE);
        when(parserContext.typeParser(KeywordFieldMapper.CONTENT_TYPE)).thenReturn(KeywordFieldMapper.PARSER);
        final SemanticFieldMapper semanticFieldMapperToMerge = buildSemanticFieldMapperWithTextAsRawFieldType(updatedConfig, parserContext);

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> semanticFieldMapper.merge(semanticFieldMapperToMerge)
        );

        final String expectedError = "Failed to update the mapper [testField] because failed to update the delegate mapper for the "
            + "raw_field_type text. mapper [testField] cannot be changed from type [text] to [keyword]";
        assertEquals(expectedError, exception.getMessage());
    }

    public void testFieldMapper_parseCreateField_shouldDelegateToDelegateMapper() throws IOException {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapperWithTextAsRawFieldType(parserContext);
        ParametrizedFieldMapper delegateMapper = mock(ParametrizedFieldMapper.class);
        semanticFieldMapper.setDelegateFieldMapper(delegateMapper);

        semanticFieldMapper.parseCreateField(parseContext);

        verify(delegateMapper, times(1)).parse(parseContext);
    }

    public void testFieldMapper_contentType_shouldReturnSemantic() {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapperWithTextAsRawFieldType(parserContext);
        assertEquals(SemanticFieldMapper.CONTENT_TYPE, semanticFieldMapper.contentType());
    }

    public void testFieldMapper_doXContentBody_shouldReturnParameters() throws IOException {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapperWithTextAsRawFieldType(parserContext);
        final XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        semanticFieldMapper.doXContentBody(xContentBuilder, false, ToXContent.EMPTY_PARAMS);
        xContentBuilder.endObject();
        final Map<String, Object> out = xContentBuilderToMap(xContentBuilder);

        final Map<String, Object> expected = createFieldConfig(TextFieldMapper.CONTENT_TYPE);

        assertEquals(expected, out);
    }

    public void testFieldMapper_doXContentBody_withDefaultRawFieldType_shouldReturnDefaultValue() throws IOException {
        final Map<String, Object> config = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        config.remove(RAW_FIELD_TYPE);

        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapperWithTextAsRawFieldType(config, parserContext);

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        semanticFieldMapper.doXContentBody(xContentBuilder, false, ToXContent.EMPTY_PARAMS);
        xContentBuilder.endObject();
        Map<String, Object> out = xContentBuilderToMap(xContentBuilder);

        Map<String, Object> expected = createFieldConfig(TextFieldMapper.CONTENT_TYPE);

        assertEquals(expected, out);
    }
}
