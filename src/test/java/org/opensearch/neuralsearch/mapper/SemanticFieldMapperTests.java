/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper;

import lombok.NonNull;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;

import org.opensearch.index.mapper.BinaryFieldMapper;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MatchOnlyTextFieldMapper;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.mapper.WildcardFieldMapper;
import org.opensearch.neuralsearch.util.prune.PruneType;
import org.opensearch.neuralsearch.util.prune.PruneUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.Version.CURRENT;
import static org.opensearch.knn.index.KNNSettings.KNN_INDEX;
import static org.opensearch.neuralsearch.constants.MappingConstants.TYPE;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.CHUNKING;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.DENSE_EMBEDDING_CONFIG;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.MODEL_ID;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.RAW_FIELD_TYPE;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SKIP_EXISTING_EMBEDDING;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SEARCH_MODEL_ID;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SEMANTIC_FIELD_SEARCH_ANALYZER;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SEMANTIC_INFO_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SPARSE_ENCODING_CONFIG;
import static org.opensearch.neuralsearch.util.TestUtils.xContentBuilderToMap;

public class SemanticFieldMapperTests extends OpenSearchTestCase {
    private final String fieldName = "testField";
    private final String modelId = "modelId";
    private final String searchModelId = "searchModelId";
    private final String semanticInfoFieldName = "semanticInfoFieldName";
    private final SemanticFieldMapper.TypeParser TYPE_PARSER = new SemanticFieldMapper.TypeParser();

    private MapperService mapperService = mock(MapperService.class);

    @Mock
    private ParseContext parseContext;

    private final Function<String, Mapper.TypeParser> typeParsers = s -> {
        switch (s) {
            case TextFieldMapper.CONTENT_TYPE:
                return TextFieldMapper.PARSER;
            case MatchOnlyTextFieldMapper.CONTENT_TYPE:
                return MatchOnlyTextFieldMapper.PARSER;
            case WildcardFieldMapper.CONTENT_TYPE:
                return WildcardFieldMapper.PARSER;
            case BinaryFieldMapper.CONTENT_TYPE:
                return BinaryFieldMapper.PARSER;
            case KeywordFieldMapper.CONTENT_TYPE:
                return KeywordFieldMapper.PARSER;
        }
        return null;
    };
    private final Mapper.TypeParser.ParserContext parserContext = new Mapper.TypeParser.ParserContext(
        null,
        mapperService,
        typeParsers,
        Version.CURRENT,
        null,
        null,
        null
    );;

    private static final IndexAnalyzers indexAnalyzers = new IndexAnalyzers(
        singletonMap("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
        emptyMap(),
        emptyMap()
    );

    private final Map<String, Object> TEST_DENSE_EMBEDDING_CONFIG = Map.of(
        "method",
        Map.of("engine", "lucene", "parameters", Map.of("ef_construction", 128))
    );

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        when(mapperService.getIndexAnalyzers()).thenReturn(indexAnalyzers);
    }

    public void testTypeParser_parse_whenTextRawFieldType_thenDelegateTextFieldMapper() {
        final Map<String, Object> node = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        node.put(DENSE_EMBEDDING_CONFIG, TEST_DENSE_EMBEDDING_CONFIG);

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(fieldName, node, parserContext);

        assertBuilderParseParametersSuccessfully(TextFieldMapper.CONTENT_TYPE, builder);
        assertTrue(builder.delegateBuilder instanceof TextFieldMapper.Builder);
    }

    public void testTypeParser_parse_whenMatchOnlyTextRawFieldType_thenDelegateTextOnlyFieldMapper() {
        final Map<String, Object> node = createFieldConfig(MatchOnlyTextFieldMapper.CONTENT_TYPE);

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(fieldName, node, parserContext);

        assertBuilderParseParametersSuccessfully(MatchOnlyTextFieldMapper.CONTENT_TYPE, builder);
        assertTrue(builder.delegateBuilder instanceof MatchOnlyTextFieldMapper.Builder);
    }

    public void testTypeParser_parse_whenKeywordRawFieldType_thenDelegateKeywordFieldMapper() {
        final Map<String, Object> node = createFieldConfig(KeywordFieldMapper.CONTENT_TYPE);

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(fieldName, node, parserContext);

        assertBuilderParseParametersSuccessfully(KeywordFieldMapper.CONTENT_TYPE, builder);
        assertTrue(builder.delegateBuilder instanceof KeywordFieldMapper.Builder);
    }

    public void testTypeParser_parse_whenWildcardRawFieldType_thenDelegateWildcardFieldMapper() {
        final Map<String, Object> node = createFieldConfig(WildcardFieldMapper.CONTENT_TYPE);

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(fieldName, node, parserContext);

        assertBuilderParseParametersSuccessfully(WildcardFieldMapper.CONTENT_TYPE, builder);
        assertTrue(builder.delegateBuilder instanceof WildcardFieldMapper.Builder);
    }

    public void testTypeParser_parse_whenBinaryRawFieldType_thenDelegateBinaryFieldMapper() {
        final Map<String, Object> node = createFieldConfig(BinaryFieldMapper.CONTENT_TYPE);

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(fieldName, node, parserContext);

        assertBuilderParseParametersSuccessfully(BinaryFieldMapper.CONTENT_TYPE, builder);
        assertTrue(builder.delegateBuilder instanceof BinaryFieldMapper.Builder);
    }

    public void testTypeParser_parse_whenNoRawFieldType_thenDelegateTextFieldMapper() {
        final Map<String, Object> node = createFieldConfig(null);
        node.remove(RAW_FIELD_TYPE);

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(fieldName, node, parserContext);

        assertBuilderParseParametersSuccessfully(TextFieldMapper.CONTENT_TYPE, builder);
        assertTrue(builder.delegateBuilder instanceof TextFieldMapper.Builder);
    }

    public void testTypeParser_parse_whenNullRawFieldType_thenException() {
        final Map<String, Object> node = createFieldConfig(null);
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> TYPE_PARSER.parse(fieldName, node, parserContext)
        );
        final String expectedError = "raw_field_type null is not supported. It should be one of";
        assertTrue(exception.getMessage().contains(expectedError));
    }

    public void testTypeParser_parse_whenUnsupportedRawFieldType_thenException() {
        final Map<String, Object> node = createFieldConfig("unsupported");
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> TYPE_PARSER.parse(fieldName, node, parserContext)
        );
        final String expectedError = "raw_field_type unsupported is not supported. It should be one of";
        assertTrue(exception.getMessage().contains(expectedError));
    }

    private void assertBuilderParseParametersSuccessfully(
        @NonNull final String rawFieldType,
        @NonNull final SemanticFieldMapper.Builder builder
    ) {
        assertEquals(rawFieldType, builder.rawFieldType.getValue());
        assertEquals(modelId, builder.modelId.getValue());
        assertEquals(searchModelId, builder.searchModelId.getValue());
        assertEquals(semanticInfoFieldName, builder.semanticInfoFieldName.getValue());
    }

    private Map<String, Object> createFieldConfig(final String rawFieldType) {
        Map<String, Object> node = new HashMap<>();
        node.put(TYPE, SemanticFieldMapper.CONTENT_TYPE);
        node.put(RAW_FIELD_TYPE, rawFieldType);
        node.put(MODEL_ID, modelId);
        node.put(SEARCH_MODEL_ID, searchModelId);
        node.put(SEMANTIC_INFO_FIELD_NAME, semanticInfoFieldName);
        return node;
    }

    public void testBuilder_getParameters() {
        final SemanticFieldMapper.Builder builder = new SemanticFieldMapper.Builder(fieldName);
        assertEquals(9, builder.getParameters().size());
        List<String> actualParams = builder.getParameters().stream().map(a -> a.name).collect(Collectors.toList());
        List<String> expectedParams = Arrays.asList(
            MODEL_ID,
            SEARCH_MODEL_ID,
            RAW_FIELD_TYPE,
            SEMANTIC_INFO_FIELD_NAME,
            CHUNKING,
            SEMANTIC_FIELD_SEARCH_ANALYZER,
            DENSE_EMBEDDING_CONFIG,
            SPARSE_ENCODING_CONFIG,
            SKIP_EXISTING_EMBEDDING
        );
        assertEquals(expectedParams, actualParams);
    }

    public void testBuilder_build_shouldBuildDelegateFieldMapper() {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(parserContext);

        assertTrue(semanticFieldMapper.getDelegateFieldMapper() instanceof TextFieldMapper);
        assertTrue(semanticFieldMapper.fieldType() instanceof SemanticFieldMapper.SemanticFieldType);
    }

    public void testFieldMapper_getMergeBuilder_shouldSetDelegateMergeBuilder() {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(parserContext);
        final SemanticFieldMapper.Builder mergeBuilder = semanticFieldMapper.getMergeBuilder();
        assertTrue(mergeBuilder.delegateBuilder instanceof TextFieldMapper.Builder);
    }

    public void testFieldMapper_merge_shouldMergeDelegateFieldMapper() {
        final String newModelId = "newModelId";
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(parserContext);

        final Map<String, Object> updatedConfig = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        updatedConfig.put(MODEL_ID, newModelId);
        final SemanticFieldMapper semanticFieldMapperToMerge = buildSemanticFieldMapper(updatedConfig, parserContext);

        final SemanticFieldMapper mergedSemanticFieldMapper = (SemanticFieldMapper) semanticFieldMapper.merge(semanticFieldMapperToMerge);

        assertEquals(newModelId, mergedSemanticFieldMapper.getMergeBuilder().modelId.getValue());
        // A new delegate field mapper should be created after the merge and it's still a text field mapper
        assertNotEquals(semanticFieldMapper.getDelegateFieldMapper(), mergedSemanticFieldMapper.getDelegateFieldMapper());
        assertTrue(mergedSemanticFieldMapper.getDelegateFieldMapper() instanceof TextFieldMapper);
    }

    public void testFieldMapper_merge_whenTryUpdateSemanticInfoFieldName_thenException() {
        final String newSemanticInfoFieldName = "newSemanticInfoFieldName";
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(parserContext);

        final Map<String, Object> updatedConfig = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        updatedConfig.put(SEMANTIC_INFO_FIELD_NAME, newSemanticInfoFieldName);
        final SemanticFieldMapper semanticFieldMapperToMerge = buildSemanticFieldMapper(updatedConfig, parserContext);

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> semanticFieldMapper.merge(semanticFieldMapperToMerge)
        );

        final String expectedError = "Mapper for [testField] conflicts with existing mapper:\n"
            + "\tCannot update parameter [semantic_info_field_name] from [semanticInfoFieldName] to [newSemanticInfoFieldName]";
        assertEquals(expectedError, exception.getMessage());
    }

    public void testFieldMapper_merge_whenTryUpdateDenseEmbeddingConfig_thenException() {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(parserContext);

        final Map<String, Object> updatedConfig = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        updatedConfig.put(DENSE_EMBEDDING_CONFIG, Map.of("method", Map.of("engine", "lucene")));
        final SemanticFieldMapper semanticFieldMapperToMerge = buildSemanticFieldMapper(updatedConfig, parserContext);

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> semanticFieldMapper.merge(semanticFieldMapperToMerge)
        );

        final String expectedError = "Mapper for [testField] conflicts with existing mapper:\n"
            + "\tCannot update parameter [dense_embedding_config] from [null] to [{method={engine=lucene}}]";
        assertEquals(expectedError, exception.getMessage());
    }

    public void testFieldMapper_merge_whenTrySparseEncodingConfig_thenException() {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(parserContext);

        final Map<String, Object> updatedConfig = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        updatedConfig.put(SPARSE_ENCODING_CONFIG, Map.of(PruneUtils.PRUNE_TYPE_FIELD, PruneType.NONE.getValue()));
        final SemanticFieldMapper semanticFieldMapperToMerge = buildSemanticFieldMapper(updatedConfig, parserContext);

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> semanticFieldMapper.merge(semanticFieldMapperToMerge)
        );

        final String expectedError = "Mapper for [testField] conflicts with existing mapper:\n"
            + "\tCannot update parameter [sparse_encoding_config] from [null] to [{prune_type=none}]";
        assertEquals(expectedError, exception.getMessage());
    }

    public void testFieldMapper_merge_whenTryUpdateChunkingEnabled_thenException() {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(parserContext);

        final Map<String, Object> updatedConfig = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        updatedConfig.put(CHUNKING, true);
        final SemanticFieldMapper semanticFieldMapperToMerge = buildSemanticFieldMapper(updatedConfig, parserContext);

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> semanticFieldMapper.merge(semanticFieldMapperToMerge)
        );

        final String expectedError = "Mapper for [testField] conflicts with existing mapper:\n"
            + "\tCannot update parameter [chunking] from [null] to [true]";
        assertEquals(expectedError, exception.getMessage());
    }

    public void testFieldMapper_merge_whenTryUpdateNonNullSearchAnalyzer_thenSucceed() {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(parserContext);

        final Map<String, Object> updatedConfig = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        updatedConfig.remove(SEARCH_MODEL_ID);
        updatedConfig.put(SEMANTIC_FIELD_SEARCH_ANALYZER, "standard");
        final SemanticFieldMapper semanticFieldMapperToMerge = buildSemanticFieldMapper(updatedConfig, parserContext);

        final SemanticFieldMapper mergedSemanticFieldMapper = (SemanticFieldMapper) semanticFieldMapper.merge(semanticFieldMapperToMerge);

        assertEquals("standard", mergedSemanticFieldMapper.getMergeBuilder().getSemanticParameters().getSemanticFieldSearchAnalyzer());
    }

    public void testFieldMapper_merge_whenConflictWithDelegateMapperParameters_thenException() {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(parserContext);

        final Map<String, Object> updatedConfig = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        updatedConfig.put(RAW_FIELD_TYPE, KeywordFieldMapper.CONTENT_TYPE);
        final SemanticFieldMapper semanticFieldMapperToMerge = buildSemanticFieldMapper(updatedConfig, parserContext);

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> semanticFieldMapper.merge(semanticFieldMapperToMerge)
        );

        final String expectedError = "Failed to update the mapper testField because failed to update the delegate "
            + "mapper for the raw_field_type text due to mapper [testField] cannot be changed from type [text] to "
            + "[keyword]";
        assertEquals(expectedError, exception.getMessage());
    }

    public void testFieldMapper_parseCreateField_shouldDelegateToDelegateMapper() throws IOException {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(parserContext);
        ParametrizedFieldMapper delegateMapper = mock(ParametrizedFieldMapper.class);
        semanticFieldMapper.setDelegateFieldMapper(delegateMapper);

        semanticFieldMapper.parseCreateField(parseContext);

        verify(delegateMapper, times(1)).parse(parseContext);
    }

    public void testFieldMapper_contentType_shouldReturnSemantic() {
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(parserContext);
        assertEquals(SemanticFieldMapper.CONTENT_TYPE, semanticFieldMapper.contentType());
    }

    public void testFieldMapper_doXContentBody_shouldReturnParameters() throws IOException {
        final Map<String, Object> config = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        config.put(DENSE_EMBEDDING_CONFIG, TEST_DENSE_EMBEDDING_CONFIG);
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(config, parserContext);
        final XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        semanticFieldMapper.doXContentBody(xContentBuilder, false, ToXContent.EMPTY_PARAMS);
        xContentBuilder.endObject();
        final Map<String, Object> out = xContentBuilderToMap(xContentBuilder);

        final Map<String, Object> expected = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        expected.put(DENSE_EMBEDDING_CONFIG, TEST_DENSE_EMBEDDING_CONFIG);
        assertEquals(expected, out);
    }

    public void testFieldMapper_doXContentBody_withDefaultRawFieldType_shouldReturnDefaultValue() throws IOException {
        final Map<String, Object> config = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        config.remove(RAW_FIELD_TYPE);

        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(config, parserContext);

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        semanticFieldMapper.doXContentBody(xContentBuilder, false, ToXContent.EMPTY_PARAMS);
        xContentBuilder.endObject();
        Map<String, Object> out = xContentBuilderToMap(xContentBuilder);

        Map<String, Object> expected = createFieldConfig(TextFieldMapper.CONTENT_TYPE);

        assertEquals(expected, out);
    }

    public void testFieldMapper_withBothSearchModelIdAndAnalyzer_shouldFail() throws IOException {
        final Map<String, Object> config = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        config.put(SEMANTIC_FIELD_SEARCH_ANALYZER, "standard");

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> buildSemanticFieldMapper(config, parserContext)
        );

        final String expectedMessage = "search_model_id can not coexist with semantic_field_search_analyzer in semantic field testField";

        assertEquals(expectedMessage, exception.getMessage());
    }

    public void testFieldMapper_parse_withEmptyAnalyzer_shouldFail() throws IOException {
        final Map<String, Object> config = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        config.remove(SEARCH_MODEL_ID);
        config.put(SEMANTIC_FIELD_SEARCH_ANALYZER, "");

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> buildSemanticFieldMapper(config, parserContext)
        );

        final String expectedMessage = "semantic_field_search_analyzer can not be empty string in semantic field testField";

        assertEquals(expectedMessage, exception.getMessage());
    }

    public void testFieldMapper_parse_withEmptySearchModelId_shouldFail() {
        final Map<String, Object> config = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        config.put(SEARCH_MODEL_ID, "");

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> buildSemanticFieldMapper(config, parserContext)
        );

        final String expectedMessage = "search_model_id can not be empty string in semantic field testField";

        assertEquals(expectedMessage, exception.getMessage());
    }

    public void testFieldMapper_iterator_whenMultiFields() {
        final Map<String, Object> config = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        final Map<String, Object> fieldsConfig = new HashMap<>();
        final Map<String, Object> keywordConfig = new HashMap<>();
        config.put("fields", fieldsConfig);
        fieldsConfig.put("keyword", keywordConfig);
        keywordConfig.put(TYPE, KeywordFieldMapper.CONTENT_TYPE);
        final SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(config, parserContext);

        // verify it should iterate the multiFields of the delegate field mapper
        List<Mapper> children = new ArrayList<>();
        for (Mapper mapper : semanticFieldMapper) {
            children.add(mapper);
        }
        assertEquals(1, children.size());
        assertTrue(children.getFirst() instanceof KeywordFieldMapper);
    }

    private SemanticFieldMapper buildSemanticFieldMapper(final @NonNull Mapper.TypeParser.ParserContext parserContext) {
        final Map<String, Object> node = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        return buildSemanticFieldMapper(node, parserContext);
    }

    private SemanticFieldMapper buildSemanticFieldMapper(
        final @NonNull Map<String, Object> fieldConfig,
        final @NonNull Mapper.TypeParser.ParserContext parserContext
    ) {
        final SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(fieldName, fieldConfig, parserContext);

        final Settings settings = Settings.builder().put(settings(CURRENT).build()).put(KNN_INDEX, true).build();
        final ParametrizedFieldMapper.BuilderContext builderContext = new ParametrizedFieldMapper.BuilderContext(
            settings,
            new ContentPath()
        );

        return builder.build(builderContext);
    }
}
