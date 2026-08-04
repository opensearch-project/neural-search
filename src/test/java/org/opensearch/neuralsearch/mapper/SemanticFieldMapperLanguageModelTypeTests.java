/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper;

import lombok.NonNull;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Before;
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
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.mapper.WildcardFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.Version.CURRENT;
import static org.opensearch.knn.index.KNNSettings.KNN_INDEX;
import static org.opensearch.neuralsearch.constants.MappingConstants.TYPE;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.LANGUAGE_OPTION;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.MODEL_ID;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.MODEL_TYPE;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.RAW_FIELD_TYPE;
import static org.opensearch.neuralsearch.util.TestUtils.xContentBuilderToMap;

public class SemanticFieldMapperLanguageModelTypeTests extends OpenSearchTestCase {

    private final String fieldName = "testField";
    private final SemanticFieldMapper.TypeParser TYPE_PARSER = new SemanticFieldMapper.TypeParser();

    private MapperService mapperService = mock(MapperService.class);

    private static final IndexAnalyzers indexAnalyzers = new IndexAnalyzers(
        singletonMap("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
        emptyMap(),
        emptyMap()
    );

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
    );

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        when(mapperService.getIndexAnalyzers()).thenReturn(indexAnalyzers);
    }

    public void testTypeParser_parse_withLanguageOptionAndModelType() {
        Map<String, Object> node = createFieldConfigWithLanguageModelType("ENGLISH", "SPARSE");

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(fieldName, node, parserContext);

        assertEquals("ENGLISH", builder.getLanguageOption().getValue());
        assertEquals("SPARSE", builder.getModelType().getValue());
        assertNull(builder.getModelId().getValue());
    }

    public void testTypeParser_parse_withLanguageOptionOnly() {
        Map<String, Object> node = createFieldConfigWithLanguageModelType("MULTILINGUAL", null);
        node.remove(MODEL_TYPE);

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(fieldName, node, parserContext);

        assertEquals("MULTILINGUAL", builder.getLanguageOption().getValue());
        assertNull(builder.getModelType().getValue());
        assertNull(builder.getModelId().getValue());
    }

    public void testTypeParser_parse_withModelTypeOnly() {
        Map<String, Object> node = createFieldConfigWithLanguageModelType(null, "DENSE");
        node.remove(LANGUAGE_OPTION);

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(fieldName, node, parserContext);

        assertNull(builder.getLanguageOption().getValue());
        assertEquals("DENSE", builder.getModelType().getValue());
        assertNull(builder.getModelId().getValue());
    }

    /**
     * TypeParser now accepts model_id + language_option because the mutual-exclusion
     * validation has been moved to the SemanticMappingTransformer (which runs before
     * the mapper parses, on the raw user input). After model resolution, the transformer
     * sets model_id on fields that originally had only language_option/model_type, so
     * the TypeParser must accept all three coexisting.
     */
    public void testTypeParser_parse_acceptsModelIdWithLanguageOption() {
        Map<String, Object> node = new HashMap<>();
        node.put(TYPE, SemanticFieldMapper.CONTENT_TYPE);
        node.put(MODEL_ID, "some_model_id");
        node.put(LANGUAGE_OPTION, "ENGLISH");

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(fieldName, node, parserContext);

        assertEquals("some_model_id", builder.getModelId().getValue());
        assertEquals("ENGLISH", builder.getLanguageOption().getValue());
    }

    public void testTypeParser_parse_acceptsModelIdWithModelType() {
        Map<String, Object> node = new HashMap<>();
        node.put(TYPE, SemanticFieldMapper.CONTENT_TYPE);
        node.put(MODEL_ID, "some_model_id");
        node.put(MODEL_TYPE, "SPARSE");

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(fieldName, node, parserContext);

        assertEquals("some_model_id", builder.getModelId().getValue());
        assertEquals("SPARSE", builder.getModelType().getValue());
    }

    public void testTypeParser_parse_acceptsModelIdWithBothLanguageOptionAndModelType() {
        Map<String, Object> node = new HashMap<>();
        node.put(TYPE, SemanticFieldMapper.CONTENT_TYPE);
        node.put(MODEL_ID, "some_model_id");
        node.put(LANGUAGE_OPTION, "ENGLISH");
        node.put(MODEL_TYPE, "SPARSE");

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(fieldName, node, parserContext);

        assertEquals("some_model_id", builder.getModelId().getValue());
        assertEquals("ENGLISH", builder.getLanguageOption().getValue());
        assertEquals("SPARSE", builder.getModelType().getValue());
    }

    public void testBuilder_build_withLanguageOptionAndModelType() {
        Map<String, Object> node = createFieldConfigWithLanguageModelType("ENGLISH", "DENSE");
        SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(node, parserContext);

        assertNotNull(semanticFieldMapper);
        assertTrue(semanticFieldMapper.fieldType() instanceof SemanticFieldMapper.SemanticFieldType);
    }

    public void testFieldMapper_doXContentBody_serializesLanguageOptionAndModelType() throws IOException {
        Map<String, Object> config = createFieldConfigWithLanguageModelType("MULTILINGUAL", "DENSE");
        SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(config, parserContext);

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        semanticFieldMapper.doXContentBody(xContentBuilder, false, ToXContent.EMPTY_PARAMS);
        xContentBuilder.endObject();
        Map<String, Object> out = xContentBuilderToMap(xContentBuilder);

        assertEquals("MULTILINGUAL", out.get(LANGUAGE_OPTION));
        assertEquals("DENSE", out.get(MODEL_TYPE));
        assertFalse(out.containsKey(MODEL_ID));
    }

    public void testBuilder_getParameters_includesLanguageOptionAndModelType() {
        SemanticFieldMapper.Builder builder = new SemanticFieldMapper.Builder(fieldName);
        assertEquals(11, builder.getParameters().size());
        assertTrue(builder.getParameters().stream().anyMatch(p -> LANGUAGE_OPTION.equals(p.name)));
        assertTrue(builder.getParameters().stream().anyMatch(p -> MODEL_TYPE.equals(p.name)));
    }

    private Map<String, Object> createFieldConfigWithLanguageModelType(String languageOption, String modelType) {
        Map<String, Object> node = new HashMap<>();
        node.put(TYPE, SemanticFieldMapper.CONTENT_TYPE);
        node.put(RAW_FIELD_TYPE, TextFieldMapper.CONTENT_TYPE);
        if (languageOption != null) {
            node.put(LANGUAGE_OPTION, languageOption);
        }
        if (modelType != null) {
            node.put(MODEL_TYPE, modelType);
        }
        return node;
    }

    private SemanticFieldMapper buildSemanticFieldMapper(
        @NonNull final Map<String, Object> fieldConfig,
        @NonNull final Mapper.TypeParser.ParserContext parserContext
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
