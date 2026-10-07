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
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MatchOnlyTextFieldMapper;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.mapper.WildcardFieldMapper;
import org.opensearch.neuralsearch.mapper.dto.ModelSelection;
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
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.MODEL_SELECTION;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.MODEL_TYPE;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.RAW_FIELD_TYPE;
import static org.opensearch.neuralsearch.util.TestUtils.xContentBuilderToMap;

public class SemanticFieldMapperModelSelectionTests extends OpenSearchTestCase {

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

    public void testTypeParser_parse_withModelSelection() {
        Map<String, Object> node = createFieldConfigWithModelSelection("ENGLISH", "SPARSE");

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(fieldName, node, parserContext);

        assertEquals(new ModelSelection("ENGLISH", "SPARSE"), builder.getModelSelection().getValue());
        assertNull(builder.getModelId().getValue());
    }

    public void testTypeParser_parse_withModelSelectionDefaults() {
        // An empty model_selection object defaults to ENGLISH / SPARSE.
        Map<String, Object> node = new HashMap<>();
        node.put(TYPE, SemanticFieldMapper.CONTENT_TYPE);
        node.put(MODEL_SELECTION, new HashMap<>());

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(fieldName, node, parserContext);

        assertEquals(new ModelSelection("ENGLISH", "SPARSE"), builder.getModelSelection().getValue());
    }

    /**
     * TypeParser accepts model_id + model_selection coexisting. The match validation lives in the
     * SemanticMappingTransformer which runs before the mapper parses, on the raw user input.
     */
    public void testTypeParser_parse_acceptsModelIdWithModelSelection() {
        Map<String, Object> node = new HashMap<>();
        node.put(TYPE, SemanticFieldMapper.CONTENT_TYPE);
        node.put(MODEL_ID, "some_model_id");
        node.put(MODEL_SELECTION, Map.of(LANGUAGE_OPTION, "ENGLISH", MODEL_TYPE, "SPARSE"));

        SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(fieldName, node, parserContext);

        assertEquals("some_model_id", builder.getModelId().getValue());
        assertEquals(new ModelSelection("ENGLISH", "SPARSE"), builder.getModelSelection().getValue());
    }

    public void testTypeParser_parse_withInvalidLanguageOption_thenThrow() {
        Map<String, Object> node = createFieldConfigWithModelSelection("KLINGON", "SPARSE");

        expectThrows(MapperParsingException.class, () -> TYPE_PARSER.parse(fieldName, node, parserContext));
    }

    public void testTypeParser_parse_withInvalidModelType_thenThrow() {
        Map<String, Object> node = createFieldConfigWithModelSelection("ENGLISH", "HYBRID");

        expectThrows(MapperParsingException.class, () -> TYPE_PARSER.parse(fieldName, node, parserContext));
    }

    public void testTypeParser_parse_withUnsupportedModelSelectionParam_thenThrow() {
        Map<String, Object> node = new HashMap<>();
        node.put(TYPE, SemanticFieldMapper.CONTENT_TYPE);
        node.put(MODEL_SELECTION, Map.of(LANGUAGE_OPTION, "ENGLISH", "unexpected", "value"));

        expectThrows(MapperParsingException.class, () -> TYPE_PARSER.parse(fieldName, node, parserContext));
    }

    public void testBuilder_build_withModelSelection() {
        Map<String, Object> node = createFieldConfigWithModelSelection("ENGLISH", "DENSE");
        SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(node, parserContext);

        assertNotNull(semanticFieldMapper);
        assertTrue(semanticFieldMapper.fieldType() instanceof SemanticFieldMapper.SemanticFieldType);
    }

    @SuppressWarnings("unchecked")
    public void testFieldMapper_doXContentBody_serializesModelSelection() throws IOException {
        Map<String, Object> config = createFieldConfigWithModelSelection("MULTILINGUAL", "DENSE");
        SemanticFieldMapper semanticFieldMapper = buildSemanticFieldMapper(config, parserContext);

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        semanticFieldMapper.doXContentBody(xContentBuilder, false, ToXContent.EMPTY_PARAMS);
        xContentBuilder.endObject();
        Map<String, Object> out = xContentBuilderToMap(xContentBuilder);

        assertTrue(out.get(MODEL_SELECTION) instanceof Map);
        Map<String, Object> modelSelection = (Map<String, Object>) out.get(MODEL_SELECTION);
        assertEquals("MULTILINGUAL", modelSelection.get(LANGUAGE_OPTION));
        assertEquals("DENSE", modelSelection.get(MODEL_TYPE));
        assertFalse(out.containsKey(MODEL_ID));
    }

    public void testBuilder_getParameters_includesModelSelection() {
        SemanticFieldMapper.Builder builder = new SemanticFieldMapper.Builder(fieldName);
        assertEquals(10, builder.getParameters().size());
        assertTrue(builder.getParameters().stream().anyMatch(p -> MODEL_SELECTION.equals(p.name)));
    }

    @SuppressWarnings("unchecked")
    public void testMerge_modelSelectionIsUpdatable() throws IOException {
        final SemanticFieldMapper original = buildSemanticFieldMapper(
            createFieldConfigWithModelSelection("ENGLISH", "SPARSE"),
            parserContext
        );
        final SemanticFieldMapper updated = buildSemanticFieldMapper(
            createFieldConfigWithModelSelection("MULTILINGUAL", "DENSE"),
            parserContext
        );

        // model_selection is an updatable parameter, so the merged mapper should carry the updated value.
        final SemanticFieldMapper merged = (SemanticFieldMapper) original.merge(updated);

        final XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        merged.doXContentBody(xContentBuilder, false, ToXContent.EMPTY_PARAMS);
        xContentBuilder.endObject();
        final Map<String, Object> out = xContentBuilderToMap(xContentBuilder);

        final Map<String, Object> modelSelection = (Map<String, Object>) out.get(MODEL_SELECTION);
        assertEquals("MULTILINGUAL", modelSelection.get(LANGUAGE_OPTION));
        assertEquals("DENSE", modelSelection.get(MODEL_TYPE));
    }

    /**
     * Canary pinning today's behavior: a model_selection field's resolved model_id is updatable on merge. When the
     * profile is rotated (M1 -> M2) and the mapping is re-applied, the merged mapper carries the NEW model_id (M2), i.e.
     * the field silently rebinds. If opensearch-project/neural-search#1963 later makes the model binding immutable,
     * this assertion should flip (the merge should then be rejected), turning this test red as a signal of that change.
     */
    public void testMerge_modelSelectionField_modelIdRebindIsAllowedToday() {
        final Map<String, Object> originalConfig = createFieldConfigWithModelSelection("ENGLISH", "SPARSE");
        originalConfig.put(MODEL_ID, "M1");
        final SemanticFieldMapper original = buildSemanticFieldMapper(originalConfig, parserContext);

        final Map<String, Object> updatedConfig = createFieldConfigWithModelSelection("ENGLISH", "SPARSE");
        updatedConfig.put(MODEL_ID, "M2");
        final SemanticFieldMapper updated = buildSemanticFieldMapper(updatedConfig, parserContext);

        final SemanticFieldMapper merged = (SemanticFieldMapper) original.merge(updated);

        assertEquals("M2", merged.getMergeBuilder().modelId.getValue());
    }

    private Map<String, Object> createFieldConfigWithModelSelection(String languageOption, String modelType) {
        Map<String, Object> modelSelection = new HashMap<>();
        if (languageOption != null) {
            modelSelection.put(LANGUAGE_OPTION, languageOption);
        }
        if (modelType != null) {
            modelSelection.put(MODEL_TYPE, modelType);
        }
        Map<String, Object> node = new HashMap<>();
        node.put(TYPE, SemanticFieldMapper.CONTENT_TYPE);
        node.put(RAW_FIELD_TYPE, TextFieldMapper.CONTENT_TYPE);
        node.put(MODEL_SELECTION, modelSelection);
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
