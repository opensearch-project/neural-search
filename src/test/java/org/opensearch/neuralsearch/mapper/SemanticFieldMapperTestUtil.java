/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper;

import lombok.NonNull;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.when;
import static org.opensearch.Version.CURRENT;
import static org.opensearch.knn.index.KNNSettings.KNN_INDEX;
import static org.opensearch.neuralsearch.constants.MappingConstants.TYPE;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.MODEL_ID;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.RAW_FIELD_TYPE;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SEARCH_MODEL_ID;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SEMANTIC_INFO_FIELD_NAME;
import static org.opensearch.test.OpenSearchTestCase.settings;

public class SemanticFieldMapperTestUtil {
    public static final String fieldName = "testField";
    public static final String modelId = "modelId";
    public static final String searchModelId = "searchModelId";
    public static final String semanticInfoFieldName = "semanticInfoFieldName";

    public static final SemanticFieldMapper.TypeParser TYPE_PARSER = new SemanticFieldMapper.TypeParser();
    private static final IndexAnalyzers indexAnalyzers = new IndexAnalyzers(
        singletonMap("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
        emptyMap(),
        emptyMap()
    );

    public static Map<String, Object> createFieldConfig(final String rawFieldType) {
        Map<String, Object> node = new HashMap<>();
        node.put(TYPE, SemanticFieldMapper.CONTENT_TYPE);
        node.put(RAW_FIELD_TYPE, rawFieldType);
        node.put(MODEL_ID, modelId);
        node.put(SEARCH_MODEL_ID, searchModelId);
        node.put(SEMANTIC_INFO_FIELD_NAME, semanticInfoFieldName);
        return node;
    }

    public static void mockParserContext(@NonNull final String rawFieldType, @NonNull final ParametrizedFieldMapper.TypeParser parser, @NonNull final Mapper.TypeParser.ParserContext parserContext) {
        when(parserContext.typeParser(rawFieldType)).thenReturn(parser);
        when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        when(parserContext.indexVersionCreated()).thenReturn(CURRENT);
    }

    public static SemanticFieldMapper buildSemanticFieldMapperWithTextAsRawFieldType(
        final @NonNull Mapper.TypeParser.ParserContext parserContext
    ) {
        final Map<String, Object> node = createFieldConfig(TextFieldMapper.CONTENT_TYPE);
        return buildSemanticFieldMapperWithTextAsRawFieldType(node, parserContext);
    }

    public static SemanticFieldMapper buildSemanticFieldMapperWithTextAsRawFieldType(
        final @NonNull Map<String, Object> fieldConfig,
        final @NonNull Mapper.TypeParser.ParserContext parserContext
    ) {
        return buildSemanticFieldMapper(fieldConfig, TextFieldMapper.CONTENT_TYPE, TextFieldMapper.PARSER, parserContext);

    }

    public static SemanticFieldMapper buildSemanticFieldMapper(
        final @NonNull String rawFieldType,
        final @NonNull ParametrizedFieldMapper.TypeParser typeParser,
        final @NonNull Mapper.TypeParser.ParserContext parserContext
    ) {
        final Map<String, Object> fieldConfig = createFieldConfig(rawFieldType);
        return buildSemanticFieldMapper(fieldConfig, rawFieldType, typeParser, parserContext);
    }

    public static SemanticFieldMapper buildSemanticFieldMapper(
        final @NonNull Map<String, Object> fieldConfig,
        final @NonNull String rawFieldType,
        final @NonNull ParametrizedFieldMapper.TypeParser typeParser,
        final @NonNull Mapper.TypeParser.ParserContext parserContext
    ) {
        mockParserContext(rawFieldType, typeParser, parserContext);

        final SemanticFieldMapper.Builder builder = TYPE_PARSER.parse(SemanticFieldMapperTestUtil.fieldName, fieldConfig, parserContext);

        final Settings settings = Settings.builder().put(settings(CURRENT).build()).put(KNN_INDEX, true).build();
        final ParametrizedFieldMapper.BuilderContext builderContext = new ParametrizedFieldMapper.BuilderContext(
            settings,
            new ContentPath()
        );

        return builder.build(builderContext);
    }
}
