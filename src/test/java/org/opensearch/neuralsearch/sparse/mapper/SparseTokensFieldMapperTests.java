/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.mapper;

import org.junit.Before;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.NAME_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.PARAMETERS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SEISMIC;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;

public class SparseTokensFieldMapperTests extends AbstractSparseTestBase {
    private SparseTokensFieldMapper.Builder builder;
    private SparseMethodContext sparseMethodContext;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put(SUMMARY_PRUNE_RATIO_FIELD, 0.5f);
        parameters.put(N_POSTINGS_FIELD, 10);
        parameters.put(CLUSTER_RATIO_FIELD, 0.3f);
        parameters.put(APPROXIMATE_THRESHOLD_FIELD, 100);

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, SEISMIC);
        methodMap.put(PARAMETERS_FIELD, parameters);
        sparseMethodContext = SparseMethodContext.parse(methodMap);

        builder = new SparseTokensFieldMapper.Builder("test_field");
    }

    public void testBuilder_withValidParameters_createsBuilder() {
        assertNotNull(builder);
        assertEquals("test_field", builder.name());
    }

    public void testBuilder_build_createsFieldMapper() {
        builder.sparseMethodContext.setValue(sparseMethodContext);

        SparseTokensFieldMapper mapper = (SparseTokensFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );

        assertNotNull(mapper);
        assertEquals("test_field", mapper.simpleName());
        assertEquals(SparseTokensFieldMapper.CONTENT_TYPE, mapper.contentType());
        assertEquals(sparseMethodContext, mapper.getSparseMethodContext());
    }

    public void testBuilder_withStoredTrue_setsStoredCorrectly() {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        builder.stored.setValue(true);

        SparseTokensFieldMapper mapper = (SparseTokensFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );

        assertTrue(mapper.isStored());
    }

    public void testBuilder_withDocValuesFalse_setsDocValuesCorrectly() {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        builder.hasDocValues.setValue(false);

        SparseTokensFieldMapper mapper = (SparseTokensFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );

        assertFalse(mapper.isHasDocValues());
    }

    public void testContentType_returnsCorrectValue() {
        assertEquals("sparse_tokens", SparseTokensFieldMapper.CONTENT_TYPE);
    }

    public void testGetMergeBuilder_returnsNewBuilder() {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        SparseTokensFieldMapper mapper = (SparseTokensFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );

        SparseTokensFieldMapper.Builder mergeBuilder = (SparseTokensFieldMapper.Builder) mapper.getMergeBuilder();

        assertNotNull(mergeBuilder);
        assertEquals(mapper.simpleName(), mergeBuilder.name());
    }

    public void testClone_createsNewInstance() {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        SparseTokensFieldMapper mapper = (SparseTokensFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );

        SparseTokensFieldMapper cloned = mapper.clone();

        assertNotNull(cloned);
        assertNotSame(mapper, cloned);
        assertEquals(mapper.simpleName(), cloned.simpleName());
        assertEquals(mapper.contentType(), cloned.contentType());
        assertEquals(mapper.getSparseMethodContext(), cloned.getSparseMethodContext());
        assertEquals(mapper.isStored(), cloned.isStored());
        assertEquals(mapper.isHasDocValues(), cloned.isHasDocValues());
        assertEquals(mapper.fieldType().getClass(), cloned.fieldType().getClass());
    }

    public void testFieldType_returnsCorrectType() {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        SparseTokensFieldMapper mapper = (SparseTokensFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );

        SparseTokensFieldType fieldType = mapper.fieldType();

        assertNotNull(fieldType);
        assertTrue(fieldType instanceof SparseTokensFieldType);
    }

    public void testParseCreateField_withExternalValueSet_throwsException() throws IOException {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        SparseTokensFieldMapper mapper = (SparseTokensFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );

        ParseContext context = mock(ParseContext.class);
        when(context.externalValueSet()).thenReturn(true);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> { mapper.parseCreateField(context); });
        assertEquals("[sparse_tokens] fields can't be used in multi-fields", exception.getMessage());
    }

    public void testParseCreateField_withInvalidToken_throwsException() throws IOException {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        SparseTokensFieldMapper mapper = (SparseTokensFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );

        ParseContext context = mock(ParseContext.class);
        XContentParser parser = mock(XContentParser.class);
        when(context.externalValueSet()).thenReturn(false);
        when(context.parser()).thenReturn(parser);
        when(parser.currentToken()).thenReturn(XContentParser.Token.VALUE_STRING);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> { mapper.parseCreateField(context); });
        assertTrue(exception.getMessage().contains("fields must be json objects"));
    }

    public void testSparseTypeParser_withValidInput_returnsBuilder() throws MapperParsingException {
        SparseTokensFieldMapper.SparseTypeParser parser = new SparseTokensFieldMapper.SparseTypeParser();
        Map<String, Object> node = new HashMap<>();
        Map<String, Object> method = new HashMap<>();
        method.put(NAME_FIELD, SEISMIC);
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(SUMMARY_PRUNE_RATIO_FIELD, 0.5f);
        method.put(PARAMETERS_FIELD, parameters);
        node.put("method", method);

        SparseTokensFieldMapper.Builder result = (SparseTokensFieldMapper.Builder) parser.parse(
            "test_field",
            node,
            mock(Mapper.TypeParser.ParserContext.class)
        );

        assertNotNull(result);
        assertEquals("test_field", result.name());
    }

    public void testSparseTypeParser_withoutMethod_throwsException() {
        SparseTokensFieldMapper.SparseTypeParser parser = new SparseTokensFieldMapper.SparseTypeParser();
        Map<String, Object> node = new HashMap<>();

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> {
            parser.parse("test_field", node, mock(Mapper.TypeParser.ParserContext.class));
        });
        assertTrue(exception.getMessage().contains("requires [method] parameter"));
    }

    public void testSparseTypeParser_withNullMethodName_throwsException() {
        // This test shows that line 252 of SparseTokensFieldMapper.java could never reach
        SparseTokensFieldMapper.SparseTypeParser parser = new SparseTokensFieldMapper.SparseTypeParser();
        Map<String, Object> node = new HashMap<>();
        Map<String, Object> method = new HashMap<>();
        method.put(NAME_FIELD, null); // null name
        node.put("method", method);

        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            parser.parse("test_field", node, mock(Mapper.TypeParser.ParserContext.class));
        });
        assertTrue(exception.getMessage().contains("Cannot invoke \"String.isEmpty()\""));
    }

    public void testSparseTypeParser_withoutMethodName_throwsException() {
        SparseTokensFieldMapper.SparseTypeParser parser = new SparseTokensFieldMapper.SparseTypeParser();
        Map<String, Object> node = new HashMap<>();
        Map<String, Object> method = new HashMap<>();
        node.put("method", method);

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> {
            parser.parse("test_field", node, mock(Mapper.TypeParser.ParserContext.class));
        });
        assertTrue(exception.getMessage().contains("name needs to be set"));
    }

    public void testSparseTypeParser_withUnsupportedMethod_throwsException() {
        SparseTokensFieldMapper.SparseTypeParser parser = new SparseTokensFieldMapper.SparseTypeParser();
        Map<String, Object> node = new HashMap<>();
        Map<String, Object> method = new HashMap<>();
        method.put(NAME_FIELD, "unsupported_method");
        node.put("method", method);

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> {
            parser.parse("test_field", node, mock(Mapper.TypeParser.ParserContext.class));
        });
        assertTrue(exception.getMessage().contains("is not supported"));
    }

    public void testSparseTypeParser_withInvalidParameters_throwsException() {
        SparseTokensFieldMapper.SparseTypeParser parser = new SparseTokensFieldMapper.SparseTypeParser();
        Map<String, Object> node = new HashMap<>();
        Map<String, Object> method = new HashMap<>();
        method.put(NAME_FIELD, SEISMIC);
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(SUMMARY_PRUNE_RATIO_FIELD, -1.0f);
        method.put(PARAMETERS_FIELD, parameters);
        node.put("method", method);

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> {
            parser.parse("test_field", node, mock(Mapper.TypeParser.ParserContext.class));
        });
        assertTrue(exception.getMessage().contains("Validation Failed"));
    }

    public void testDefaults_fieldTypeAttributes() {
        Map<String, String> fieldTypeAttrs = SparseTokensFieldMapper.Defaults.FIELD_TYPE.getAttributes();
        assertTrue(fieldTypeAttrs.containsKey("sparse_tokens_field"));
        assertEquals("true", fieldTypeAttrs.get("sparse_tokens_field"));

        Map<String, String> tokenFieldTypeAttrs = SparseTokensFieldMapper.Defaults.TOKEN_FIELD_TYPE.getAttributes();
        assertTrue(tokenFieldTypeAttrs.containsKey("sparse_tokens_field"));
        assertEquals("true", tokenFieldTypeAttrs.get("sparse_tokens_field"));
    }

    public void testBuilder_getParameters_returnsCorrectParameters() {
        assertEquals(3, builder.getParameters().size());
    }

    public void testParseCreateField_withValidJsonObject_parsesSuccessfully() throws IOException {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        SparseTokensFieldMapper mapper = (SparseTokensFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );

        ParseContext context = mock(ParseContext.class);
        XContentParser parser = mock(XContentParser.class);
        ParseContext.Document doc = mock(ParseContext.Document.class);

        when(context.externalValueSet()).thenReturn(false);
        when(context.parser()).thenReturn(parser);
        when(context.doc()).thenReturn(doc);
        when(parser.currentToken()).thenReturn(XContentParser.Token.START_OBJECT);
        when(parser.nextToken()).thenReturn(XContentParser.Token.FIELD_NAME)
            .thenReturn(XContentParser.Token.VALUE_NUMBER)
            .thenReturn(XContentParser.Token.END_OBJECT);
        when(parser.currentName()).thenReturn("feature1");
        when(parser.floatValue(true)).thenReturn(0.5f);
        when(doc.getByKey(any())).thenReturn(null);

        mapper.parseCreateField(context);

        verify(doc, times(1)).add(any()); // Only SparseTokensField is added to doc
        verify(doc, times(1)).addWithKey(any(), any()); // FeatureField is added with key
    }

    public void testParseCreateField_withNullValue_ignoresFeature() throws IOException {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        SparseTokensFieldMapper mapper = (SparseTokensFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );

        ParseContext context = mock(ParseContext.class);
        XContentParser parser = mock(XContentParser.class);
        ParseContext.Document doc = mock(ParseContext.Document.class);

        when(context.externalValueSet()).thenReturn(false);
        when(context.parser()).thenReturn(parser);
        when(context.doc()).thenReturn(doc);
        when(parser.currentToken()).thenReturn(XContentParser.Token.START_OBJECT);
        when(parser.nextToken()).thenReturn(XContentParser.Token.FIELD_NAME)
            .thenReturn(XContentParser.Token.VALUE_NULL)
            .thenReturn(XContentParser.Token.END_OBJECT);
        when(parser.currentName()).thenReturn("feature1");

        mapper.parseCreateField(context);

        verify(doc, times(1)).add(any()); // Only SparseTokensField
    }

    public void testParseCreateField_withDuplicateFeature_throwsException() throws IOException {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        SparseTokensFieldMapper mapper = (SparseTokensFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );

        ParseContext context = mock(ParseContext.class);
        XContentParser parser = mock(XContentParser.class);
        ParseContext.Document doc = mock(ParseContext.Document.class);

        when(context.externalValueSet()).thenReturn(false);
        when(context.parser()).thenReturn(parser);
        when(context.doc()).thenReturn(doc);
        when(parser.currentToken()).thenReturn(XContentParser.Token.START_OBJECT);
        when(parser.nextToken()).thenReturn(XContentParser.Token.FIELD_NAME).thenReturn(XContentParser.Token.VALUE_NUMBER);
        when(parser.currentName()).thenReturn("feature1");
        when(parser.floatValue(true)).thenReturn(0.5f);
        when(doc.getByKey(any())).thenReturn(mock(org.apache.lucene.document.Field.class));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> { mapper.parseCreateField(context); });
        assertTrue(exception.getMessage().contains("do not support indexing multiple values"));
    }

    public void testParseCreateField_withInvalidTokenType_throwsException() throws IOException {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        SparseTokensFieldMapper mapper = (SparseTokensFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );

        ParseContext context = mock(ParseContext.class);
        XContentParser parser = mock(XContentParser.class);
        ParseContext.Document doc = mock(ParseContext.Document.class);

        when(context.externalValueSet()).thenReturn(false);
        when(context.parser()).thenReturn(parser);
        when(context.doc()).thenReturn(doc);
        when(parser.currentToken()).thenReturn(XContentParser.Token.START_OBJECT);
        when(parser.nextToken()).thenReturn(XContentParser.Token.FIELD_NAME).thenReturn(XContentParser.Token.START_ARRAY);
        when(parser.currentName()).thenReturn("feature1");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> { mapper.parseCreateField(context); });
        assertTrue(exception.getMessage().contains("got unexpected token"));
    }

    public void testSparseMethodContextSerialization_withValidContext_serializesCorrectly() throws Exception {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        SparseTokensFieldMapper mapper = (SparseTokensFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );

        // Use XContentFactory to create a real XContentBuilder
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();

        // This will trigger the serializer code: b.startObject(n); v.toXContent(b, ToXContent.EMPTY_PARAMS); b.endObject();
        mapper.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);

        xContentBuilder.endObject();
        String result = xContentBuilder.toString();

        // Verify the serialization contains the method object
        assertTrue(result.contains("method"));
        assertTrue(result.contains(SEISMIC));
    }

}
