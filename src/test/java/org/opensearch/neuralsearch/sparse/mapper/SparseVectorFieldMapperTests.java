/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.mapper;

import org.junit.After;
import org.junit.Before;
import org.mockito.MockitoAnnotations;
import org.opensearch.Version;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.SparseSettings;
import org.opensearch.neuralsearch.sparse.algorithm.ClusterTrainingExecutor;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.algorithm.SparseForwardIndex;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTERING_BATCH_SIZE_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.ENGINE_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.FORWARD_INDEX_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.NAME_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.PARAMETERS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.QUANTIZATION_CEILING_INGEST_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.QUANTIZATION_CEILING_SEARCH_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SEISMIC;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_CLUSTERING_BATCH_SIZE;
import static org.opensearch.neuralsearch.sparse.mapper.SparseVectorField.SPARSE_FIELD;

public class SparseVectorFieldMapperTests extends AbstractSparseTestBase {
    private SparseVectorFieldMapper.Builder builder;
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
        parameters.put(QUANTIZATION_CEILING_SEARCH_FIELD, 3.0f);
        parameters.put(QUANTIZATION_CEILING_INGEST_FIELD, 3.0f);

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, SEISMIC);
        methodMap.put(PARAMETERS_FIELD, parameters);
        sparseMethodContext = SparseMethodContext.parse(methodMap);

        builder = new SparseVectorFieldMapper.Builder("test_field");
    }

    @After
    public void resetSparseSettings() {
        SparseSettings.reset();
    }

    /** Puts the singleton into the state a node with these two flag values would be in. */
    private void setNativeEngineFlags(boolean featureEnabled, boolean dynamicEnabled) {
        Settings nodeSettings = Settings.builder()
            .put(SparseSettings.SPARSE_NATIVE_ENGINE_FEATURE_ENABLED, featureEnabled)
            .put(SparseSettings.SPARSE_NATIVE_ENGINE_ENABLED, dynamicEnabled)
            .build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(nodeSettings);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                nodeSettings,
                Set.of(
                    SparseSettings.SPARSE_NATIVE_ENGINE_FEATURE_ENABLED_SETTING,
                    SparseSettings.SPARSE_NATIVE_ENGINE_ENABLED_SETTING,
                    SparseSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING
                )
            )
        );
        ClusterTrainingExecutor.getInstance().initialize(mock(ThreadPool.class));
        SparseSettings.reset();
        SparseSettings.state().initialize(clusterService, nodeSettings);
    }

    /** A parser context for an index created on a cluster whose oldest node was {@code version}. */
    private static Mapper.TypeParser.ParserContext parserContext(Version version) {
        Mapper.TypeParser.ParserContext parserContext = mock(Mapper.TypeParser.ParserContext.class);
        when(parserContext.indexVersionCreated()).thenReturn(version);
        return parserContext;
    }

    private Map<String, Object> nativeMethodNode() {
        Map<String, Object> method = new HashMap<>();
        method.put(NAME_FIELD, SEISMIC);
        method.put(ENGINE_FIELD, SparseEngine.NATIVE.getName());
        method.put(PARAMETERS_FIELD, new HashMap<String, Object>());
        Map<String, Object> node = new HashMap<>();
        node.put("method", method);
        return node;
    }

    /** Both native-only knobs are method parameters, so they go in the parameters map. */
    private static Map<String, Object> methodNode(SparseEngine engine, Map<String, Object> parameters) {
        Map<String, Object> method = new HashMap<>();
        method.put(NAME_FIELD, SEISMIC);
        method.put(ENGINE_FIELD, engine.getName());
        method.put(PARAMETERS_FIELD, new HashMap<>(parameters));
        Map<String, Object> node = new HashMap<>();
        node.put("method", method);
        return node;
    }

    public void testSparseTypeParser_acceptsPerBlockForwardIndexOnNativeEngine() {
        setNativeEngineFlags(true, true);

        Map<String, Object> node = methodNode(SparseEngine.NATIVE, Map.of(FORWARD_INDEX_FIELD, SparseForwardIndex.PER_BLOCK.getName()));

        assertNotNull(new SparseVectorFieldMapper.SparseTypeParser().parse("test_field", node, parserContext(Version.CURRENT)));
    }

    public void testSparseTypeParser_rejectsPerBlockForwardIndexOnLuceneEngine() {
        setNativeEngineFlags(true, true);

        Map<String, Object> node = methodNode(SparseEngine.LUCENE, Map.of(FORWARD_INDEX_FIELD, SparseForwardIndex.PER_BLOCK.getName()));

        MapperParsingException exception = expectThrows(
            MapperParsingException.class,
            () -> new SparseVectorFieldMapper.SparseTypeParser().parse("test_field", node, parserContext(Version.CURRENT))
        );
        assertTrue(exception.getMessage(), exception.getMessage().contains(FORWARD_INDEX_FIELD));
    }

    public void testSparseTypeParser_acceptsClusteringBatchSizeOnNativeEngine() {
        setNativeEngineFlags(true, true);

        Map<String, Object> node = methodNode(SparseEngine.NATIVE, Map.of(CLUSTERING_BATCH_SIZE_FIELD, 64));

        assertNotNull(new SparseVectorFieldMapper.SparseTypeParser().parse("test_field", node, parserContext(Version.CURRENT)));
    }

    public void testSparseTypeParser_rejectsClusteringBatchSizeOnLuceneEngine() {
        setNativeEngineFlags(true, true);

        Map<String, Object> node = methodNode(SparseEngine.LUCENE, Map.of(CLUSTERING_BATCH_SIZE_FIELD, 64));

        MapperParsingException exception = expectThrows(
            MapperParsingException.class,
            () -> new SparseVectorFieldMapper.SparseTypeParser().parse("test_field", node, parserContext(Version.CURRENT))
        );
        assertTrue(exception.getMessage(), exception.getMessage().contains(CLUSTERING_BATCH_SIZE_FIELD));
    }

    /** The default is what a field that never asked for batching has, whichever engine it runs. */
    public void testSparseTypeParser_acceptsDefaultClusteringBatchSizeOnLuceneEngine() {
        setNativeEngineFlags(true, true);

        Map<String, Object> node = methodNode(SparseEngine.LUCENE, Map.of(CLUSTERING_BATCH_SIZE_FIELD, DEFAULT_CLUSTERING_BATCH_SIZE));

        assertNotNull(new SparseVectorFieldMapper.SparseTypeParser().parse("test_field", node, parserContext(Version.CURRENT)));
    }

    /**
     * The setting being on is not enough: a node that predates these parameters cannot read the
     * mapping at all, so an index created while one was in the cluster is refused. Each parameter is
     * rejected on presence alone, whatever it is set to.
     */
    public void testSparseTypeParser_rejectsNativeEngineParametersOnAnOlderIndex() {
        setNativeEngineFlags(true, true);

        for (Map<String, Object> node : List.of(
            nativeMethodNode(),
            methodNode(SparseEngine.LUCENE, Map.of()),
            methodNode(SparseEngine.NATIVE, Map.of(FORWARD_INDEX_FIELD, SparseForwardIndex.PER_BLOCK.getName())),
            methodNode(SparseEngine.NATIVE, Map.of(CLUSTERING_BATCH_SIZE_FIELD, 64))
        )) {
            MapperParsingException exception = expectThrows(
                MapperParsingException.class,
                () -> new SparseVectorFieldMapper.SparseTypeParser().parse("test_field", node, parserContext(Version.V_3_8_0))
            );
            assertTrue(
                exception.getMessage(),
                exception.getMessage().contains("can only be used on indices created on or after version " + Version.V_3_9_0)
            );
        }
    }

    /** A field that asks for none of them is untouched by the gate, however old the index is. */
    public void testSparseTypeParser_acceptsAMappingWithoutNativeEngineParametersOnAnOlderIndex() {
        setNativeEngineFlags(true, true);

        Map<String, Object> method = new HashMap<>();
        method.put(NAME_FIELD, SEISMIC);
        method.put(PARAMETERS_FIELD, Map.of(N_POSTINGS_FIELD, 10));
        Map<String, Object> node = new HashMap<>();
        node.put(SparseVectorFieldMapper.METHOD, method);

        assertNotNull(new SparseVectorFieldMapper.SparseTypeParser().parse("test_field", node, parserContext(Version.V_3_8_0)));
    }

    public void testSparseTypeParser_rejectsNativeEngineWhenDisabled() {
        setNativeEngineFlags(true, false);

        MapperParsingException exception = expectThrows(
            MapperParsingException.class,
            () -> new SparseVectorFieldMapper.SparseTypeParser().parse("test_field", nativeMethodNode(), parserContext(Version.CURRENT))
        );
        assertTrue(exception.getMessage(), exception.getMessage().contains(SparseSettings.NATIVE_ENGINE_DISABLED_REASON));
    }

    public void testSparseTypeParser_acceptsNativeEngineWhenEnabled() {
        setNativeEngineFlags(true, true);

        assertNotNull(
            new SparseVectorFieldMapper.SparseTypeParser().parse("test_field", nativeMethodNode(), parserContext(Version.CURRENT))
        );
    }

    public void testSparseTypeParser_allowsLuceneEngineWhenNativeIsDisabled() {
        setNativeEngineFlags(true, false);

        Map<String, Object> method = new HashMap<>();
        method.put(NAME_FIELD, SEISMIC);
        method.put(ENGINE_FIELD, SparseEngine.LUCENE.getName());
        method.put(PARAMETERS_FIELD, new HashMap<String, Object>());
        Map<String, Object> node = new HashMap<>();
        node.put("method", method);

        assertNotNull(new SparseVectorFieldMapper.SparseTypeParser().parse("test_field", node, parserContext(Version.CURRENT)));
    }

    public void testParseCreateField_rejectsNativeEngineWhenDisabled() {
        setNativeEngineFlags(true, false);

        Map<String, Object> method = new HashMap<>();
        method.put(NAME_FIELD, SEISMIC);
        method.put(ENGINE_FIELD, SparseEngine.NATIVE.getName());
        method.put(PARAMETERS_FIELD, new HashMap<String, Object>());
        builder.sparseMethodContext.setValue(SparseMethodContext.parse(method));
        SparseVectorFieldMapper mapper = (SparseVectorFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );

        ParseContext context = mock(ParseContext.class);
        when(context.externalValueSet()).thenReturn(false);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> mapper.parseCreateField(context));
        assertTrue(exception.getMessage(), exception.getMessage().contains(SparseSettings.NATIVE_ENGINE_DISABLED_REASON));
        // Rejected before the document was even parsed
        verify(context, never()).parser();
    }

    public void testBuilder_withValidParameters_createsBuilder() {
        assertNotNull(builder);
        assertEquals("test_field", builder.name());
    }

    public void testBuilder_build_createsFieldMapper() {
        builder.sparseMethodContext.setValue(sparseMethodContext);

        SparseVectorFieldMapper mapper = (SparseVectorFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );

        assertNotNull(mapper);
        assertEquals("test_field", mapper.simpleName());
        assertEquals(SparseVectorFieldMapper.CONTENT_TYPE, mapper.contentType());
        assertEquals(sparseMethodContext, mapper.getSparseMethodContext());
    }

    public void testContentType_returnsCorrectValue() {
        assertEquals("sparse_vector", SparseVectorFieldMapper.CONTENT_TYPE);
    }

    public void testGetMergeBuilder_returnsNewBuilder() {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        SparseVectorFieldMapper mapper = (SparseVectorFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );

        SparseVectorFieldMapper.Builder mergeBuilder = (SparseVectorFieldMapper.Builder) mapper.getMergeBuilder();

        assertNotNull(mergeBuilder);
        assertEquals(mapper.simpleName(), mergeBuilder.name());
    }

    public void testClone_createsNewInstance() {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        SparseVectorFieldMapper mapper = (SparseVectorFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );

        SparseVectorFieldMapper cloned = mapper.clone();

        assertNotNull(cloned);
        assertNotSame(mapper, cloned);
        assertEquals(mapper.simpleName(), cloned.simpleName());
        assertEquals(mapper.contentType(), cloned.contentType());
        assertEquals(mapper.getSparseMethodContext(), cloned.getSparseMethodContext());
        assertEquals(mapper.fieldType().getClass(), cloned.fieldType().getClass());
    }

    public void testFieldType_returnsCorrectType() {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        SparseVectorFieldMapper mapper = (SparseVectorFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );

        SparseVectorFieldType fieldType = mapper.fieldType();

        assertNotNull(fieldType);
    }

    public void testParseCreateField_withExternalValueSet_throwsException() throws IOException {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        SparseVectorFieldMapper mapper = (SparseVectorFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );

        ParseContext context = mock(ParseContext.class);
        when(context.externalValueSet()).thenReturn(true);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> { mapper.parseCreateField(context); });
        assertEquals("[sparse_vector] fields can't be used in multi-fields", exception.getMessage());
    }

    public void testParseCreateField_withInvalidToken_throwsException() throws IOException {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        SparseVectorFieldMapper mapper = (SparseVectorFieldMapper) builder.build(
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

    public void testParseCreateField_withoutSeismicInSparseMethodContext() throws IOException {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(SUMMARY_PRUNE_RATIO_FIELD, 0.5f);
        parameters.put(N_POSTINGS_FIELD, 10);
        parameters.put(CLUSTER_RATIO_FIELD, 0.3f);
        parameters.put(APPROXIMATE_THRESHOLD_FIELD, 100);

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "non" + SEISMIC);
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext nonSeismicSparseMethodContext = SparseMethodContext.parse(methodMap);
        builder.sparseMethodContext.setValue(nonSeismicSparseMethodContext);
        SparseVectorFieldMapper mapper = (SparseVectorFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );
        // Only the marker attribute from Defaults.FIELD_TYPE; no seismic parameters get
        // written for a method that is not seismic.
        Map<String, String> attributes = mapper.getLuceneFieldType().getAttributes();
        assertEquals(1, attributes.size());
        assertTrue(Boolean.parseBoolean(attributes.get(SPARSE_FIELD)));
    }

    public void testParseCreateField_withSeismicInSparseMethodContext() throws IOException {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        SparseVectorFieldMapper mapper = (SparseVectorFieldMapper) builder.build(
            new ParametrizedFieldMapper.BuilderContext(TestsPrepareUtils.prepareIndexSettings(), TestsPrepareUtils.prepareContentPath())
        );

        Map<String, String> attributes = mapper.getLuceneFieldType().getAttributes();
        assertTrue(Boolean.parseBoolean(attributes.get(SPARSE_FIELD)));
        assertEquals("10", attributes.get(N_POSTINGS_FIELD));
        assertEquals("0.5", attributes.get(SUMMARY_PRUNE_RATIO_FIELD));
        assertEquals("0.3", attributes.get(CLUSTER_RATIO_FIELD));
        assertEquals("100", attributes.get(APPROXIMATE_THRESHOLD_FIELD));
        assertEquals("3.0", attributes.get(QUANTIZATION_CEILING_INGEST_FIELD));
        assertEquals("3.0", attributes.get(QUANTIZATION_CEILING_SEARCH_FIELD));
        // The engine attribute is what the codec dispatches the field on at write time
        assertEquals(SparseEngine.DEFAULT.getName(), attributes.get(ENGINE_FIELD));
        assertEquals(SparseForwardIndex.DEFAULT.getName(), attributes.get(FORWARD_INDEX_FIELD));
        assertEquals(String.valueOf(DEFAULT_CLUSTERING_BATCH_SIZE), attributes.get(CLUSTERING_BATCH_SIZE_FIELD));
    }

    public void testSparseTypeParser_withValidInput_returnsBuilder() throws MapperParsingException {
        SparseVectorFieldMapper.SparseTypeParser parser = new SparseVectorFieldMapper.SparseTypeParser();
        Map<String, Object> node = new HashMap<>();
        Map<String, Object> method = new HashMap<>();
        method.put(NAME_FIELD, SEISMIC);
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(SUMMARY_PRUNE_RATIO_FIELD, 0.5f);
        method.put(PARAMETERS_FIELD, parameters);
        node.put("method", method);

        SparseVectorFieldMapper.Builder result = (SparseVectorFieldMapper.Builder) parser.parse(
            "test_field",
            node,
            parserContext(Version.CURRENT)
        );

        assertNotNull(result);
        assertEquals("test_field", result.name());
    }

    public void testSparseTypeParser_withoutMethod_throwsException() {
        SparseVectorFieldMapper.SparseTypeParser parser = new SparseVectorFieldMapper.SparseTypeParser();
        Map<String, Object> node = new HashMap<>();

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> {
            parser.parse("test_field", node, parserContext(Version.CURRENT));
        });
        assertTrue(exception.getMessage().contains("requires [method] parameter"));
    }

    public void testSparseTypeParser_withNullMethodName_throwsException() {
        // This test shows that line 252 of SparseVectorFieldMapper.java could never reach
        SparseVectorFieldMapper.SparseTypeParser parser = new SparseVectorFieldMapper.SparseTypeParser();
        Map<String, Object> node = new HashMap<>();
        Map<String, Object> method = new HashMap<>();
        method.put(NAME_FIELD, null); // null name
        node.put("method", method);

        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            parser.parse("test_field", node, parserContext(Version.CURRENT));
        });
        assertTrue(exception.getMessage().contains("Cannot invoke \"String.isEmpty()\""));
    }

    public void testSparseTypeParser_withoutMethodName_throwsException() {
        SparseVectorFieldMapper.SparseTypeParser parser = new SparseVectorFieldMapper.SparseTypeParser();
        Map<String, Object> node = new HashMap<>();
        Map<String, Object> method = new HashMap<>();
        node.put("method", method);

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> {
            parser.parse("test_field", node, parserContext(Version.CURRENT));
        });
        assertTrue(exception.getMessage().contains("name needs to be set"));
    }

    public void testSparseTypeParser_withUnsupportedMethod_throwsException() {
        SparseVectorFieldMapper.SparseTypeParser parser = new SparseVectorFieldMapper.SparseTypeParser();
        Map<String, Object> node = new HashMap<>();
        Map<String, Object> method = new HashMap<>();
        method.put(NAME_FIELD, "unsupported_method");
        node.put("method", method);

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> {
            parser.parse("test_field", node, parserContext(Version.CURRENT));
        });
        assertTrue(exception.getMessage().contains("is not supported"));
    }

    public void testSparseTypeParser_withInvalidParameters_throwsException() {
        SparseVectorFieldMapper.SparseTypeParser parser = new SparseVectorFieldMapper.SparseTypeParser();
        Map<String, Object> node = new HashMap<>();
        Map<String, Object> method = new HashMap<>();
        method.put(NAME_FIELD, SEISMIC);
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(SUMMARY_PRUNE_RATIO_FIELD, -1.0f);
        method.put(PARAMETERS_FIELD, parameters);
        node.put("method", method);

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> {
            parser.parse("test_field", node, parserContext(Version.CURRENT));
        });
        assertTrue(exception.getMessage().contains("Validation Failed"));
    }

    public void testDefaults_fieldTypeAttributes() {
        Map<String, String> fieldTypeAttrs = SparseVectorFieldMapper.Defaults.FIELD_TYPE.getAttributes();
        assertTrue(fieldTypeAttrs.containsKey("sparse_vector_field"));
        assertEquals("true", fieldTypeAttrs.get("sparse_vector_field"));
    }

    public void testBuilder_getParameters_returnsCorrectParameters() {
        assertEquals(1, builder.getParameters().size());
    }

    private void testParseCreateField_withValidJsonObject_parsesSuccessfully(XContentParser.Token valueToken) throws IOException {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        SparseVectorFieldMapper mapper = (SparseVectorFieldMapper) builder.build(
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
            .thenReturn(valueToken)
            .thenReturn(XContentParser.Token.END_OBJECT);
        when(parser.currentName()).thenReturn("1");
        when(parser.floatValue(true)).thenReturn(0.5f);
        when(doc.getByKey(any())).thenReturn(null);

        mapper.parseCreateField(context);

        verify(doc, times(1)).add(any()); // Only SparseVectorField is added to doc
        verify(doc, times(1)).addWithKey(any(), any()); // FeatureField is added with key
    }

    public void testParseCreateField_withValueNumber_parsesSuccessfully() throws IOException {
        testParseCreateField_withValidJsonObject_parsesSuccessfully(XContentParser.Token.VALUE_NUMBER);
    }

    public void testParseCreateField_withValueString_parsesSuccessfully() throws IOException {
        testParseCreateField_withValidJsonObject_parsesSuccessfully(XContentParser.Token.VALUE_STRING);
    }

    public void testParseCreateField_withNullValue_ignoresFeature() throws IOException {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        SparseVectorFieldMapper mapper = (SparseVectorFieldMapper) builder.build(
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

        verify(doc, times(1)).add(any()); // Only SparseVectorField
    }

    public void testParseCreateField_withDuplicateFeature_throwsException() throws IOException {
        builder.sparseMethodContext.setValue(sparseMethodContext);
        SparseVectorFieldMapper mapper = (SparseVectorFieldMapper) builder.build(
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
        SparseVectorFieldMapper mapper = (SparseVectorFieldMapper) builder.build(
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
        SparseVectorFieldMapper mapper = (SparseVectorFieldMapper) builder.build(
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
