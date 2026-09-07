/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.mapper;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.algorithm.SparseForwardIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.ENGINE_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.FORWARD_INDEX_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.NAME_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.PARAMETERS_FIELD;

public class SparseMethodContextTests extends AbstractSparseTestBase {

    @Mock
    private StreamInput mockStreamInput;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        // The stream constructor branches on the peer version before reading the new fields
        when(mockStreamInput.getVersion()).thenReturn(Version.CURRENT);
    }

    public void testParseWithEmptyName() {
        Map<String, Object> input = new HashMap<>();
        input.put(NAME_FIELD, "");
        input.put(PARAMETERS_FIELD, new HashMap<>());

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> { SparseMethodContext.parse(input); });
        assertEquals("name needs to be set", exception.getMessage());
    }

    public void testParseWithInvalidParameterKey() {
        Map<String, Object> input = new HashMap<>();
        input.put(NAME_FIELD, "testMethod");
        input.put("invalidKey", "someValue");

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> { SparseMethodContext.parse(input); });
        assertEquals("Invalid parameter: invalidKey", exception.getMessage());
    }

    /** It moved into method.parameters, where {@link org.opensearch.neuralsearch.sparse.algorithm.seismic.Seismic} validates it. */
    public void testParseRejectsForwardIndexOutsideParameters() {
        Map<String, Object> input = new HashMap<>();
        input.put(NAME_FIELD, "testMethod");
        input.put(FORWARD_INDEX_FIELD, SparseForwardIndex.PER_BLOCK.getName());

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> SparseMethodContext.parse(input));
        assertEquals("Invalid parameter: " + FORWARD_INDEX_FIELD, exception.getMessage());
    }

    public void testParseKeepsForwardIndexAsAMethodParameter() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(FORWARD_INDEX_FIELD, SparseForwardIndex.PER_BLOCK.getName());
        Map<String, Object> input = new HashMap<>();
        input.put(NAME_FIELD, "testMethod");
        input.put(PARAMETERS_FIELD, parameters);

        assertEquals(
            SparseForwardIndex.PER_BLOCK.getName(),
            SparseMethodContext.parse(input).getMethodComponentContext().getParameters().get(FORWARD_INDEX_FIELD)
        );
    }

    public void testParseWithInvalidParametersType() {
        Map<String, Object> input = new HashMap<>();
        input.put(NAME_FIELD, "testMethod");
        input.put(PARAMETERS_FIELD, "Not a map");

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> { SparseMethodContext.parse(input); });
        assertEquals("Unable to parse parameters for main method component", exception.getMessage());
    }

    public void testParseWithNonMapInput() {
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> { SparseMethodContext.parse("Not a map"); });
        assertEquals("Unable to parse mapping into SparseMethodContext. Object not of type \"Map\"", exception.getMessage());
    }

    public void testSparseMethodContextConstructorWithIOException() throws IOException {
        StreamInput mockInput = mock(StreamInput.class);
        when(mockInput.readString()).thenThrow(new IOException("Simulated IO error"));

        expectThrows(IOException.class, () -> { new SparseMethodContext(mockInput); });
    }

    public void testSparseMethodConstructorWithStreamInput() throws IOException {
        when(mockStreamInput.readString()).thenReturn("testMethod");

        SparseMethodContext sparseMethodContext = new SparseMethodContext(mockStreamInput);

        assertEquals("testMethod", sparseMethodContext.getName());
        assertNotNull(sparseMethodContext.getMethodComponentContext());
    }

    public void testParseInvalidParameterAndEmptyName() {
        Map<String, Object> inputMap = new HashMap<>();
        inputMap.put("invalid_key", "some_value");

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> { SparseMethodContext.parse(inputMap); });
        assertEquals("Invalid parameter: invalid_key", exception.getMessage());
    }

    public void testParseNullParameters() {
        Map<String, Object> input = new HashMap<>();
        input.put(NAME_FIELD, "testMethod");
        input.put(PARAMETERS_FIELD, null);

        SparseMethodContext result = SparseMethodContext.parse(input);

        assertEquals("testMethod", result.getName());
        assertTrue(result.getMethodComponentContext().getParameters().isEmpty());
    }

    public void testParseValidNameOnly() {
        Map<String, Object> input = new HashMap<>();
        input.put(NAME_FIELD, "testMethod");

        SparseMethodContext result = SparseMethodContext.parse(input);

        assertEquals("testMethod", result.getName());
        assertEquals(new HashMap<>(), result.getMethodComponentContext().getParameters());
    }

    public void testToXContentSerializesCorrectly() throws IOException {
        String name = "test_method";
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("param1", "value1");
        parameters.put("param2", 42);

        MethodComponentContext methodComponentContext = new MethodComponentContext(name, parameters);
        SparseMethodContext sparseMethodContext = new SparseMethodContext(name, SparseEngine.DEFAULT.getName(), methodComponentContext);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        sparseMethodContext.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String result = builder.toString();
        assertTrue(result.contains("test_method"));
        assertTrue(result.contains("param1"));
        assertTrue(result.contains("value1"));
        assertTrue(result.contains("param2"));
        assertTrue(result.contains("42"));
    }

    /**
     * An index created before this field existed does not have it in the mapping source held in cluster
     * state. MapperService compares a re-serialization against that source and fails the node with an
     * AssertionError when they differ, so emitting the default kills every upgraded node holding one.
     */
    public void testToXContentOmitsDefaultEngine() throws IOException {
        SparseMethodContext context = new SparseMethodContext(
            "seismic",
            SparseEngine.DEFAULT.getName(),
            new MethodComponentContext("seismic", new HashMap<>())
        );

        String result = toXContentString(context);
        assertFalse(result.contains("\"" + ENGINE_FIELD + "\""));
    }

    public void testToXContentEmitsNonDefaultEngine() throws IOException {
        SparseMethodContext context = new SparseMethodContext(
            "seismic",
            SparseEngine.NATIVE.getName(),
            new MethodComponentContext("seismic", new HashMap<>())
        );

        String result = toXContentString(context);
        assertTrue(result.contains("\"" + ENGINE_FIELD + "\":\"" + SparseEngine.NATIVE.getName() + "\""));
    }

    /**
     * The mapping-source comparison is what parse and serialize have to agree on: serializing a
     * context and parsing the result back must land on the same context, whichever fields the
     * original source carried.
     */
    public void testToXContentRoundTripsWhatParseAccepts() throws IOException {
        Map<String, Object> withoutEngine = new HashMap<>();
        withoutEngine.put(NAME_FIELD, "seismic");
        withoutEngine.put(PARAMETERS_FIELD, new HashMap<>());
        Map<String, Object> withEngine = new HashMap<>(withoutEngine);
        withEngine.put(ENGINE_FIELD, SparseEngine.NATIVE.getName());
        withEngine.put(PARAMETERS_FIELD, Map.of(FORWARD_INDEX_FIELD, SparseForwardIndex.PER_BLOCK.getName()));

        for (Map<String, Object> source : List.of(withoutEngine, withEngine)) {
            SparseMethodContext parsed = SparseMethodContext.parse(source);
            Map<String, Object> reserialized = XContentHelper.convertToMap(
                new BytesArray(toXContentString(parsed)),
                false,
                MediaTypeRegistry.JSON
            ).v2();
            assertEquals(parsed, SparseMethodContext.parse(reserialized));
        }
    }

    private String toXContentString(SparseMethodContext context) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        context.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder.toString();
    }

    public void testWriteToAndReadFrom() throws IOException {
        String name = "testMethod";
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("param1", "value1");
        parameters.put("param2", 42);
        MethodComponentContext methodComponentContext = new MethodComponentContext(name, parameters);
        String engine = SparseEngine.DEFAULT.getName();
        SparseMethodContext sparseMethodContext = new SparseMethodContext(name, engine, methodComponentContext);

        BytesStreamOutput out = new BytesStreamOutput();
        sparseMethodContext.writeTo(out);

        BytesReference bytesRef = out.bytes();
        StreamInput in = bytesRef.streamInput();
        SparseMethodContext readContext = new SparseMethodContext(in);

        assertEquals(name, readContext.getName());
        assertEquals(engine, readContext.getSparseEngine());
        assertEquals(methodComponentContext, readContext.getMethodComponentContext());
    }

    public void testNonDefaultEngineAndForwardIndexSurviveRoundTrip() throws IOException {
        SparseMethodContext context = context(SparseEngine.NATIVE.getName(), SparseForwardIndex.PER_BLOCK.getName());

        SparseMethodContext readContext = roundTrip(context, Version.CURRENT);

        assertEquals(SparseEngine.NATIVE.getName(), readContext.getSparseEngine());
        assertEquals(context.getMethodComponentContext(), readContext.getMethodComponentContext());
    }

    /**
     * The context has been Writeable since 3.3, so what a pre-3.9 peer sees has to stay exactly what
     * 3.8 wrote: the name followed straight by the component context, with no optional string in
     * between.
     */
    public void testBytesWrittenToAnOlderPeerMatchTheReleasedFormat() throws IOException {
        SparseMethodContext context = context(SparseEngine.NATIVE.getName(), SparseForwardIndex.PER_BLOCK.getName());

        BytesStreamOutput actual = new BytesStreamOutput();
        actual.setVersion(Version.V_3_8_0);
        context.writeTo(actual);

        BytesStreamOutput expected = new BytesStreamOutput();
        expected.setVersion(Version.V_3_8_0);
        expected.writeString(context.getName());
        context.getMethodComponentContext().writeTo(expected);

        assertEquals(expected.bytes(), actual.bytes());
    }

    public void testReadFromAnOlderPeerFallsBackToDefaults() throws IOException {
        SparseMethodContext context = context(SparseEngine.NATIVE.getName(), SparseForwardIndex.PER_BLOCK.getName());

        SparseMethodContext readContext = roundTrip(context, Version.V_3_8_0);

        // A pre-3.9 node only ever ran the Lucene engine, so that is what its stream means
        assertEquals(SparseEngine.DEFAULT.getName(), readContext.getSparseEngine());
    }

    /**
     * The regression the version guard exists for. Reading an optional string off a pre-3.9 stream
     * swallows the parameter map, and because {@link MethodComponentContext} decides on
     * {@code available() > 0} the loss is silent: the seismic parameters come back null rather than
     * failing.
     */
    public void testReadingAStreamFromAnOlderNodeKeepsTheMethodParameters() throws IOException {
        SparseMethodContext context = context(SparseEngine.DEFAULT.getName(), SparseForwardIndex.DEFAULT.getName());

        // Hand-written in the 3.8 layout rather than round-tripped: only an asymmetric read, which is
        // what a mixed cluster actually does, can consume the map bytes as the optional string.
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_3_8_0);
        out.writeString(context.getName());
        context.getMethodComponentContext().writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_3_8_0);
        SparseMethodContext readContext = new SparseMethodContext(in);

        assertEquals(context.getName(), readContext.getName());
        assertEquals(SparseEngine.DEFAULT.getName(), readContext.getSparseEngine());
        assertEquals(
            Map.of("param1", "value1", "param2", 42, FORWARD_INDEX_FIELD, SparseForwardIndex.DEFAULT.getName()),
            readContext.getMethodComponentContext().getParameters()
        );
    }

    private SparseMethodContext context(String engine, String forwardIndex) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("param1", "value1");
        parameters.put("param2", 42);
        parameters.put(FORWARD_INDEX_FIELD, forwardIndex);
        return new SparseMethodContext("testMethod", engine, new MethodComponentContext("testMethod", parameters));
    }

    /** Serializes and deserializes as if both peers were on {@code version}. */
    private SparseMethodContext roundTrip(SparseMethodContext context, Version version) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(version);
        context.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(version);
        return new SparseMethodContext(in);
    }

    public void testParseWithNonMapParameterValues() {
        Map<String, Object> input = new HashMap<>();
        input.put(NAME_FIELD, "testMethod");

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("stringParam", "stringValue");
        parameters.put("intParam", 42);
        parameters.put("boolParam", true);
        input.put(PARAMETERS_FIELD, parameters);

        SparseMethodContext result = SparseMethodContext.parse(input);

        assertEquals("testMethod", result.getName());
        Map<String, Object> resultParams = result.getMethodComponentContext().getParameters();
        assertEquals("stringValue", resultParams.get("stringParam"));
        assertEquals(42, resultParams.get("intParam"));
        assertEquals(true, resultParams.get("boolParam"));
    }

    public void testParseThrowExceptionWithNestedMap() {
        Map<String, Object> input = new HashMap<>();
        input.put(NAME_FIELD, "testMethod");

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("stringParam", "stringValue");
        parameters.put("intParam", 42);
        parameters.put("boolParam", true);
        parameters.put("mapParam", Map.of("nested", 1));
        input.put(PARAMETERS_FIELD, parameters);

        expectThrows(IllegalArgumentException.class, () -> SparseMethodContext.parse(input));
    }
}
