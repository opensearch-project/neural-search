/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.mapper;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
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
        SparseMethodContext sparseMethodContext = new SparseMethodContext(name, methodComponentContext);

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

    public void testWriteToAndReadFrom() throws IOException {
        String name = "testMethod";
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("param1", "value1");
        parameters.put("param2", 42);
        MethodComponentContext methodComponentContext = new MethodComponentContext(name, parameters);
        SparseMethodContext sparseMethodContext = new SparseMethodContext(name, methodComponentContext);

        BytesStreamOutput out = new BytesStreamOutput();
        sparseMethodContext.writeTo(out);

        BytesReference bytesRef = out.bytes();
        StreamInput in = bytesRef.streamInput();
        SparseMethodContext readContext = new SparseMethodContext(in);

        assertEquals(name, readContext.getName());
        assertEquals(methodComponentContext, readContext.getMethodComponentContext());
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
