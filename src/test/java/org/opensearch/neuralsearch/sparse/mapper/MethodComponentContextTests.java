/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.mapper;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MethodComponentContextTests extends AbstractSparseTestBase {

    private static final String NAME = "test_name";

    public void testConstructorWithNullContext() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new MethodComponentContext((MethodComponentContext) null);
        });
        assertNotNull(exception);
    }

    public void testConstructorWithNullParameter() {
        MethodComponentContext contextInput = new MethodComponentContext(NAME, null);
        MethodComponentContext context = new MethodComponentContext(contextInput);
        assertTrue(context.getParameters().isEmpty());
    }

    public void testEqualsWithDifferentClass() {
        MethodComponentContext context = new MethodComponentContext(NAME, new HashMap<>());
        assertFalse(context.equals(new Object()));
    }

    public void testEqualsWithDifferentName() {
        MethodComponentContext context1 = new MethodComponentContext("test1", new HashMap<>());
        MethodComponentContext context2 = new MethodComponentContext("test2", new HashMap<>());
        assertFalse(context1.equals(context2));
    }

    public void testEqualsWithDifferentParameters() {
        Map<String, Object> params1 = new HashMap<>();
        params1.put("key", "value1");
        Map<String, Object> params2 = new HashMap<>();
        params2.put("key", "value2");

        MethodComponentContext context1 = new MethodComponentContext(NAME, params1);
        MethodComponentContext context2 = new MethodComponentContext(NAME, params2);
        assertFalse(context1.equals(context2));
    }

    public void testEqualsWithNull() {
        MethodComponentContext context = new MethodComponentContext(NAME, new HashMap<>());
        assertFalse(context.equals(null));
    }

    public void testGetFloatWithNonNumberValue() {
        MethodComponentContext context = new MethodComponentContext(NAME, Map.of("key", "not a number"));
        Float defaultValue = 1.0f;
        Float result = context.getFloat("key", defaultValue);
        assertEquals(defaultValue, result);
    }

    public void testMethodComponentContextWithEmptyStreamInput() throws IOException {
        StreamInput mockStreamInput = mock(StreamInput.class);
        when(mockStreamInput.available()).thenReturn(0);

        MethodComponentContext context = new MethodComponentContext(mockStreamInput, NAME);
        assertTrue(context.getParameters().isEmpty());
    }

    public void testToXContentWithNullParameters() throws IOException {
        MethodComponentContext context = new MethodComponentContext(NAME, null);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        context.toXContent(builder, null);
        builder.endObject();

        String expected = "{\"name\":\"" + NAME + "\",\"parameters\":null}";
        assertEquals(expected, builder.toString());
    }

    public void testToXContentWithIOExceptionHandling() throws IOException {
        // Create a MethodComponentContext with a parameter that will cause an IOException
        String name = "errorMethod";
        Map<String, Object> params = new HashMap<>();

        // Create a mock MethodComponentContext that throws IOException when toXContent is called
        MethodComponentContext problematicNestedContext = mock(MethodComponentContext.class);
        doThrow(new IOException("Test IOException")).when(problematicNestedContext).toXContent(any(), any());

        params.put("problematicContext", problematicNestedContext);
        MethodComponentContext context = new MethodComponentContext(name, params);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        // The toXContent method should throw a RuntimeException wrapping the IOException
        RuntimeException exception = assertThrows(RuntimeException.class, () -> context.toXContent(builder, null));

        // Verify the exception message
        assertEquals("Unable to generate xcontent for method component", exception.getMessage());

        builder.endObject();
    }

    public void testCopyConstructorWithNonMethodComponentContextParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("param1", "value1");
        parameters.put("param2", 42);

        MethodComponentContext original = new MethodComponentContext(NAME, parameters);
        MethodComponentContext copy = new MethodComponentContext(original);

        assertNotNull(copy);
        assertEquals(NAME, copy.getName());
        assertNotNull(copy.getParameters());
        assertEquals(2, copy.getParameters().size());
        assertEquals("value1", copy.getParameters().get("param1"));
        assertEquals(42, copy.getParameters().get("param2"));
    }

    public void testEqualsIdenticalObjects() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("param1", "value1");
        parameters.put("param2", 42);

        MethodComponentContext context1 = new MethodComponentContext(NAME, parameters);
        MethodComponentContext context2 = new MethodComponentContext(NAME, parameters);

        assertTrue(context1.equals(context2));
        assertTrue(context2.equals(context1));
    }

    public void testGetFloatWhenParameterIsNumber() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("testKey", 42);
        MethodComponentContext context = new MethodComponentContext(NAME, parameters);

        Float result = context.getFloat("testKey", 0.0f);

        assertEquals(42.0f, result, 0.001f);
    }

    public void testGetParameterWhenKeyExists() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("testKey", "testValue");
        MethodComponentContext context = new MethodComponentContext(NAME, parameters);

        Object result = context.getParameter("testKey", "defaultValue");

        assertEquals("testValue", result);
    }

    public void testGetParametersWhenParametersIsNull() {
        MethodComponentContext context = new MethodComponentContext(NAME, null);
        Map<String, Object> result = context.getParameters();
        assertEquals(Collections.emptyMap(), result);
    }

    public void testHashCode() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("key1", "value1");
        parameters.put("key2", 42);

        MethodComponentContext context = new MethodComponentContext(NAME, parameters);

        int expectedHashCode = new HashCodeBuilder().append(NAME).append(parameters).toHashCode();
        int actualHashCode = context.hashCode();

        assertEquals(expectedHashCode, actualHashCode);
    }

    public void testHashCodeDifferentNamesEqualParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("key", "value");

        MethodComponentContext context1 = new MethodComponentContext("name1", parameters);
        MethodComponentContext context2 = new MethodComponentContext("name2", parameters);

        assertNotEquals(context1.hashCode(), context2.hashCode());
    }

    public void testWriteToAndReadFrom() throws IOException {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("param1", "value1");
        parameters.put("param2", 42);

        MethodComponentContext context = new MethodComponentContext(NAME, parameters);

        BytesStreamOutput out = new BytesStreamOutput();
        context.writeTo(out);

        BytesReference bytesRef = out.bytes();
        StreamInput in = bytesRef.streamInput();
        MethodComponentContext readContext = new MethodComponentContext(in, NAME);

        assertEquals(NAME, readContext.getName());
        assertEquals(parameters, readContext.getParameters());
    }

    public void testWriteToWithNullParameters() throws IOException {
        MethodComponentContext context = new MethodComponentContext(NAME, null);

        BytesStreamOutput out = new BytesStreamOutput();
        context.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        assertEquals(0, in.available());
    }
}
