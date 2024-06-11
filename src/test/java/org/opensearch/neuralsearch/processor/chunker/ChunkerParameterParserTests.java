/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import org.junit.Assert;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Locale;
import java.util.Map;

public class ChunkerParameterParserTests extends OpenSearchTestCase {

    private static final String fieldName = "parameter";
    private static final String defaultString = "default_string";
    private static final Integer defaultInteger = 0;
    private static final Integer defaultPositiveInteger = 100;
    private static final Double defaultDouble = 0.0;

    public void testParseString_withFieldValueNotString_thenFail() {
        Map<String, Object> parameters = Map.of(fieldName, 1);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> ChunkerParameterParser.parseString(parameters, fieldName)
        );
        Assert.assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", fieldName, String.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseString_withFieldValueEmptyString_thenFail() {
        Map<String, Object> parameters = Map.of(fieldName, "");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> ChunkerParameterParser.parseString(parameters, fieldName)
        );
        Assert.assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] should not be empty.", fieldName),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseString_withFieldValueValidString_thenSucceed() {
        String parameterValue = "string_parameter_value";
        Map<String, Object> parameters = Map.of(fieldName, parameterValue);
        String parsedStringValue = ChunkerParameterParser.parseString(parameters, fieldName);
        assertEquals(parameterValue, parsedStringValue);
    }

    public void testParseStringWithDefault_withFieldValueNotString_thenFail() {
        Map<String, Object> parameters = Map.of(fieldName, 1);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> ChunkerParameterParser.parseStringWithDefault(parameters, fieldName, defaultString)
        );
        Assert.assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", fieldName, String.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseStringWithDefault_withFieldValueEmptyString_thenFail() {
        Map<String, Object> parameters = Map.of(fieldName, "");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> ChunkerParameterParser.parseStringWithDefault(parameters, fieldName, defaultString)
        );
        Assert.assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] should not be empty.", fieldName),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseStringWithDefault_withFieldValueValidString_thenSucceed() {
        String parameterValue = "string_parameter_value";
        Map<String, Object> parameters = Map.of(fieldName, parameterValue);
        String parsedStringValue = ChunkerParameterParser.parseStringWithDefault(parameters, fieldName, defaultString);
        assertEquals(parameterValue, parsedStringValue);
    }

    public void testParseStringWithDefault_withFieldValueMissing_thenSucceed() {
        Map<String, Object> parameters = Map.of();
        String parsedStringValue = ChunkerParameterParser.parseStringWithDefault(parameters, fieldName, defaultString);
        assertEquals(defaultString, parsedStringValue);
    }

    public void testParseInteger_withFieldValueString_thenFail() {
        Map<String, Object> parameters = Map.of(fieldName, "a");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> ChunkerParameterParser.parseInteger(parameters, fieldName)
        );
        Assert.assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", fieldName, Integer.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseInteger_withFieldValueDouble_thenFail() {
        Map<String, Object> parameters = Map.of(fieldName, "1.0");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> ChunkerParameterParser.parseInteger(parameters, fieldName)
        );
        Assert.assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", fieldName, Integer.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseInteger_withFieldValueValidInteger_thenSucceed() {
        String parameterValue = "1";
        Integer expectedIntegerValue = 1;
        Map<String, Object> parameters = Map.of(fieldName, parameterValue);
        Integer parsedIntegerValue = ChunkerParameterParser.parseInteger(parameters, fieldName);
        assertEquals(expectedIntegerValue, parsedIntegerValue);
    }

    public void testParseIntegerWithDefault_withFieldValueString_thenFail() {
        Map<String, Object> parameters = Map.of(fieldName, "a");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> ChunkerParameterParser.parseIntegerWithDefault(parameters, fieldName, defaultInteger)
        );
        Assert.assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", fieldName, Integer.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseIntegerWithDefault_withFieldValueDouble_thenFail() {
        Map<String, Object> parameters = Map.of(fieldName, "1.0");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> ChunkerParameterParser.parseIntegerWithDefault(parameters, fieldName, defaultInteger)
        );
        Assert.assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", fieldName, Integer.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseIntegerWithDefault_withFieldValueValidInteger_thenSucceed() {
        String parameterValue = "1";
        Integer expectedIntegerValue = 1;
        Map<String, Object> parameters = Map.of(fieldName, parameterValue);
        Integer parsedIntegerValue = ChunkerParameterParser.parseIntegerWithDefault(parameters, fieldName, defaultInteger);
        assertEquals(expectedIntegerValue, parsedIntegerValue);
    }

    public void testParseIntegerWithDefault_withFieldValueMissing_thenSucceed() {
        Map<String, Object> parameters = Map.of();
        Integer parsedIntegerValue = ChunkerParameterParser.parseIntegerWithDefault(parameters, fieldName, defaultInteger);
        assertEquals(defaultInteger, parsedIntegerValue);
    }

    public void testParsePositiveInteger_withFieldValueString_thenFail() {
        Map<String, Object> parameters = Map.of(fieldName, "a");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> ChunkerParameterParser.parsePositiveInteger(parameters, fieldName)
        );
        Assert.assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", fieldName, Integer.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testParsePositiveInteger_withFieldValueDouble_thenFail() {
        Map<String, Object> parameters = Map.of(fieldName, "1.0");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> ChunkerParameterParser.parsePositiveInteger(parameters, fieldName)
        );
        Assert.assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", fieldName, Integer.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testParsePositiveInteger_withFieldValueNegativeInteger_thenFail() {
        String parameterValue = "-1";
        Map<String, Object> parameters = Map.of(fieldName, parameterValue);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> ChunkerParameterParser.parsePositiveInteger(parameters, fieldName)
        );
        Assert.assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be positive.", fieldName),
            illegalArgumentException.getMessage()
        );
    }

    public void testParsePositiveInteger_withFieldValueValidInteger_thenSucceed() {
        String parameterValue = "1";
        Integer expectedIntegerValue = 1;
        Map<String, Object> parameters = Map.of(fieldName, parameterValue);
        Integer parsedIntegerValue = ChunkerParameterParser.parsePositiveInteger(parameters, fieldName);
        assertEquals(expectedIntegerValue, parsedIntegerValue);
    }

    public void testParsePositiveIntegerWithDefault_withFieldValueString_thenFail() {
        Map<String, Object> parameters = Map.of(fieldName, "a");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> ChunkerParameterParser.parsePositiveIntegerWithDefault(parameters, fieldName, defaultPositiveInteger)
        );
        Assert.assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", fieldName, Integer.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testParsePositiveIntegerWithDefault_withFieldValueDouble_thenFail() {
        Map<String, Object> parameters = Map.of(fieldName, "1.0");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> ChunkerParameterParser.parsePositiveIntegerWithDefault(parameters, fieldName, defaultPositiveInteger)
        );
        Assert.assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", fieldName, Integer.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testParsePositiveIntegerWithDefault_withFieldValueNegativeInteger_thenFail() {
        String parameterValue = "-1";
        Map<String, Object> parameters = Map.of(fieldName, parameterValue);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> ChunkerParameterParser.parsePositiveIntegerWithDefault(parameters, fieldName, defaultPositiveInteger)
        );
        Assert.assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be positive.", fieldName),
            illegalArgumentException.getMessage()
        );
    }

    public void testParsePositiveIntegerWithDefault_withFieldValueValidInteger_thenSucceed() {
        String parameterValue = "1";
        Integer expectedIntegerValue = 1;
        Map<String, Object> parameters = Map.of(fieldName, parameterValue);
        Integer parsedIntegerValue = ChunkerParameterParser.parsePositiveIntegerWithDefault(parameters, fieldName, defaultPositiveInteger);
        assertEquals(expectedIntegerValue, parsedIntegerValue);
    }

    public void testParsePositiveIntegerWithDefault_withFieldValueMissing_thenSucceed() {
        Map<String, Object> parameters = Map.of();
        Integer parsedIntegerValue = ChunkerParameterParser.parsePositiveIntegerWithDefault(parameters, fieldName, defaultPositiveInteger);
        assertEquals(defaultPositiveInteger, parsedIntegerValue);
    }

    public void testParseDouble_withFieldValueString_thenFail() {
        Map<String, Object> parameters = Map.of(fieldName, "a");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> ChunkerParameterParser.parseDouble(parameters, fieldName)
        );
        Assert.assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", fieldName, Double.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseDouble_withFieldValueValidDouble_thenSucceed() {
        String parameterValue = "1";
        Double expectedDoubleValue = 1.0;
        Map<String, Object> parameters = Map.of(fieldName, parameterValue);
        Double parsedDoubleValue = ChunkerParameterParser.parseDouble(parameters, fieldName);
        assertEquals(expectedDoubleValue, parsedDoubleValue);
    }

    public void testParseDoubleWithDefault_withFieldValueString_thenFail() {
        Map<String, Object> parameters = Map.of(fieldName, "a");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> ChunkerParameterParser.parseDoubleWithDefault(parameters, fieldName, defaultDouble)
        );
        Assert.assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", fieldName, Double.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseDoubleWithDefault_withFieldValueValidDouble_thenSucceed() {
        String parameterValue = "1";
        Double expectedDoubleValue = 1.0;
        Map<String, Object> parameters = Map.of(fieldName, parameterValue);
        Double parsedDoubleValue = ChunkerParameterParser.parseDoubleWithDefault(parameters, fieldName, defaultDouble);
        assertEquals(expectedDoubleValue, parsedDoubleValue);
    }

    public void testParseDoubleWithDefault_withFieldValueMissing_thenSucceed() {
        Map<String, Object> parameters = Map.of();
        Double parsedDoubleValue = ChunkerParameterParser.parseDoubleWithDefault(parameters, fieldName, defaultDouble);
        assertEquals(defaultDouble, parsedDoubleValue);
    }
}
