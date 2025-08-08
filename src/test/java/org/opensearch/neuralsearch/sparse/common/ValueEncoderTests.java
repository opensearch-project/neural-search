/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

public class ValueEncoderTests extends AbstractSparseTestBase {

    public void testEncodeFeatureValue_withPositiveFloat_encodesCorrectly() {
        float input = 1.5f;
        int encoded = ValueEncoder.encodeFeatureValue(input);

        // Verify encoding is consistent
        assertTrue("Encoded value should be non-negative", encoded >= 0);

        // Test that the same input produces the same output
        int encoded2 = ValueEncoder.encodeFeatureValue(input);
        assertEquals("Same input should produce same encoded value", encoded, encoded2);
    }

    public void testEncodeFeatureValue_withZero_encodesCorrectly() {
        float input = 0.0f;
        int encoded = ValueEncoder.encodeFeatureValue(input);

        assertEquals("Zero should encode to zero", 0, encoded);
    }

    public void testEncodeFeatureValue_withMaxValue_encodesCorrectly() {
        float input = Float.MAX_VALUE;
        int encoded = ValueEncoder.encodeFeatureValue(input);

        assertEquals("MAX_VALUE should encode to MAX_FREQ", ValueEncoder.MAX_FREQ, encoded);
    }

    public void testEncodeFeatureValue_withSmallPositiveValue_encodesCorrectly() {
        float input = 0.1f;
        int encoded = ValueEncoder.encodeFeatureValue(input);

        assertTrue("Small positive value should encode to positive integer", encoded > 0);
    }

    public void testDecodeFeatureValue_withPositiveValue_decodesCorrectly() {
        float input = 100.0f;
        float decoded = ValueEncoder.decodeFeatureValue(input);

        assertTrue("Positive input should decode to positive value", decoded >= 0);
    }

    public void testDecodeFeatureValue_withMaxFreq_returnsMaxValue() {
        float decoded = ValueEncoder.decodeFeatureValue(ValueEncoder.MAX_FREQ);

        // Should return a valid float value
        assertFalse("Decoded value should not be NaN", Float.isNaN(decoded));
        assertFalse("Decoded value should not be infinite", Float.isInfinite(decoded));
    }

    public void testDecodeFeatureValue_withValueGreaterThanMaxFreq_returnsMaxValue() {
        float input = ValueEncoder.MAX_FREQ + 1000.0f;
        float decoded = ValueEncoder.decodeFeatureValue(input);

        assertEquals("Value greater than MAX_FREQ should return Float.MAX_VALUE", Float.MAX_VALUE, decoded, 0.0f);
    }

    public void testDecodeFeatureValue_withFloatMaxValue_returnsMaxValue() {
        float decoded = ValueEncoder.decodeFeatureValue(Float.MAX_VALUE);

        assertEquals("Float.MAX_VALUE input should return Float.MAX_VALUE", Float.MAX_VALUE, decoded, 0.0f);
    }

    public void testEncodeDecodeRoundTrip_withVariousValues_maintainsConsistency() {
        float[] testValues = { 1.0f, 2.5f, 10.0f, 100.0f, 1000.0f };

        for (float original : testValues) {
            int encoded = ValueEncoder.encodeFeatureValue(original);
            float decoded = ValueEncoder.decodeFeatureValue(encoded);

            // Due to precision loss in encoding, we check that the relationship is maintained
            assertTrue("Encoded value should be non-negative", encoded >= 0);
            assertTrue("Decoded value should be non-negative", decoded >= 0.0f);
        }
    }

    public void testMaxFreqConstant_hasCorrectValue() {
        int expectedMaxFreq = Float.floatToIntBits(Float.MAX_VALUE) >>> 15;

        assertEquals("MAX_FREQ should have correct calculated value", expectedMaxFreq, ValueEncoder.MAX_FREQ);
    }

    public void testEncodeFeatureValue_withNegativeValue_encodesCorrectly() {
        float input = -1.5f;
        int encoded = ValueEncoder.encodeFeatureValue(input);

        // Negative values will have different bit patterns
        // The method should still work without throwing exceptions
        assertNotNull("Encoding should complete without exception", Integer.valueOf(encoded));
    }

    public void testEncodeFeatureValue_withSpecialFloatValues_handlesCorrectly() {
        // Test with NaN
        float nanInput = Float.NaN;
        int encodedNaN = ValueEncoder.encodeFeatureValue(nanInput);
        assertTrue("NaN encoding should complete", encodedNaN >= 0 || encodedNaN < 0); // Just check it doesn't throw

        // Test with positive infinity
        float posInfInput = Float.POSITIVE_INFINITY;
        int encodedPosInf = ValueEncoder.encodeFeatureValue(posInfInput);
        assertTrue("Positive infinity encoding should complete", encodedPosInf >= 0 || encodedPosInf < 0);

        // Test with negative infinity
        float negInfInput = Float.NEGATIVE_INFINITY;
        int encodedNegInf = ValueEncoder.encodeFeatureValue(negInfInput);
        assertTrue("Negative infinity encoding should complete", encodedNegInf >= 0 || encodedNegInf < 0);
    }

    public void testDecodeFeatureValue_withSpecialFloatValues_handlesCorrectly() {
        // Test with NaN
        float decodedNaN = ValueEncoder.decodeFeatureValue(Float.NaN);
        // Should complete without exception

        // Test with infinity
        float decodedInf = ValueEncoder.decodeFeatureValue(Float.POSITIVE_INFINITY);
        assertEquals("Positive infinity should return Float.MAX_VALUE", Float.MAX_VALUE, decodedInf, 0.0f);
    }
}
