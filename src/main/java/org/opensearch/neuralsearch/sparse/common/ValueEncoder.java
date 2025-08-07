/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

/**
 * Utility class for encoding and decoding feature values in sparse vectors.
 * Uses bit manipulation to compress float values by shifting bits.
 */
public class ValueEncoder {
    /** Maximum frequency value after bit shifting. */
    static final int MAX_FREQ = Float.floatToIntBits(Float.MAX_VALUE) >>> 15;

    /**
     * Encodes a feature value by converting to int bits and right-shifting by 15 bits.
     *
     * @param featureValue the float feature value to encode
     * @return the encoded integer value
     */
    public static int encodeFeatureValue(float featureValue) {
        int freqBits = Float.floatToIntBits(featureValue);
        return freqBits >>> 15;
    }

    /**
     * Decodes a frequency value back to the original feature value by left-shifting by 15 bits.
     *
     * @param freq the frequency value to decode
     * @return the decoded float feature value
     */
    public static float decodeFeatureValue(float freq) {
        if (freq > MAX_FREQ) {
            // This is never used in practice but callers of the SimScorer API might
            // occasionally call it on eg. Float.MAX_VALUE to compute the max score
            // so we need to be consistent.
            return Float.MAX_VALUE;
        }
        int tf = (int) freq; // lossless
        int featureBits = tf << 15;
        return Float.intBitsToFloat(featureBits);
    }
}
