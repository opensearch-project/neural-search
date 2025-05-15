/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

public class ValueEncoder {
    static final int MAX_FREQ = Float.floatToIntBits(Float.MAX_VALUE) >>> 15;

    public static int encodeFeatureValue(float featureValue) {
        int freqBits = Float.floatToIntBits(featureValue);
        return freqBits >>> 15;
    }

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
