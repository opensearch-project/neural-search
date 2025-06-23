/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.apache.lucene.search.similarities.Similarity;

public final class ByteQuantizer {

    // The maximum float value to consider
    private static final float MAX_FLOAT_VALUE = 3.0f;
    // Use full precision of byte (0-255)
    private static final float MAX_UNSIGNED_BYTE_VALUE = 255.0f;
    /**
     * Rescaling factor to convert quantized dot product scores back to their original float scale.
     * When vector components are quantized from float (0-3.0) to byte (0-255), dot products of these
     * quantized vectors will have a different scale than the original float vectors. This constant
     * provides the exact ratio needed to convert the quantized dot product back to the equivalent
     * float dot product scale.
     * Calculation: (MAX_FLOAT_VALUE²) / (MAX_UNSIGNED_BYTE_VALUE²)
     * This represents the ratio between the square of the maximum float value (3.0²) and
     * the square of the maximum byte value (255²), which is the scaling factor needed for
     * dot products of quantized vectors.
     */
    public static final float SCORE_RESCALE_RATIO = MAX_FLOAT_VALUE * MAX_FLOAT_VALUE / MAX_UNSIGNED_BYTE_VALUE / MAX_UNSIGNED_BYTE_VALUE;

    private ByteQuantizer() {} // no instance of this utility class

    /**
     * Maps a positive float value to an unsigned integer within the range of the specified type.
     *
     * @param value The float value to map
     * @return The mapped unsigned integer value
     */
    public static byte quantizeFloatToByte(float value) {
        // Ensure the value is within the specified range
        value = Math.max(0.0f, Math.min(MAX_FLOAT_VALUE, value));

        // Scale the value to fit in the byte range (0-255)
        // Note: In Java, byte is signed (-128 to 127), but we'll use the full precision
        value = (value * MAX_UNSIGNED_BYTE_VALUE) / MAX_FLOAT_VALUE;

        // Round to nearest integer and cast to byte
        return (byte) Math.round(value);
    }

    /**
     * Overloaded method to get unsigned frequency directly from a byte
     * @param value The byte value
     * @return The unsigned integer value of the frequency
     */
    public static int getUnsignedByte(byte value) {
        return value & 0xFF;
    }

    /**
     * Compares two bytes as if they were unsigned values (0-255).
     * This is necessary because Java bytes are signed (-128 to 127).
     *
     * @param x First byte to compare
     * @param y Second byte to compare
     * @return -1 if x is smaller than y in unsigned comparison, 0 if equal, 1 if x is greater than y
     */
    public static int compareUnsignedByte(byte x, byte y) {
        // Convert to unsigned integers (0-255) and compare directly
        // This is more efficient than the branching approach
        return (x & 0xFF) - (y & 0xFF);
    }

    /**
     * Multiplies two bytes as unsigned values (0-255).
     * This method treats the input bytes as unsigned values by masking with 0xFF,
     * then performs multiplication on the resulting integers.
     *
     * @param x First byte to be treated as unsigned (0-255)
     * @param y Second byte to be treated as unsigned (0-255)
     * @return The product of the two unsigned byte values as an integer (0-65025)
     */
    public static int multiplyUnsignedByte(byte x, byte y) {
        return (x & 0xFF) * (y & 0xFF);
    }

    public static Similarity.SimScorer getSimScorer(float boost) {
        return new Similarity.SimScorer() {
            @Override
            public float score(float freq, long norm) {
                return boost * freq * SCORE_RESCALE_RATIO;
            }
        };
    }
}
