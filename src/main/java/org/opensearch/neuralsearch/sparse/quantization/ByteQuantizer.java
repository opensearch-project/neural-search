/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.quantization;

import static org.opensearch.neuralsearch.sparse.quantization.ByteQuantizerUtil.MAX_UNSIGNED_BYTE_VALUE;

/**
 * Maps a positive float value to an unsigned integer within the range of the specified type.
 * The function scales values from the range [0, ceilValue] to [0, 255].
 */
public final class ByteQuantizer {

    private final float ceilValue;

    public ByteQuantizer(float ceilValue) {
        if (ceilValue < 0) {
            throw new IllegalArgumentException("Ceiling value must be positive for byte quantizer");
        }
        this.ceilValue = ceilValue;
    }

    public byte quantize(float value) {
        // Ensure the value is within the specified range
        value = Math.max(0.0f, Math.min(ceilValue, value));

        // Scale the value to fit in the byte range (0-255)
        // Note: In Java, byte is signed (-128 to 127), but we'll use the full precision
        value = (value * MAX_UNSIGNED_BYTE_VALUE) / ceilValue;

        // Round to nearest integer and cast to byte
        return (byte) Math.round(value);
    }
}
