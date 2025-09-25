/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.quantization;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.similarities.Similarity;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.QUANTIZATION_CEILING_INGEST_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.QUANTIZATION_CEILING_SEARCH_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_QUANTIZATION_CEILING_INGEST;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_QUANTIZATION_CEILING_SEARCH;

/**
 * Utility class for float -> byte quantization
 */
public final class ByteQuantizationUtil {

    private ByteQuantizationUtil() {} // no instance of this utility class

    // Use full precision of byte (0-255)
    public static final int MAX_UNSIGNED_BYTE_VALUE = 255;

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

    /**
     * Rescaling factor to convert quantized dot product scores back to their original float scale.
     * When vector components are quantized from float (0-ceilingValue) to byte (0-255), dot products of these
     * quantized vectors will have a different scale than the original float vectors. This constant
     * provides the exact ratio needed to convert the quantized dot product back to the equivalent
     * float dot product scale.
     * Calculation: (quantizationCeilSearch * quantizationCeilIngest) / (MAX_UNSIGNED_BYTE_VALUE²)
     * This represents the ratio between the product of ceiling value and
     * the square of the maximum byte value (255²), which is the scaling factor needed for
     * dot products of quantized vectors.
     */
    public static Similarity.SimScorer getSimScorer(float boost) {
        return new Similarity.SimScorer() {
            @Override
            public float score(float freq, long norm) {
                return boost * freq;
            }
        };
    }

    /**
     * Get the quantization ceiling value ingest from fieldInfo, which is parsed from MAX_FLOAT_VALUE_INGEST_FIELD
     */
    public static float getCeilingValueIngest(FieldInfo fieldInfo) {
        if (fieldInfo == null) {
            return DEFAULT_QUANTIZATION_CEILING_INGEST;
        }
        String stringValue = fieldInfo.getAttribute(QUANTIZATION_CEILING_INGEST_FIELD);
        return StringUtils.isEmpty(stringValue) ? DEFAULT_QUANTIZATION_CEILING_INGEST : NumberUtils.createFloat(stringValue);
    }

    /**
     * Get the quantization ceiling value search from fieldInfo, which is parsed from DEFAULT_QUANTIZATION_CEILING_SEARCH
     */
    public static float getCeilingValueSearch(FieldInfo fieldInfo) {
        if (fieldInfo == null) {
            return DEFAULT_QUANTIZATION_CEILING_SEARCH;
        }
        String stringValue = fieldInfo.getAttribute(QUANTIZATION_CEILING_SEARCH_FIELD);
        return StringUtils.isEmpty(stringValue) ? DEFAULT_QUANTIZATION_CEILING_SEARCH : NumberUtils.createFloat(stringValue);
    }

    /**
     * Get a byte quantizer object during ingestion
     */
    public static ByteQuantizer getByteQuantizerIngest(FieldInfo fieldInfo) {
        float quantizationCeilIngest = getCeilingValueIngest(fieldInfo);
        return new ByteQuantizer(quantizationCeilIngest);
    }
}
