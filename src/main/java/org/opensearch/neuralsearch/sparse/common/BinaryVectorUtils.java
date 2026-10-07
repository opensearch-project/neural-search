/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.apache.lucene.util.BytesRef;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decodes a sparse vector from the binary doc value it is stored as: a flat sequence of
 * (token id, weight) pairs, each a big-endian {@code int} followed by a {@code float}, in the order
 * the parser produced them rather than sorted by token.
 */
public class BinaryVectorUtils {
    /**
     * Reads the vector into a token-to-weight map. A repeated token keeps its last weight.
     *
     * @param bytesRef the encoded vector
     * @return the decoded pairs, unordered
     */
    public static Map<Integer, Float> readToMap(BytesRef bytesRef) throws IOException {
        Map<Integer, Float> map = new HashMap<>();
        // Windowed rather than copied: ArrayUtil.copyOfSubArray takes (from, to), so passing the
        // length as the second argument truncated any BytesRef whose offset was not 0.
        try (
            ByteArrayInputStream bais = new ByteArrayInputStream(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            DataInputStream dis = new DataInputStream(bais)
        ) {
            while (bais.available() > 0) {
                int key = dis.readInt();
                float value = dis.readFloat();
                map.put(key, value);
            }
        }
        return map;
    }

    /**
     * Appends the vector to two parallel lists, preserving the encoded order and any duplicate
     * tokens. Used where the pairs feed a CSR buffer and the map's rehashing would be wasted.
     *
     * @param bytesRef the encoded vector
     * @param keys     receives the token ids
     * @param values   receives the weights, index-aligned with {@code keys}
     */
    public static void readToList(BytesRef bytesRef, List<Integer> keys, List<Float> values) throws IOException {
        // Windowed rather than copied: ArrayUtil.copyOfSubArray takes (from, to), so passing the
        // length as the second argument truncated any BytesRef whose offset was not 0.
        try (
            ByteArrayInputStream bais = new ByteArrayInputStream(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            DataInputStream dis = new DataInputStream(bais)
        ) {
            while (bais.available() > 0) {
                int key = dis.readInt();
                float value = dis.readFloat();
                keys.add(key);
                values.add(value);
            }
        }
    }
}
