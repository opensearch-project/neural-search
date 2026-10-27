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
     * A token at or above {@code upperBound} is folded to {@code token % upperBound}. Once any token
     * in the vector is folded, entries sharing a token are merged into the first one by max weight,
     * the same rule the query side applies, so the index never holds two weights for one term.
     *
     * @param bytesRef   the encoded vector
     * @param keys       receives the token ids, each below {@code upperBound}
     * @param values     receives the weights, index-aligned with {@code keys}
     * @param upperBound the exclusive limit on token ids the caller can store
     */
    public static void readToList(BytesRef bytesRef, List<Integer> keys, List<Float> values, int upperBound) throws IOException {
        int start = keys.size();
        boolean folded = false;
        // Windowed rather than copied: ArrayUtil.copyOfSubArray takes (from, to), so passing the
        // length as the second argument truncated any BytesRef whose offset was not 0.
        try (
            ByteArrayInputStream bais = new ByteArrayInputStream(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            DataInputStream dis = new DataInputStream(bais)
        ) {
            while (bais.available() > 0) {
                int key = dis.readInt();
                float value = dis.readFloat();
                if (key >= upperBound) {
                    key %= upperBound;
                    folded = true;
                }
                keys.add(key);
                values.add(value);
            }
        }
        if (folded) {
            mergeByMax(keys, values, start);
        }
    }

    /**
     * Collapses repeated tokens in {@code keys[start..]} in place, keeping first-seen order.
     *
     * Must keep the weight {@code SparseVector.foldTokens} keeps when it folds the query for the
     * native engine: the larger float. Both sides are quantized only afterwards, by nsparse, so a
     * folded id is weighted alike in the index and in the query.
     */
    private static void mergeByMax(List<Integer> keys, List<Float> values, int start) {
        Map<Integer, Integer> slotByKey = new HashMap<>();
        int write = start;
        for (int read = start; read < keys.size(); read++) {
            int key = keys.get(read);
            float value = values.get(read);
            Integer slot = slotByKey.putIfAbsent(key, write);
            if (slot == null) {
                keys.set(write, key);
                values.set(write, value);
                write++;
            } else {
                values.set(slot, Math.max(values.get(slot), value));
            }
        }
        keys.subList(write, keys.size()).clear();
        values.subList(write, values.size()).clear();
    }
}
