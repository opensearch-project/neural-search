/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.apache.lucene.util.BytesRef;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;
import java.util.TreeMap;

public class SparseVector {
    private final int size;
    // tokens will be stored in order
    private int[] tokens;
    private float[] scores;

    public SparseVector(BytesRef bytesRef) throws IOException {
        Map<Integer, Float> map = new TreeMap<>();
        try (
            ByteArrayInputStream bais = new ByteArrayInputStream(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            ObjectInputStream objectInputStream = new ObjectInputStream(bais)
        ) {
            String key = (String) objectInputStream.readObject();
            float value = objectInputStream.readFloat();
            map.put(key.hashCode(), value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.size = map.size();
        this.tokens = new int[this.size];
        this.scores = new float[this.size];
        int i = 0;
        for (Map.Entry<Integer, Float> entry : map.entrySet()) {
            this.tokens[i] = entry.getKey();
            this.scores[i] = entry.getValue();
            ++i;
        }
    }

    public float dotProduct(final SparseVector vector) {
        int iA = 0;
        int iB = 0;
        float score = 0;
        while (iA < this.size && iB < vector.size) {
            int tokenA = this.tokens[iA];
            int tokenB = vector.tokens[iB];
            if (tokenA == tokenB) {
                score += this.scores[iA] * vector.scores[iB];
                ++iA;
                ++iB;
            } else if (tokenA < tokenB) {
                ++iA;
            } else {
                ++iB;
            }
        }
        return score;
    }
}
