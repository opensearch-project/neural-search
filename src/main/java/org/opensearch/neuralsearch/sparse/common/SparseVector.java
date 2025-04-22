/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Sparse vector implementation, which is a list of (token, freq) pairs
 */
public class SparseVector implements Iterator<SparseVector.Item> {
    private final int size;
    // tokens will be stored in order
    private int[] tokens;
    private float[] freqs;
    private int current = -1;

    public SparseVector(BytesRef bytesRef) throws IOException {
        this(readToMap(bytesRef));
    }

    public SparseVector(Map<String, Float> pairs) {
        this(pairs.entrySet().stream().map(t -> new Item(convertStringToInteger(t.getKey()), t.getValue())).collect(Collectors.toList()));
    }

    private static Integer convertStringToInteger(String value) {
        return value.hashCode();
    }

    private static Integer convertFloatToInteger(Float value) {
        int freqBits = Float.floatToIntBits(value);
        return freqBits >>> 15;
    }

    public SparseVector(List<Item> items) {
        items.sort((o1, o2) -> o1.getToken() - o2.getToken());
        this.size = items.size();
        this.tokens = new int[this.size];
        this.freqs = new float[this.size];
        for (int i = 0; i < this.size; ++i) {
            this.tokens[i] = items.get(i).getToken();
            this.freqs[i] = items.get(i).getFreq();
        }
    }

    private static Map<String, Float> readToMap(BytesRef bytesRef) {
        Map<String, Float> map = new HashMap<>();
        try (
            ByteArrayInputStream bais = new ByteArrayInputStream(
                ArrayUtil.copyOfSubArray(bytesRef.bytes, bytesRef.offset, bytesRef.length)
            );
            ObjectInputStream objectInputStream = new ObjectInputStream(bais)
        ) {
            int available = bais.available();
            while (bais.available() > 0) {
                String key = (String) objectInputStream.readObject();
                available = bais.available();
                float value = objectInputStream.readFloat();
                available = bais.available();
                map.put(key, value);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return map;
    }

    public float dotProduct(final SparseVector vector) {
        int iA = 0;
        int iB = 0;
        float score = 0.0f;
        while (iA < this.size && iB < vector.size) {
            int tokenA = this.tokens[iA];
            int tokenB = vector.tokens[iB];
            if (tokenA == tokenB) {
                score += this.freqs[iA] * vector.freqs[iB];
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

    @Override
    public boolean hasNext() {
        return this.size > 0 && this.current + 1 < this.size;
    }

    @Override
    public Item next() {
        ++this.current;
        return Item.of(this.tokens[this.current], this.freqs[this.current]);
    }

    public void reset() {
        this.current = -1;
    }

    @AllArgsConstructor
    @Getter
    public static class Item {
        int token;
        float freq;

        static Item of(int token, float freq) {
            return new Item(token, freq);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < this.size; ++i) {
            sb.append(this.tokens[i]).append(":").append(this.freqs[i]).append(" ");
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        long result = 0;
        for (int i = 0; i < this.size; ++i) {
            result += (long) (this.tokens[i] * this.freqs[i]);
        }
        return (int) result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj instanceof SparseVector) {
            SparseVector vector = (SparseVector) obj;
            if (this.size != vector.size) {
                return false;
            }
            for (int i = 0; i < this.size; ++i) {
                if (this.tokens[i] != vector.tokens[i] || this.freqs[i] != vector.freqs[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
