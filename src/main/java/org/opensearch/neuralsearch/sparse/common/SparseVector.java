/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

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
@EqualsAndHashCode
public class SparseVector implements Accountable {
    private final int size;
    // tokens will be stored in order
    private short[] tokens;
    private float[] freqs;

    public SparseVector(BytesRef bytesRef) throws IOException {
        this(readToMap(bytesRef));
    }

    public SparseVector(Map<String, Float> pairs) {
        this(pairs.entrySet().stream().map(t -> new Item(convertStringToInteger(t.getKey()), t.getValue())).collect(Collectors.toList()));
    }

    private static Integer convertStringToInteger(String value) {
        return NumberUtils.createInteger(value);
    }

    private static Integer convertFloatToInteger(Float value) {
        int freqBits = Float.floatToIntBits(value);
        return freqBits >>> 15;
    }

    public SparseVector(List<Item> items) {
        items.sort((o1, o2) -> o1.getToken() - o2.getToken());
        this.size = items.size();
        this.tokens = new short[this.size];
        this.freqs = new float[this.size];
        for (int i = 0; i < this.size; ++i) {
            this.tokens[i] = (short) items.get(i).getToken();
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
            while (bais.available() > 0) {
                String key = (String) objectInputStream.readObject();
                float value = objectInputStream.readFloat();
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

    public float[] toDenseVector() {
        if (this.size == 0) {
            return new float[0];
        }
        int maxToken = this.tokens[this.size - 1];
        float[] denseVector = new float[maxToken + 1];
        for (int i = 0; i < this.size; ++i) {
            denseVector[this.tokens[i]] = this.freqs[i];
        }
        return denseVector;
    }

    public float dotProduct(final float[] denseVector) {
        float score = 0.0f;

        // Early exit for empty vectors
        if (this.size == 0 || denseVector.length == 0) return 0;

        // Loop unrolling for better performance
        final int unrollFactor = 4;
        final int limit = this.size - (this.size % unrollFactor);

        // Main loop with unrolling
        int i = 0;
        for (; i < limit; i += unrollFactor) {
            if (this.tokens[i] >= denseVector.length) {
                break;
            }
            score += this.freqs[i] * denseVector[this.tokens[i]];

            if (this.tokens[i + 1] >= denseVector.length) {
                ++i;
                break;
            }
            score += this.freqs[i + 1] * denseVector[this.tokens[i + 1]];

            if (this.tokens[i + 2] >= denseVector.length) {
                i += 2;
                break;
            }
            score += this.freqs[i + 2] * denseVector[this.tokens[i + 2]];

            if (this.tokens[i + 3] >= denseVector.length) {
                i += 3;
                break;
            }
            score += this.freqs[i + 3] * denseVector[this.tokens[i + 3]];
        }

        // Handle remaining elements
        for (; i < this.size; ++i) {
            if (this.tokens[i] >= denseVector.length) {
                break;
            }
            score += this.freqs[i] * denseVector[this.tokens[i]];
        }

        return score;
    }

    public IteratorWrapper<Item> iterator() {
        return new IteratorWrapper<>(new Iterator<>() {
            private int current = -1;

            @Override
            public boolean hasNext() {
                return current + 1 < size;
            }

            @Override
            public Item next() {
                if (!hasNext()) {
                    return null;
                }
                ++current;
                return new Item(tokens[current], freqs[current]);
            }
        });
    }

    @Override
    public long ramBytesUsed() {
        return RamUsageEstimator.shallowSizeOfInstance(SparseVector.class) + RamUsageEstimator.sizeOf(tokens) + RamUsageEstimator.sizeOf(
            freqs
        );
    }

    @AllArgsConstructor
    @Getter
    @EqualsAndHashCode
    public static class Item {
        int token;
        float freq;

        static Item of(int token, float freq) {
            return new Item(token, freq);
        }
    }
}
