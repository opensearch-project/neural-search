/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.data;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
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
    // tokens will be stored in order
    private final short[] tokens;
    private final byte[] weights;

    public SparseVector(BytesRef bytesRef) throws IOException {
        this(readToMap(bytesRef));
    }

    public int getSize() {
        return tokens == null ? 0 : tokens.length;
    }

    public SparseVector(Map<String, Float> pairs) {
        this(
            pairs.entrySet()
                .stream()
                .map(t -> new Item(convertStringToInteger(t.getKey()), ByteQuantizer.quantizeFloatToByte(t.getValue())))
                .collect(Collectors.toList())
        );
    }

    private static Integer convertStringToInteger(String value) {
        return NumberUtils.createInteger(value);
    }

    public SparseVector(List<Item> items) {
        items.sort(Comparator.comparingInt(Item::getToken));
        int size = items.size();
        this.tokens = new short[size];
        this.weights = new byte[size];
        for (int i = 0; i < size; ++i) {
            this.tokens[i] = (short) items.get(i).getToken();
            this.weights[i] = items.get(i).getWeight();
        }
    }

    private static Map<String, Float> readToMap(BytesRef bytesRef) {
        Map<String, Float> map = new HashMap<>();
        try (
            ByteArrayInputStream bais = new ByteArrayInputStream(
                ArrayUtil.copyOfSubArray(bytesRef.bytes, bytesRef.offset, bytesRef.length)
            );
            DataInputStream dis = new DataInputStream(bais)
        ) {
            while (bais.available() > 0) {
                int stringLength = dis.readInt();
                byte[] stringBytes = new byte[stringLength];
                dis.readFully(stringBytes);
                String key = new String(stringBytes, StandardCharsets.UTF_8);
                float value = dis.readFloat();
                map.put(key, value);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return map;
    }

    public byte[] toDenseVector() {
        int size = getSize();
        if (size == 0) {
            return new byte[0];
        }
        int maxToken = this.tokens[size - 1];
        byte[] denseVector = new byte[maxToken + 1];
        for (int i = 0; i < size; ++i) {
            denseVector[this.tokens[i]] = this.weights[i];
        }
        return denseVector;
    }

    public int dotProduct(final byte[] denseVector) {
        int score = 0;
        int size = getSize();

        // Early exit for empty vectors
        if (size == 0 || denseVector.length == 0) return 0;

        // Loop unrolling for better performance
        final int unrollFactor = 4;
        final int limit = size - (size % unrollFactor);

        // Main loop with unrolling
        int i = 0;
        for (; i < limit; i += unrollFactor) {
            if (this.tokens[i] >= denseVector.length) {
                break;
            }
            score += ByteQuantizer.multiplyUnsignedByte(this.weights[i], denseVector[this.tokens[i]]);

            if (this.tokens[i + 1] >= denseVector.length) {
                ++i;
                break;
            }
            score += ByteQuantizer.multiplyUnsignedByte(this.weights[i + 1], denseVector[this.tokens[i + 1]]);

            if (this.tokens[i + 2] >= denseVector.length) {
                i += 2;
                break;
            }
            score += ByteQuantizer.multiplyUnsignedByte(this.weights[i + 2], denseVector[this.tokens[i + 2]]);

            if (this.tokens[i + 3] >= denseVector.length) {
                i += 3;
                break;
            }
            score += ByteQuantizer.multiplyUnsignedByte(this.weights[i + 3], denseVector[this.tokens[i + 3]]);
        }

        // Handle remaining elements
        for (; i < size; ++i) {
            if (this.tokens[i] >= denseVector.length) {
                break;
            }
            score += ByteQuantizer.multiplyUnsignedByte(this.weights[i], denseVector[this.tokens[i]]);
        }

        return score;
    }

    public IteratorWrapper<Item> iterator() {
        return new IteratorWrapper<>(new Iterator<>() {
            private int size = getSize();
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
                return new Item(tokens[current], weights[current]);
            }
        });
    }

    @Override
    public long ramBytesUsed() {
        return RamUsageEstimator.shallowSizeOfInstance(SparseVector.class) + RamUsageEstimator.sizeOf(tokens) + RamUsageEstimator.sizeOf(
            weights
        );
    }

    @AllArgsConstructor
    @Getter
    @EqualsAndHashCode
    public static class Item {
        int token;
        byte weight;

        static Item of(int token, byte weight) {
            return new Item(token, weight);
        }

        public int getIntWeight() {
            return ByteQuantizer.getUnsignedByte(weight);
        }
    }
}
