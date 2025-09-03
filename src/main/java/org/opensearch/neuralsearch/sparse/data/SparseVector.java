/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.data;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.MODULUS_FOR_SHORT;

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

    public SparseVector(Map<Integer, Float> pairs) {
        this(
            pairs.entrySet()
                .stream()
                .map(t -> new Item(t.getKey(), ByteQuantizer.quantizeFloatToByte(t.getValue())))
                .collect(Collectors.toList())
        );
    }

    public SparseVector(List<Item> items) {
        List<Item> processedItems = processListItems(items);
        int size = processedItems.size();
        this.tokens = new short[size];
        this.weights = new byte[size];
        for (int i = 0; i < size; ++i) {
            this.tokens[i] = (short) processedItems.get(i).getToken();
            this.weights[i] = processedItems.get(i).getWeight();
        }
    }

    private List<Item> processListItems(List<Item> items) {
        // processItems contains token already mod by MODULUS_FOR_SHORT and max weight
        List<Item> processedItems = new ArrayList<>();
        if (items.isEmpty()) {
            return processedItems;
        }
        items.sort(Comparator.comparingInt(item -> prepareTokenForShortType(item.getToken())));
        processedItems.add(new Item(prepareTokenForShortType(items.getFirst().getToken()), items.getFirst().getWeight()));
        for (int i = 1; i < items.size(); ++i) {
            int token = prepareTokenForShortType(items.get(i).getToken());
            if (token == processedItems.getLast().getToken()) {
                if (ByteQuantizer.compareUnsignedByte(processedItems.getLast().weight, items.get(i).getWeight()) < 0) {
                    // merge by taking the maximum value
                    processedItems.getLast().weight = items.get(i).getWeight();
                }
            } else {
                processedItems.add(new Item(token, items.get(i).getWeight()));
            }
        }
        return processedItems;
    }

    public static int prepareTokenForShortType(int token) {
        return token % MODULUS_FOR_SHORT;
    }

    private static Map<Integer, Float> readToMap(BytesRef bytesRef) throws IOException {
        Map<Integer, Float> map = new HashMap<>();
        try (
            ByteArrayInputStream bais = new ByteArrayInputStream(
                ArrayUtil.copyOfSubArray(bytesRef.bytes, bytesRef.offset, bytesRef.length)
            );
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
        if (size == 0 || denseVector == null || denseVector.length == 0) return 0;

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
