/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import lombok.SneakyThrows;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@code readToList} feeds the native writer's CSR buffer, so its order and duplicate handling are
 * part of what the index is built from -- unlike {@code readToMap}, which is free to collapse both.
 */
public class BinaryVectorUtilsTests extends AbstractSparseTestBase {

    @SneakyThrows
    public void testReadToMapDecodesPairs() {
        Map<Integer, Float> map = BinaryVectorUtils.readToMap(encode(new int[] { 7, 3 }, new float[] { 0.5f, 1.5f }));

        assertEquals(Map.of(7, 0.5f, 3, 1.5f), map);
    }

    @SneakyThrows
    public void testReadToMapKeepsLastWeightForRepeatedToken() {
        Map<Integer, Float> map = BinaryVectorUtils.readToMap(encode(new int[] { 4, 4 }, new float[] { 0.25f, 0.75f }));

        assertEquals(1, map.size());
        assertEquals(0.75f, map.get(4), 0.0f);
    }

    @SneakyThrows
    public void testReadToListPreservesEncodedOrder() {
        List<Integer> tokens = new ArrayList<>();
        List<Float> weights = new ArrayList<>();

        // Deliberately not ascending: doc-value tokens are stored in parser order, and the writer
        // derives the index dimension from the maximum rather than from the last element.
        BinaryVectorUtils.readToList(encode(new int[] { 9, 2, 5 }, new float[] { 0.1f, 0.2f, 0.3f }), tokens, weights);

        assertEquals(List.of(9, 2, 5), tokens);
        assertEquals(List.of(0.1f, 0.2f, 0.3f), weights);
    }

    @SneakyThrows
    public void testReadToListKeepsDuplicateTokens() {
        List<Integer> tokens = new ArrayList<>();
        List<Float> weights = new ArrayList<>();

        BinaryVectorUtils.readToList(encode(new int[] { 4, 4 }, new float[] { 0.25f, 0.75f }), tokens, weights);

        assertEquals(List.of(4, 4), tokens);
        assertEquals(List.of(0.25f, 0.75f), weights);
    }

    @SneakyThrows
    public void testReadToListAppendsToNonEmptyLists() {
        List<Integer> tokens = new ArrayList<>(List.of(1));
        List<Float> weights = new ArrayList<>(List.of(1.0f));

        BinaryVectorUtils.readToList(encode(new int[] { 2 }, new float[] { 2.0f }), tokens, weights);

        assertEquals(List.of(1, 2), tokens);
        assertEquals(List.of(1.0f, 2.0f), weights);
    }

    @SneakyThrows
    public void testReadHonoursBytesRefOffsetAndLength() {
        BytesRef encoded = encode(new int[] { 6 }, new float[] { 1.25f });
        byte[] padded = new byte[encoded.length + 4];
        System.arraycopy(encoded.bytes, encoded.offset, padded, 3, encoded.length);
        BytesRef slice = new BytesRef(padded, 3, encoded.length);

        List<Integer> tokens = new ArrayList<>();
        List<Float> weights = new ArrayList<>();
        BinaryVectorUtils.readToList(slice, tokens, weights);

        assertEquals(List.of(6), tokens);
        assertEquals(List.of(1.25f), weights);
        assertEquals(Map.of(6, 1.25f), BinaryVectorUtils.readToMap(slice));
    }

    @SneakyThrows
    public void testReadEmptyVector() {
        List<Integer> tokens = new ArrayList<>();
        List<Float> weights = new ArrayList<>();
        BinaryVectorUtils.readToList(new BytesRef(new byte[0]), tokens, weights);

        assertTrue(tokens.isEmpty());
        assertTrue(weights.isEmpty());
        assertTrue(BinaryVectorUtils.readToMap(new BytesRef(new byte[0])).isEmpty());
    }

    @SneakyThrows
    private BytesRef encode(int[] tokens, float[] weights) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            for (int i = 0; i < tokens.length; i++) {
                dos.writeInt(tokens[i]);
                dos.writeFloat(weights[i]);
            }
        }
        return new BytesRef(baos.toByteArray());
    }
}
