/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.apache.lucene.util.BytesRef;
import org.junit.Assert;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.algorithm.ByteQuantizer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparseVectorTests extends AbstractSparseTestBase {

    public void testConstructorWithItems() {
        // Create items
        List<SparseVector.Item> items = new ArrayList<>();
        items.add(new SparseVector.Item(3, (byte) 10));
        items.add(new SparseVector.Item(1, (byte) 20));
        items.add(new SparseVector.Item(2, (byte) 30));

        // Create sparse vector
        SparseVector vector = new SparseVector(items);

        // Verify size
        Assert.assertEquals(3, vector.getSize());

        // Verify items are sorted by token
        IteratorWrapper<SparseVector.Item> iterator = vector.iterator();
        Assert.assertTrue(iterator.hasNext());
        SparseVector.Item item1 = iterator.next();
        Assert.assertEquals(1, item1.getToken());
        Assert.assertEquals(20, ByteQuantizer.getUnsignedByte(item1.getFreq()));

        Assert.assertTrue(iterator.hasNext());
        SparseVector.Item item2 = iterator.next();
        Assert.assertEquals(2, item2.getToken());
        Assert.assertEquals(30, ByteQuantizer.getUnsignedByte(item2.getFreq()));

        Assert.assertTrue(iterator.hasNext());
        SparseVector.Item item3 = iterator.next();
        Assert.assertEquals(3, item3.getToken());
        Assert.assertEquals(10, ByteQuantizer.getUnsignedByte(item3.getFreq()));

        Assert.assertFalse(iterator.hasNext());
    }

    public void testConstructorWithMap() {
        // Create map
        Map<String, Float> map = new HashMap<>();
        map.put("3", 0.1f);
        map.put("1", 0.2f);
        map.put("2", 0.3f);

        // Create sparse vector
        SparseVector vector = new SparseVector(map);

        // Verify size
        Assert.assertEquals(3, vector.getSize());

        // Verify items are sorted by token
        IteratorWrapper<SparseVector.Item> iterator = vector.iterator();
        Assert.assertTrue(iterator.hasNext());
        SparseVector.Item item1 = iterator.next();
        Assert.assertEquals(1, item1.getToken());

        Assert.assertTrue(iterator.hasNext());
        SparseVector.Item item2 = iterator.next();
        Assert.assertEquals(2, item2.getToken());

        Assert.assertTrue(iterator.hasNext());
        SparseVector.Item item3 = iterator.next();
        Assert.assertEquals(3, item3.getToken());
    }

    public void testConstructorWithBytesRef() throws IOException {
        // Create map and serialize to BytesRef
        Map<String, Float> map = new HashMap<>();
        map.put("3", 0.1f);
        map.put("1", 0.2f);
        map.put("2", 0.3f);

        BytesRef bytesRef = serializeMap(map);

        // Create sparse vector
        SparseVector vector = new SparseVector(bytesRef);

        // Verify size
        Assert.assertEquals(3, vector.getSize());

        // Verify items are sorted by token
        IteratorWrapper<SparseVector.Item> iterator = vector.iterator();
        Assert.assertTrue(iterator.hasNext());
        SparseVector.Item item1 = iterator.next();
        Assert.assertEquals(1, item1.getToken());

        Assert.assertTrue(iterator.hasNext());
        SparseVector.Item item2 = iterator.next();
        Assert.assertEquals(2, item2.getToken());

        Assert.assertTrue(iterator.hasNext());
        SparseVector.Item item3 = iterator.next();
        Assert.assertEquals(3, item3.getToken());
    }

    public void testToDenseVector() {
        // Create items
        List<SparseVector.Item> items = new ArrayList<>();
        items.add(new SparseVector.Item(1, (byte) 20));
        items.add(new SparseVector.Item(3, (byte) 10));

        // Create sparse vector
        SparseVector vector = new SparseVector(items);

        // Convert to dense vector
        byte[] denseVector = vector.toDenseVector();

        // Verify dense vector
        Assert.assertEquals(4, denseVector.length); // max token (3) + 1
        Assert.assertEquals(0, denseVector[0]);
        Assert.assertEquals(20, denseVector[1] & 0xFF);
        Assert.assertEquals(0, denseVector[2]);
        Assert.assertEquals(10, denseVector[3] & 0xFF);
    }

    public void testDotProduct() {
        // Create sparse vector
        List<SparseVector.Item> items = new ArrayList<>();
        items.add(new SparseVector.Item(0, (byte) 10));
        items.add(new SparseVector.Item(2, (byte) 20));
        items.add(new SparseVector.Item(4, (byte) 30));
        SparseVector vector = new SparseVector(items);

        // Create dense vector
        byte[] denseVector = new byte[5];
        denseVector[0] = 5;
        denseVector[1] = 0;
        denseVector[2] = 10;
        denseVector[3] = 0;
        denseVector[4] = 15;

        // Calculate dot product
        int dotProduct = vector.dotProduct(denseVector);

        // Verify dot product: (10*5) + (20*10) + (30*15) = 50 + 200 + 450 = 700
        Assert.assertEquals(700, dotProduct);
    }

    public void testDotProductWithEmptyVectors() {
        // Empty sparse vector
        SparseVector emptyVector = new SparseVector(new ArrayList<>());
        byte[] denseVector = new byte[] { 1, 2, 3 };
        Assert.assertEquals(0, emptyVector.dotProduct(denseVector));

        // Empty dense vector
        List<SparseVector.Item> items = new ArrayList<>();
        items.add(new SparseVector.Item(0, (byte) 10));
        SparseVector vector = new SparseVector(items);
        Assert.assertEquals(0, vector.dotProduct(new byte[0]));
    }

    public void testDotProductWithDenseShorterThanSparse() {
        // Create sparse vector with tokens beyond dense vector length
        List<SparseVector.Item> items = new ArrayList<>();
        items.add(new SparseVector.Item(0, (byte) 10)); // Within dense vector bounds
        items.add(new SparseVector.Item(2, (byte) 20)); // Within dense vector bounds
        items.add(new SparseVector.Item(5, (byte) 30)); // Beyond dense vector bounds
        SparseVector vector = new SparseVector(items);

        // Create dense vector shorter than max token in sparse
        byte[] denseVector = new byte[3]; // Length 3, so token 5 is out of bounds
        denseVector[0] = 5;
        denseVector[1] = 0;
        denseVector[2] = 10;

        // Calculate dot product - should only include tokens within bounds
        int dotProduct = vector.dotProduct(denseVector);

        // Verify dot product: (10*5) + (20*10) = 50 + 200 = 250
        Assert.assertEquals(250, dotProduct);
    }

    public void testDotProductLoopUnrolling() {
        // Create sparse vector with exactly 4 items to test loop unrolling
        List<SparseVector.Item> items = new ArrayList<>();
        items.add(new SparseVector.Item(0, (byte) 10));
        items.add(new SparseVector.Item(1, (byte) 20));
        items.add(new SparseVector.Item(2, (byte) 30));
        items.add(new SparseVector.Item(3, (byte) 40));
        SparseVector vector = new SparseVector(items);

        // Create dense vector
        byte[] denseVector = new byte[4];
        denseVector[0] = 5;
        denseVector[1] = 10;
        denseVector[2] = 15;
        denseVector[3] = 20;

        // Calculate dot product
        int dotProduct = vector.dotProduct(denseVector);

        // Verify dot product: (10*5) + (20*10) + (30*15) + (40*20) = 50 + 200 + 450 + 800 = 1500
        Assert.assertEquals(1500, dotProduct);
    }

    public void testDotProductWithNonUnrollableSize() {
        // Create sparse vector with non-multiple of 4 items to test remainder handling
        List<SparseVector.Item> items = new ArrayList<>();
        items.add(new SparseVector.Item(0, (byte) 10));
        items.add(new SparseVector.Item(1, (byte) 20));
        items.add(new SparseVector.Item(2, (byte) 30));
        items.add(new SparseVector.Item(3, (byte) 40));
        items.add(new SparseVector.Item(4, (byte) 50));
        SparseVector vector = new SparseVector(items);

        // Create dense vector
        byte[] denseVector = new byte[5];
        denseVector[0] = 5;
        denseVector[1] = 10;
        denseVector[2] = 15;
        denseVector[3] = 20;
        denseVector[4] = 25;

        // Calculate dot product
        int dotProduct = vector.dotProduct(denseVector);

        // Verify dot product: (10*5) + (20*10) + (30*15) + (40*20) + (50*25) = 50 + 200 + 450 + 800 + 1250 = 2750
        Assert.assertEquals(2750, dotProduct);
    }

    public void testEquals() {
        // Create two identical vectors
        List<SparseVector.Item> items1 = new ArrayList<>();
        items1.add(new SparseVector.Item(1, (byte) 20));
        items1.add(new SparseVector.Item(3, (byte) 10));
        SparseVector vector1 = new SparseVector(items1);

        List<SparseVector.Item> items2 = new ArrayList<>();
        items2.add(new SparseVector.Item(1, (byte) 20));
        items2.add(new SparseVector.Item(3, (byte) 10));
        SparseVector vector2 = new SparseVector(items2);

        // Verify equals
        Assert.assertEquals(vector1, vector2);
        Assert.assertEquals(vector1.hashCode(), vector2.hashCode());

        // Create a different vector
        List<SparseVector.Item> items3 = new ArrayList<>();
        items3.add(new SparseVector.Item(1, (byte) 20));
        items3.add(new SparseVector.Item(3, (byte) 15)); // Different frequency
        SparseVector vector3 = new SparseVector(items3);

        // Verify not equals
        Assert.assertNotEquals(vector1, vector3);
    }

    private BytesRef serializeMap(Map<String, Float> map) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        for (Map.Entry<String, Float> entry : map.entrySet()) {
            byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            dos.writeInt(keyBytes.length);
            dos.write(keyBytes);
            dos.writeFloat(entry.getValue());
        }

        dos.flush();
        byte[] bytes = baos.toByteArray();
        return new BytesRef(bytes);
    }
}
