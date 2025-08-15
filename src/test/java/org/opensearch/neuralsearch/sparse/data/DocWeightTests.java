/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.data;

import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

public class DocWeightTests extends AbstractSparseTestBase {

    public void testGetDocID_afterConstruction_returnsCorrectValue() {
        int docID = 42;
        byte freq = 5;
        DocWeight docWeight = new DocWeight(docID, freq);
        assertEquals(docID, docWeight.getDocID());
    }

    public void testGetWeight_afterConstruction_returnsCorrectValue() {
        int docID = 42;
        byte freq = 5;
        DocWeight docWeight = new DocWeight(docID, freq);
        assertEquals(freq, docWeight.getWeight());
    }

    public void testGetIntWeight_withPositiveByteValue_returnsCorrectValue() {
        DocWeight docWeight = new DocWeight(1, (byte) 5);
        assertEquals(5, docWeight.getIntWeight());
    }

    public void testGetIntWeight_withNegativeByteValue_returnsUnsignedValue() {
        DocWeight docWeight = new DocWeight(2, (byte) 0xFF); // -1 as signed byte, 255 as unsigned
        assertEquals(255, docWeight.getIntWeight());
    }

    public void testGetIntWeight_withZeroValue_returnsZero() {
        DocWeight docWeight = new DocWeight(3, (byte) 0);
        assertEquals(0, docWeight.getIntWeight());
    }

    public void testGetIntWeight_withMaxSignedByteValue_returnsCorrectValue() {
        DocWeight docWeight = new DocWeight(4, (byte) 127);
        assertEquals(127, docWeight.getIntWeight());
    }

    public void testCompareTo_withSmallerDocID_returnsNegative() {
        DocWeight docWeight1 = new DocWeight(1, (byte) 10);
        DocWeight docWeight2 = new DocWeight(2, (byte) 5);
        assertTrue(docWeight1.compareTo(docWeight2) < 0);
    }

    public void testCompareTo_withLargerDocID_returnsPositive() {
        DocWeight docWeight1 = new DocWeight(2, (byte) 5);
        DocWeight docWeight2 = new DocWeight(1, (byte) 10);
        assertTrue(docWeight1.compareTo(docWeight2) > 0);
    }

    public void testCompareTo_withSameDocID_returnsZero() {
        DocWeight docWeight1 = new DocWeight(1, (byte) 10);
        DocWeight docWeight2 = new DocWeight(1, (byte) 20);
        assertEquals(0, docWeight1.compareTo(docWeight2));
    }

    public void testEquals_withSameValues_returnsTrue() {
        DocWeight docWeight1 = new DocWeight(1, (byte) 10);
        DocWeight docWeight2 = new DocWeight(1, (byte) 10);
        assertEquals(docWeight1, docWeight2);
    }

    public void testEquals_withDifferentFreq_returnsFalse() {
        DocWeight docWeight1 = new DocWeight(1, (byte) 10);
        DocWeight docWeight2 = new DocWeight(1, (byte) 20);
        assertNotEquals(docWeight1, docWeight2);
    }

    public void testEquals_withDifferentDocID_returnsFalse() {
        DocWeight docWeight1 = new DocWeight(1, (byte) 10);
        DocWeight docWeight2 = new DocWeight(2, (byte) 10);
        assertNotEquals(docWeight1, docWeight2);
    }

    public void testEquals_withNull_returnsFalse() {
        DocWeight docWeight = new DocWeight(1, (byte) 10);
        assertNotEquals(docWeight, null);
    }

    public void testEquals_withDifferentType_returnsFalse() {
        DocWeight docWeight = new DocWeight(1, (byte) 10);
        assertNotEquals(docWeight, "not a DocWeight");
    }

    public void testHashCode_withEqualObjects_returnsSameValue() {
        DocWeight docWeight1 = new DocWeight(1, (byte) 10);
        DocWeight docWeight2 = new DocWeight(1, (byte) 10);
        assertEquals(docWeight1.hashCode(), docWeight2.hashCode());
    }

    public void testCompareTo_withMinAndMaxValues_returnsNegative() {
        DocWeight minDocWeight = new DocWeight(Integer.MIN_VALUE, (byte) 0);
        DocWeight maxDocWeight = new DocWeight(Integer.MAX_VALUE, (byte) 0);
        assertTrue(minDocWeight.compareTo(maxDocWeight) < 0);
    }

    public void testCompareTo_withMaxAndMinValues_returnsPositive() {
        DocWeight minDocWeight = new DocWeight(Integer.MIN_VALUE, (byte) 0);
        DocWeight maxDocWeight = new DocWeight(Integer.MAX_VALUE, (byte) 0);
        assertTrue(maxDocWeight.compareTo(minDocWeight) > 0);
    }

    public void testCompareTo_withSameObject_returnsZero() {
        DocWeight docWeight = new DocWeight(1, (byte) 10);
        assertEquals(0, docWeight.compareTo(docWeight));
    }
}
