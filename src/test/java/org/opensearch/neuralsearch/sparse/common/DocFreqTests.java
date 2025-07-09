/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

public class DocFreqTests extends AbstractSparseTestBase {

    public void testGetDocID_afterConstruction_returnsCorrectValue() {
        int docID = 42;
        byte freq = 5;
        DocFreq docFreq = new DocFreq(docID, freq);
        assertEquals(docID, docFreq.getDocID());
    }

    public void testGetFreq_afterConstruction_returnsCorrectValue() {
        int docID = 42;
        byte freq = 5;
        DocFreq docFreq = new DocFreq(docID, freq);
        assertEquals(freq, docFreq.getFreq());
    }

    public void testGetIntFreq_withPositiveByteValue_returnsCorrectValue() {
        DocFreq docFreq = new DocFreq(1, (byte) 5);
        assertEquals(5, docFreq.getIntFreq());
    }

    public void testGetIntFreq_withNegativeByteValue_returnsUnsignedValue() {
        DocFreq docFreq = new DocFreq(2, (byte) 0xFF); // -1 as signed byte, 255 as unsigned
        assertEquals(255, docFreq.getIntFreq());
    }

    public void testGetIntFreq_withZeroValue_returnsZero() {
        DocFreq docFreq = new DocFreq(3, (byte) 0);
        assertEquals(0, docFreq.getIntFreq());
    }

    public void testGetIntFreq_withMaxSignedByteValue_returnsCorrectValue() {
        DocFreq docFreq = new DocFreq(4, (byte) 127);
        assertEquals(127, docFreq.getIntFreq());
    }

    public void testCompareTo_withSmallerDocID_returnsNegative() {
        DocFreq docFreq1 = new DocFreq(1, (byte) 10);
        DocFreq docFreq2 = new DocFreq(2, (byte) 5);
        assertTrue(docFreq1.compareTo(docFreq2) < 0);
    }

    public void testCompareTo_withLargerDocID_returnsPositive() {
        DocFreq docFreq1 = new DocFreq(2, (byte) 5);
        DocFreq docFreq2 = new DocFreq(1, (byte) 10);
        assertTrue(docFreq1.compareTo(docFreq2) > 0);
    }

    public void testCompareTo_withSameDocID_returnsZero() {
        DocFreq docFreq1 = new DocFreq(1, (byte) 10);
        DocFreq docFreq2 = new DocFreq(1, (byte) 20);
        assertEquals(0, docFreq1.compareTo(docFreq2));
    }

    public void testEquals_withSameValues_returnsTrue() {
        DocFreq docFreq1 = new DocFreq(1, (byte) 10);
        DocFreq docFreq2 = new DocFreq(1, (byte) 10);
        assertEquals(docFreq1, docFreq2);
    }

    public void testEquals_withDifferentFreq_returnsFalse() {
        DocFreq docFreq1 = new DocFreq(1, (byte) 10);
        DocFreq docFreq2 = new DocFreq(1, (byte) 20);
        assertNotEquals(docFreq1, docFreq2);
    }

    public void testEquals_withDifferentDocID_returnsFalse() {
        DocFreq docFreq1 = new DocFreq(1, (byte) 10);
        DocFreq docFreq2 = new DocFreq(2, (byte) 10);
        assertNotEquals(docFreq1, docFreq2);
    }

    public void testEquals_withNull_returnsFalse() {
        DocFreq docFreq = new DocFreq(1, (byte) 10);
        assertNotEquals(docFreq, null);
    }

    public void testEquals_withDifferentType_returnsFalse() {
        DocFreq docFreq = new DocFreq(1, (byte) 10);
        assertNotEquals(docFreq, "not a DocFreq");
    }

    public void testHashCode_withEqualObjects_returnsSameValue() {
        DocFreq docFreq1 = new DocFreq(1, (byte) 10);
        DocFreq docFreq2 = new DocFreq(1, (byte) 10);
        assertEquals(docFreq1.hashCode(), docFreq2.hashCode());
    }

    public void testCompareTo_withMinAndMaxValues_returnsNegative() {
        DocFreq minDocFreq = new DocFreq(Integer.MIN_VALUE, (byte) 0);
        DocFreq maxDocFreq = new DocFreq(Integer.MAX_VALUE, (byte) 0);
        assertTrue(minDocFreq.compareTo(maxDocFreq) < 0);
    }

    public void testCompareTo_withMaxAndMinValues_returnsPositive() {
        DocFreq minDocFreq = new DocFreq(Integer.MIN_VALUE, (byte) 0);
        DocFreq maxDocFreq = new DocFreq(Integer.MAX_VALUE, (byte) 0);
        assertTrue(maxDocFreq.compareTo(minDocFreq) > 0);
    }

    public void testCompareTo_withSameObject_returnsZero() {
        DocFreq docFreq = new DocFreq(1, (byte) 10);
        assertEquals(0, docFreq.compareTo(docFreq));
    }
}
