/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.DocFreqIterator;
import org.opensearch.neuralsearch.sparse.common.SparseVector;

public class DocumentClusterTests extends AbstractSparseTestBase {

    public void testConstructor_withValidInputs_createsCluster() {
        Map<String, Float> summaryMap = new HashMap<>();
        summaryMap.put("1", 0.5f);
        summaryMap.put("2", 0.8f);
        SparseVector summary = new SparseVector(summaryMap);
        List<DocFreq> docs = Arrays.asList(new DocFreq(10, (byte) 2), new DocFreq(5, (byte) 1));

        DocumentCluster cluster = new DocumentCluster(summary, docs, false);

        assertEquals(summary, cluster.getSummary());
        assertEquals(2, cluster.size());
        assertFalse(cluster.isShouldNotSkip());
    }

    public void testConstructor_withValidInputsNullSummary_createsCluster() {
        List<DocFreq> docs = Arrays.asList(new DocFreq(10, (byte) 2), new DocFreq(5, (byte) 1));

        DocumentCluster cluster = new DocumentCluster(null, docs, false);

        assertEquals(null, cluster.getSummary());
        assertEquals(2, cluster.size());
        assertFalse(cluster.isShouldNotSkip());
    }

    public void testConstructor_sortsDocumentsByDocId() {
        Map<String, Float> summaryMap = new HashMap<>();
        summaryMap.put("1", 1.0f);
        SparseVector summary = new SparseVector(summaryMap);
        List<DocFreq> docs = Arrays.asList(new DocFreq(10, (byte) 2), new DocFreq(5, (byte) 1), new DocFreq(15, (byte) 3));

        DocumentCluster cluster = new DocumentCluster(summary, docs, true);

        Iterator<DocFreq> iterator = cluster.iterator();
        assertEquals(5, iterator.next().getDocID());
        assertEquals(10, iterator.next().getDocID());
        assertEquals(15, iterator.next().getDocID());
    }

    public void testSize_withEmptyDocs_returnsZero() {
        List<DocFreq> docs = new ArrayList<>();

        DocumentCluster cluster = new DocumentCluster(null, docs, false);

        assertEquals(0, cluster.size());
    }

    public void testIterator_withMultipleDocs_iteratesInOrder() {
        List<DocFreq> docs = Arrays.asList(new DocFreq(20, (byte) 4), new DocFreq(10, (byte) 2), new DocFreq(30, (byte) 6));

        DocumentCluster cluster = new DocumentCluster(null, docs, false);
        Iterator<DocFreq> iterator = cluster.iterator();

        DocFreq first = iterator.next();
        assertEquals(10, first.getDocID());
        assertEquals(2, first.getFreq());

        DocFreq second = iterator.next();
        assertEquals(20, second.getDocID());
        assertEquals(4, second.getFreq());

        DocFreq third = iterator.next();
        assertEquals(30, third.getDocID());
        assertEquals(6, third.getFreq());

        assertFalse(iterator.hasNext());
    }

    public void testGetDisi_withValidDocs_returnsCorrectIterator() throws Exception {
        List<DocFreq> docs = Arrays.asList(new DocFreq(5, (byte) 1), new DocFreq(10, (byte) 2));

        DocumentCluster cluster = new DocumentCluster(null, docs, false);
        DocFreqIterator disi = cluster.getDisi();

        assertEquals(-1, disi.docID());
        assertEquals(5, disi.nextDoc());
        assertEquals(5, disi.docID());
        assertEquals(1, disi.freq());

        assertEquals(10, disi.nextDoc());
        assertEquals(10, disi.docID());
        assertEquals(2, disi.freq());

        assertEquals(DocFreqIterator.NO_MORE_DOCS, disi.nextDoc());
    }

    public void testGetDisi_withValidDocs_returnsZeroAdvance() throws Exception {
        List<DocFreq> docs = Arrays.asList(new DocFreq(5, (byte) 1), new DocFreq(10, (byte) 2));

        DocumentCluster cluster = new DocumentCluster(null, docs, false);
        DocFreqIterator disi = cluster.getDisi();

        assertEquals(-1, disi.docID());
        assertEquals(5, disi.nextDoc());
        assertEquals(0, disi.advance(disi.docID()));

        assertEquals(10, disi.nextDoc());
        assertEquals(0, disi.advance(disi.docID()));

        assertEquals(DocFreqIterator.NO_MORE_DOCS, disi.nextDoc());
    }

    public void testGetDisi_withValidDocs_returnsZeroCost() throws Exception {
        List<DocFreq> docs = Arrays.asList(new DocFreq(5, (byte) 1), new DocFreq(10, (byte) 2));

        DocumentCluster cluster = new DocumentCluster(null, docs, false);
        DocFreqIterator disi = cluster.getDisi();

        assertEquals(-1, disi.docID());
        assertEquals(5, disi.nextDoc());
        assertEquals(0, disi.cost());

        assertEquals(10, disi.nextDoc());
        assertEquals(0, disi.cost());

        assertEquals(DocFreqIterator.NO_MORE_DOCS, disi.nextDoc());
    }

    public void testRamBytesUsed_withNullSummary_returnsPositiveValue() {
        List<DocFreq> docs = Arrays.asList(new DocFreq(1, (byte) 1));

        DocumentCluster cluster = new DocumentCluster(null, docs, false);

        assertTrue(cluster.ramBytesUsed() > 0);
    }

    public void testRamBytesUsed_withSummary_includesSummarySize() {
        Map<String, Float> summaryMap = new HashMap<>();
        summaryMap.put("1", 0.5f);
        summaryMap.put("2", 0.8f);
        SparseVector summary = new SparseVector(summaryMap);
        List<DocFreq> docs = Arrays.asList(new DocFreq(1, (byte) 1));

        DocumentCluster cluster = new DocumentCluster(summary, docs, false);

        assertTrue(cluster.ramBytesUsed() > 0);
    }

    public void testGetChildResources_withNullSummary_returnsEmptyList() {
        List<DocFreq> docs = Arrays.asList(new DocFreq(1, (byte) 1));

        DocumentCluster cluster = new DocumentCluster(null, docs, false);

        assertTrue(cluster.getChildResources().isEmpty());
    }

    public void testGetChildResources_withSummary_returnsSummary() {
        Map<String, Float> summaryMap = new HashMap<>();
        summaryMap.put("1", 1.0f);
        SparseVector summary = new SparseVector(summaryMap);
        List<DocFreq> docs = Arrays.asList(new DocFreq(1, (byte) 1));

        DocumentCluster cluster = new DocumentCluster(summary, docs, false);

        assertEquals(1, cluster.getChildResources().size());
        assertTrue(cluster.getChildResources().contains(summary));
    }

    public void testEquals_withSameValues_returnsTrue() {
        Map<String, Float> summaryMap = new HashMap<>();
        summaryMap.put("1", 1.0f);
        SparseVector summary = new SparseVector(summaryMap);
        List<DocFreq> docs = Arrays.asList(new DocFreq(1, (byte) 1));

        DocumentCluster cluster1 = new DocumentCluster(summary, docs, false);
        DocumentCluster cluster2 = new DocumentCluster(summary, docs, false);

        assertEquals(cluster1, cluster2);
    }

    public void testEquals_withDifferentShouldNotSkip_returnsFalse() {
        Map<String, Float> summaryMap = new HashMap<>();
        summaryMap.put("1", 1.0f);
        SparseVector summary = new SparseVector(summaryMap);
        List<DocFreq> docs = Arrays.asList(new DocFreq(1, (byte) 1));

        DocumentCluster cluster1 = new DocumentCluster(summary, docs, true);
        DocumentCluster cluster2 = new DocumentCluster(summary, docs, false);

        assertNotEquals(cluster1, cluster2);
    }

    public void testHashCode_withSameValues_returnsSameHashCode() {
        Map<String, Float> summaryMap = new HashMap<>();
        summaryMap.put("1", 1.0f);
        SparseVector summary = new SparseVector(summaryMap);
        List<DocFreq> docs = Arrays.asList(new DocFreq(1, (byte) 1));

        DocumentCluster cluster1 = new DocumentCluster(summary, docs, false);
        DocumentCluster cluster2 = new DocumentCluster(summary, docs, false);

        assertEquals(cluster1.hashCode(), cluster2.hashCode());
    }

    public void testSetSummary_updatesCorrectly() {
        Map<String, Float> originalSummaryMap = new HashMap<>();
        originalSummaryMap.put("1", 1.0f);
        SparseVector originalSummary = new SparseVector(originalSummaryMap);

        Map<String, Float> newSummaryMap = new HashMap<>();
        newSummaryMap.put("2", 2.0f);
        SparseVector newSummary = new SparseVector(newSummaryMap);

        List<DocFreq> docs = Arrays.asList(new DocFreq(1, (byte) 1));

        DocumentCluster cluster = new DocumentCluster(originalSummary, docs, false);
        cluster.setSummary(newSummary);

        assertEquals(newSummary, cluster.getSummary());
    }

    public void testSetShouldNotSkip_updatesCorrectly() {
        Map<String, Float> summaryMap = new HashMap<>();
        summaryMap.put("1", 1.0f);
        SparseVector summary = new SparseVector(summaryMap);
        List<DocFreq> docs = Arrays.asList(new DocFreq(1, (byte) 1));

        DocumentCluster cluster = new DocumentCluster(summary, docs, false);
        cluster.setShouldNotSkip(true);

        assertTrue(cluster.isShouldNotSkip());
    }
}
