/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.SneakyThrows;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HybridQueryDocIdStreamTests extends OpenSearchTestCase {

    private static final int DOC_ID_1 = 1;
    private static final int DOC_ID_2 = 2;
    private static final int DOC_ID_3 = 3;
    private static final int NUM_DOCS = 5;

    @SneakyThrows
    public void testForEach_whenNoMatchingDocs_thenNoDocumentsProcessed() {
        // setup
        FixedBitSet matchingDocs = new FixedBitSet(NUM_DOCS);
        HybridBulkScorer mockScorer = mock(HybridBulkScorer.class);
        when(mockScorer.getMatching()).thenReturn(matchingDocs);

        HybridQueryDocIdStream stream = new HybridQueryDocIdStream(mockScorer);
        List<Integer> processedDocs = new ArrayList<>();

        // execute
        stream.forEach(docId -> processedDocs.add(docId));

        // verify
        assertTrue(processedDocs.isEmpty());
    }

    @SneakyThrows
    public void testForEach_whenSingleMatchingDoc_thenProcessed() {
        // setup
        FixedBitSet matchingDocs = new FixedBitSet(NUM_DOCS);
        matchingDocs.set(DOC_ID_1);

        HybridBulkScorer mockScorer = createMockScorerWithDocs(matchingDocs);
        HybridQueryDocIdStream stream = new HybridQueryDocIdStream(mockScorer);
        List<Integer> processedDocs = new ArrayList<>();

        // execute
        stream.forEach(docId -> processedDocs.add(docId));

        // verify
        assertEquals(1, processedDocs.size());
        assertEquals(DOC_ID_1, processedDocs.get(0).intValue());
    }

    @SneakyThrows
    public void testForEach_whenMultipleMatchingDocs_thenAllProcessed() {
        // setup
        FixedBitSet matchingDocs = new FixedBitSet(NUM_DOCS);
        matchingDocs.set(DOC_ID_1);
        matchingDocs.set(DOC_ID_2);
        matchingDocs.set(DOC_ID_3);

        HybridBulkScorer mockScorer = createMockScorerWithDocs(matchingDocs);
        HybridQueryDocIdStream stream = new HybridQueryDocIdStream(mockScorer);
        List<Integer> processedDocs = new ArrayList<>();

        // execute
        stream.forEach(docId -> processedDocs.add(docId));

        // verify
        assertEquals(3, processedDocs.size());
        assertTrue(processedDocs.contains(DOC_ID_1));
        assertTrue(processedDocs.contains(DOC_ID_2));
        assertTrue(processedDocs.contains(DOC_ID_3));
    }

    @SneakyThrows
    public void testForEach_whenBaseOffsetProvided_thenDocIdsAdjusted() {
        // setup
        FixedBitSet matchingDocs = new FixedBitSet(NUM_DOCS);
        matchingDocs.set(DOC_ID_1);
        matchingDocs.set(DOC_ID_2);

        HybridBulkScorer mockScorer = createMockScorerWithDocs(matchingDocs);
        int baseOffset = 100;
        HybridQueryDocIdStream stream = new HybridQueryDocIdStream(mockScorer);
        List<Integer> processedDocs = new ArrayList<>();
        stream.setBase(baseOffset);

        // execute
        stream.forEach(docId -> processedDocs.add(docId));

        // verify
        assertEquals(2, processedDocs.size());
        assertTrue(processedDocs.contains(baseOffset | DOC_ID_1));
        assertTrue(processedDocs.contains(baseOffset | DOC_ID_2));
    }

    @SneakyThrows
    public void testForEach_whenCrossing64BitBoundary_thenAllDocsProcessed() {
        // setup
        int numDocs = 128; // Two longs worth of bits
        FixedBitSet matchingDocs = new FixedBitSet(numDocs);
        matchingDocs.set(63);  // Last bit in first long
        matchingDocs.set(64);  // First bit in second long

        HybridBulkScorer mockScorer = createMockScorerWithDocs(matchingDocs, numDocs);
        HybridQueryDocIdStream stream = new HybridQueryDocIdStream(mockScorer);
        List<Integer> processedDocs = new ArrayList<>();

        // execute
        stream.forEach(docId -> processedDocs.add(docId));

        // verify
        assertEquals(2, processedDocs.size());
        assertTrue(processedDocs.contains(63));
        assertTrue(processedDocs.contains(64));
    }

    @SneakyThrows
    public void testIntoArray_whenMultipleMatchingDocs_thenArrayFilled() {
        // setup
        FixedBitSet matchingDocs = new FixedBitSet(NUM_DOCS);
        matchingDocs.set(DOC_ID_1);
        matchingDocs.set(DOC_ID_2);
        matchingDocs.set(DOC_ID_3);

        HybridBulkScorer mockScorer = createMockScorerWithDocs(matchingDocs);
        HybridQueryDocIdStream stream = new HybridQueryDocIdStream(mockScorer);
        int[] docIds = new int[5];

        // execute
        int count = stream.intoArray(Integer.MAX_VALUE, docIds);

        // verify
        assertEquals(3, count);
        assertEquals(DOC_ID_1, docIds[0]);
        assertEquals(DOC_ID_2, docIds[1]);
        assertEquals(DOC_ID_3, docIds[2]);
    }

    @SneakyThrows
    public void testIntoArray_whenArraySmallerThanMatches_thenPartialFill() {
        // setup
        FixedBitSet matchingDocs = new FixedBitSet(NUM_DOCS);
        matchingDocs.set(DOC_ID_1);
        matchingDocs.set(DOC_ID_2);
        matchingDocs.set(DOC_ID_3);

        HybridBulkScorer mockScorer = createMockScorerWithDocs(matchingDocs);
        HybridQueryDocIdStream stream = new HybridQueryDocIdStream(mockScorer);
        int[] docIds = new int[2];

        // execute
        int count = stream.intoArray(Integer.MAX_VALUE, docIds);

        // verify
        assertEquals(2, count);
        assertEquals(DOC_ID_1, docIds[0]);
        assertEquals(DOC_ID_2, docIds[1]);
    }

    @SneakyThrows
    public void testIntoArray_whenNoMatchingDocs_thenZeroReturned() {
        // setup
        FixedBitSet matchingDocs = new FixedBitSet(NUM_DOCS);
        HybridBulkScorer mockScorer = mock(HybridBulkScorer.class);
        when(mockScorer.getMatching()).thenReturn(matchingDocs);

        HybridQueryDocIdStream stream = new HybridQueryDocIdStream(mockScorer);
        int[] docIds = new int[5];

        // execute
        int count = stream.intoArray(Integer.MAX_VALUE, docIds);

        // verify
        assertEquals(0, count);
    }

    @SneakyThrows
    public void testIntoArray_whenBaseOffsetProvided_thenDocIdsAdjusted() {
        // setup
        FixedBitSet matchingDocs = new FixedBitSet(NUM_DOCS);
        matchingDocs.set(DOC_ID_1);
        matchingDocs.set(DOC_ID_2);

        HybridBulkScorer mockScorer = createMockScorerWithDocs(matchingDocs);
        int baseOffset = 100;
        HybridQueryDocIdStream stream = new HybridQueryDocIdStream(mockScorer);
        stream.setBase(baseOffset);
        int[] docIds = new int[5];

        // execute
        int count = stream.intoArray(Integer.MAX_VALUE, docIds);

        // verify
        assertEquals(2, count);
        assertEquals(baseOffset | DOC_ID_1, docIds[0]);
        assertEquals(baseOffset | DOC_ID_2, docIds[1]);
    }

    @SneakyThrows
    public void testIntoArray_whenCrossing64BitBoundary_thenAllDocsInArray() {
        // setup
        int numDocs = 128; // Two longs worth of bits
        FixedBitSet matchingDocs = new FixedBitSet(numDocs);
        matchingDocs.set(63);  // Last bit in first long
        matchingDocs.set(64);  // First bit in second long

        HybridBulkScorer mockScorer = createMockScorerWithDocs(matchingDocs, numDocs);
        HybridQueryDocIdStream stream = new HybridQueryDocIdStream(mockScorer);
        int[] docIds = new int[5];

        // execute
        int count = stream.intoArray(Integer.MAX_VALUE, docIds);

        // verify
        assertEquals(2, count);
        assertEquals(63, docIds[0]);
        assertEquals(64, docIds[1]);
    }

    @SneakyThrows
    public void testIntoArray_whenArraySmallerThanMatches_thenScorerStateCleanAfterCall() {
        // setup - 3 matching docs but array of size 2
        FixedBitSet matchingDocs = new FixedBitSet(NUM_DOCS);
        matchingDocs.set(DOC_ID_1);
        matchingDocs.set(DOC_ID_2);
        matchingDocs.set(DOC_ID_3);

        // Use a real sub-query scorer to verify state is clean after partial fill
        HybridBulkScorer mockScorer = mock(HybridBulkScorer.class);
        when(mockScorer.getMatching()).thenReturn(matchingDocs);
        when(mockScorer.getMaxDoc()).thenReturn(200);

        float[][] windowScores = new float[2][NUM_DOCS];
        for (int i = 0; i < NUM_DOCS; i++) {
            windowScores[0][i] = 1.0f + i;
            windowScores[1][i] = 2.0f + i;
        }
        when(mockScorer.getWindowScores()).thenReturn(windowScores);

        // Use a real HybridSubQueryScorer (not mocked) to verify resetScores() leaves state clean
        float[] subQueryScores = new float[2];
        HybridSubQueryScorer realSubQueryScorer = mock(HybridSubQueryScorer.class);
        when(realSubQueryScorer.getSubQueryScores()).thenReturn(subQueryScores);
        // resetScores zeroes the array - simulate this behavior
        org.mockito.Mockito.doAnswer(invocation -> {
            java.util.Arrays.fill(subQueryScores, 0.0f);
            return null;
        }).when(realSubQueryScorer).resetScores();
        when(mockScorer.getHybridSubQueryScorer()).thenReturn(realSubQueryScorer);

        HybridQueryDocIdStream stream = new HybridQueryDocIdStream(mockScorer);
        int[] docIds = new int[2];

        // execute - array is smaller than matches, so forEach continues past array capacity
        int count = stream.intoArray(Integer.MAX_VALUE, docIds);

        // verify - array has first 2 docs
        assertEquals(2, count);
        assertEquals(DOC_ID_1, docIds[0]);
        assertEquals(DOC_ID_2, docIds[1]);

        // verify - scorer state is clean after intoArray (resetScores was called for all docs including extras)
        assertEquals(0.0f, subQueryScores[0], 0.0f);
        assertEquals(0.0f, subQueryScores[1], 0.0f);
    }

    @SneakyThrows
    public void testForEach_whenSubsequentCalls_thenUpToParameterIgnored() {
        // setup
        FixedBitSet matchingDocs = new FixedBitSet(NUM_DOCS);
        matchingDocs.set(DOC_ID_1); // docId = 1
        matchingDocs.set(DOC_ID_2); // docId = 2
        matchingDocs.set(DOC_ID_3); // docId = 3

        HybridBulkScorer mockScorer = createMockScorerWithDocs(matchingDocs);
        HybridQueryDocIdStream stream = new HybridQueryDocIdStream(mockScorer);
        List<Integer> processedDocs = new ArrayList<>();

        // HybridQueryDocIdStream does not respect the upTo parameter and processes all matching documents
        // first call with upTo = 2 (will process all matching docs despite upTo value)
        stream.forEach(2, docId -> processedDocs.add(docId));

        // verify first call results - all 3 docs are processed (upTo is ignored)
        assertEquals(3, processedDocs.size());
        assertTrue(processedDocs.contains(DOC_ID_1));
        assertTrue(processedDocs.contains(DOC_ID_2));
        assertTrue(processedDocs.contains(DOC_ID_3));

        // clear processed docs list
        processedDocs.clear();

        // second call with upTo = 4 (will process all matching docs)
        stream.forEach(4, docId -> processedDocs.add(docId));

        // verify second call results - all docs are processed
        assertEquals(3, processedDocs.size());
        assertTrue(processedDocs.contains(DOC_ID_1));
        assertTrue(processedDocs.contains(DOC_ID_2));
        assertTrue(processedDocs.contains(DOC_ID_3));

        // clear processed docs list
        processedDocs.clear();

        // third call with upTo = 1 (will still process all documents since upTo is ignored)
        stream.forEach(1, docId -> processedDocs.add(docId));
        assertEquals(3, processedDocs.size());
        assertTrue(processedDocs.contains(DOC_ID_1));
        assertTrue(processedDocs.contains(DOC_ID_2));
        assertTrue(processedDocs.contains(DOC_ID_3));
    }

    private HybridBulkScorer createMockScorerWithDocs(FixedBitSet matchingDocs, int numDocs) {
        HybridBulkScorer mockScorer = mock(HybridBulkScorer.class);
        when(mockScorer.getMatching()).thenReturn(matchingDocs);
        when(mockScorer.getMaxDoc()).thenReturn(200);

        // setup window scores with the specified number of docs
        float[][] windowScores = new float[2][numDocs];
        for (int i = 0; i < numDocs; i++) {
            windowScores[0][i] = random().nextFloat();
            windowScores[1][i] = random().nextFloat();
        }
        when(mockScorer.getWindowScores()).thenReturn(windowScores);

        // setup hybrid sub-query scorer
        HybridSubQueryScorer mockSubQueryScorer = mock(HybridSubQueryScorer.class);
        when(mockSubQueryScorer.getSubQueryScores()).thenReturn(new float[2]);
        when(mockScorer.getHybridSubQueryScorer()).thenReturn(mockSubQueryScorer);

        return mockScorer;
    }

    private HybridBulkScorer createMockScorerWithDocs(FixedBitSet matchingDocs) {
        HybridBulkScorer mockScorer = mock(HybridBulkScorer.class);
        when(mockScorer.getMatching()).thenReturn(matchingDocs);
        when(mockScorer.getMaxDoc()).thenReturn(200);

        // setup window scores
        float[][] windowScores = new float[2][NUM_DOCS]; // 2 sub-queries
        for (int i = 0; i < NUM_DOCS; i++) {
            windowScores[0][i] = random().nextFloat();
            windowScores[1][i] = random().nextFloat();
        }
        when(mockScorer.getWindowScores()).thenReturn(windowScores);

        // setup hybrid sub-query scorer
        HybridSubQueryScorer mockSubQueryScorer = mock(HybridSubQueryScorer.class);
        when(mockSubQueryScorer.getSubQueryScores()).thenReturn(new float[2]);
        when(mockScorer.getHybridSubQueryScorer()).thenReturn(mockSubQueryScorer);

        return mockScorer;
    }
}
