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
