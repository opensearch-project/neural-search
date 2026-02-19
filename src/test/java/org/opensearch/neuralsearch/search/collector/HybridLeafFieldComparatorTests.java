/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.opensearch.neuralsearch.query.HybridSubQueryScorer;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class HybridLeafFieldComparatorTests extends OpenSearchTestCase {

    /**
     * Tests that the wrapper correctly intercepts and returns individual sub-query scores
     * instead of the sum from HybridSubQueryScorer.
     */
    public void testSetCurrentSubQueryScore_returnsIndividualScore() throws IOException {
        // Create a mock HybridSubQueryScorer that returns sum of scores
        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(2);
        hybridScorer.getSubQueryScores()[0] = 0.9f;
        hybridScorer.getSubQueryScores()[1] = 0.5f;
        // hybridScorer.score() would return 1.4 (sum)

        // Create a test comparator that captures the score
        TestRelevanceComparator testComparator = new TestRelevanceComparator(5);

        // Wrap it with HybridLeafFieldComparator
        HybridLeafFieldComparator wrapper = new HybridLeafFieldComparator(testComparator);

        // Set the individual sub-query score
        wrapper.setCurrentSubQueryScore(0.9f);

        // Set the scorer (this should wrap it)
        wrapper.setScorer(hybridScorer);

        // Verify the wrapped scorer returns individual score, not sum
        assertEquals(0.9f, testComparator.capturedScorer.score(), 0.0001f);

        // Verify the original scorer still returns sum
        assertEquals(1.4f, hybridScorer.score(), 0.0001f);
    }

    /**
     * Tests that copy() uses the individual sub-query score set via setCurrentSubQueryScore.
     */
    public void testCopy_usesIndividualScore() throws IOException {
        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(2);
        hybridScorer.getSubQueryScores()[0] = 0.9f;
        hybridScorer.getSubQueryScores()[1] = 0.5f;

        TestRelevanceComparator testComparator = new TestRelevanceComparator(5);
        HybridLeafFieldComparator wrapper = new HybridLeafFieldComparator(testComparator);

        // Set individual score for sub-query 0
        wrapper.setCurrentSubQueryScore(0.9f);
        wrapper.setScorer(hybridScorer);

        // Copy should use 0.9, not 1.4
        wrapper.copy(0, 100);

        assertEquals(0.9f, testComparator.scores[0], 0.0001f);
    }

    /**
     * Tests that different sub-query scores can be set and used correctly.
     */
    public void testMultipleSubQueryScores() throws IOException {
        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(3);
        hybridScorer.getSubQueryScores()[0] = 0.9f;
        hybridScorer.getSubQueryScores()[1] = 0.5f;
        hybridScorer.getSubQueryScores()[2] = 0.3f;

        TestRelevanceComparator testComparator = new TestRelevanceComparator(5);
        HybridLeafFieldComparator wrapper = new HybridLeafFieldComparator(testComparator);
        wrapper.setScorer(hybridScorer);

        // Copy scores for different sub-queries
        wrapper.setCurrentSubQueryScore(0.9f);
        wrapper.copy(0, 100);

        wrapper.setCurrentSubQueryScore(0.5f);
        wrapper.copy(1, 101);

        wrapper.setCurrentSubQueryScore(0.3f);
        wrapper.copy(2, 102);

        // Verify each slot has the correct individual score
        assertEquals(0.9f, testComparator.scores[0], 0.0001f);
        assertEquals(0.5f, testComparator.scores[1], 0.0001f);
        assertEquals(0.3f, testComparator.scores[2], 0.0001f);
    }

    /**
     * Tests that delegate methods are properly forwarded.
     */
    public void testDelegateMethods() throws IOException {
        TestRelevanceComparator testComparator = new TestRelevanceComparator(5);
        HybridLeafFieldComparator wrapper = new HybridLeafFieldComparator(testComparator);

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(1);
        hybridScorer.getSubQueryScores()[0] = 0.8f;

        wrapper.setCurrentSubQueryScore(0.8f);
        wrapper.setScorer(hybridScorer);
        wrapper.copy(0, 100);

        // Test setBottom
        wrapper.setBottom(0);
        assertEquals(0.8f, testComparator.bottom, 0.0001f);

        // Test compareBottom (should delegate)
        wrapper.copy(1, 101);
        wrapper.setCurrentSubQueryScore(0.6f);
        int result = wrapper.compareBottom(101);
        assertTrue(result != 0); // Should compare 0.8 vs 0.6
    }

    /**
     * Tests that the wrapper works correctly when score is updated multiple times
     * for the same slot (simulating document replacement in queue).
     */
    public void testScoreUpdateForSameSlot() throws IOException {
        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(2);
        hybridScorer.getSubQueryScores()[0] = 0.5f;
        hybridScorer.getSubQueryScores()[1] = 0.3f;

        TestRelevanceComparator testComparator = new TestRelevanceComparator(3);
        HybridLeafFieldComparator wrapper = new HybridLeafFieldComparator(testComparator);
        wrapper.setScorer(hybridScorer);

        // Initial copy with score 0.5
        wrapper.setCurrentSubQueryScore(0.5f);
        wrapper.copy(0, 100);
        assertEquals(0.5f, testComparator.scores[0], 0.0001f);

        // Update same slot with higher score 0.9
        hybridScorer.getSubQueryScores()[0] = 0.9f;
        wrapper.setCurrentSubQueryScore(0.9f);
        wrapper.copy(0, 101);
        assertEquals(0.9f, testComparator.scores[0], 0.0001f);
    }

    /**
     * Test comparator that mimics RelevanceComparator behavior for testing.
     */
    private static class TestRelevanceComparator implements LeafFieldComparator {
        final float[] scores;
        float bottom;
        Scorable capturedScorer;

        TestRelevanceComparator(int numHits) {
            this.scores = new float[numHits];
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            this.capturedScorer = scorer;
        }

        @Override
        public void copy(int slot, int doc) throws IOException {
            scores[slot] = capturedScorer.score();
        }

        @Override
        public void setBottom(int slot) throws IOException {
            this.bottom = scores[slot];
        }

        @Override
        public int compareBottom(int doc) throws IOException {
            float score = capturedScorer.score();
            return Float.compare(score, bottom);
        }

        @Override
        public int compareTop(int doc) throws IOException {
            return 0;
        }
    }
}
