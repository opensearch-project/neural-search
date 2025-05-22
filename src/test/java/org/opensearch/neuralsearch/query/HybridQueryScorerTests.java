/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.tests.util.TestUtil;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import lombok.SneakyThrows;
import org.opensearch.neuralsearch.search.HybridDisiWrapper;

public class HybridQueryScorerTests extends OpenSearchQueryTestCase {
    private static final int NUM_SUB_QUERIES = 2;
    private static final int DOC_ID_1 = 1;

    @SneakyThrows
    public void testWithRandomDocuments_whenOneSubScorer_thenReturnSuccessfully() {
        int maxDocId = TestUtil.nextInt(random(), 10, 10_000);
        Pair<int[], float[]> docsAndScores = generateDocuments(maxDocId);
        int[] docs = docsAndScores.getLeft();
        float[] scores = docsAndScores.getRight();

        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(
            Arrays.asList(scorer(docsAndScores.getLeft(), docsAndScores.getRight(), fakeWeight(new MatchAllDocsQuery())))
        );

        testWithQuery(docs, scores, hybridQueryScorer);
    }

    @SneakyThrows
    public void testWithRandomDocuments_whenMultipleScorersAndSomeScorersEmpty_thenReturnSuccessfully() {
        int maxDocId = TestUtil.nextInt(random(), 10, 10_000);
        Pair<int[], float[]> docsAndScores = generateDocuments(maxDocId);
        int[] docs = docsAndScores.getLeft();
        float[] scores = docsAndScores.getRight();

        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(
            Arrays.asList(null, scorer(docs, scores, fakeWeight(new MatchAllDocsQuery())), null)
        );

        testWithQuery(docs, scores, hybridQueryScorer);
    }

    @SneakyThrows
    public void testMaxScore_whenMultipleScorers_thenSuccessful() {
        int maxDocId = TestUtil.nextInt(random(), 10, 10_000);
        Pair<int[], float[]> docsAndScores = generateDocuments(maxDocId);
        int[] docs = docsAndScores.getLeft();
        float[] scores = docsAndScores.getRight();

        HybridQueryScorer hybridQueryScorerWithAllNonNullSubScorers = new HybridQueryScorer(
            Arrays.asList(
                scorer(docs, scores, fakeWeight(new MatchAllDocsQuery())),
                scorer(docs, scores, fakeWeight(new MatchNoDocsQuery()))
            )
        );

        float maxScore = hybridQueryScorerWithAllNonNullSubScorers.getMaxScore(Integer.MAX_VALUE);
        assertTrue(maxScore > 0.0f);

        HybridQueryScorer hybridQueryScorerWithSomeNullSubScorers = new HybridQueryScorer(
            Arrays.asList(null, scorer(docs, scores, fakeWeight(new MatchAllDocsQuery())), null)
        );

        maxScore = hybridQueryScorerWithSomeNullSubScorers.getMaxScore(Integer.MAX_VALUE);
        assertTrue(maxScore > 0.0f);
    }

    @SneakyThrows
    public void testMaxScoreFailures_whenScorerThrowsException_thenFail() {
        int maxDocId = TestUtil.nextInt(random(), 10, 10_000);
        Pair<int[], float[]> docsAndScores = generateDocuments(maxDocId);
        int[] docs = docsAndScores.getLeft();

        Scorer scorer = mock(Scorer.class);
        when(scorer.iterator()).thenReturn(iterator(docs));
        when(scorer.getMaxScore(anyInt())).thenThrow(new IOException("Test exception"));

        IOException runtimeException = expectThrows(IOException.class, () -> new HybridQueryScorer(Arrays.asList(scorer)));
        assertTrue(runtimeException.getMessage().contains("Test exception"));
    }

    @SneakyThrows
    public void testApproximationIterator_whenSubScorerSupportsApproximation_thenSuccessful() {
        final int maxDoc = TestUtil.nextInt(random(), 10, 1_000);
        final int numDocs = TestUtil.nextInt(random(), 1, maxDoc / 2);
        final Set<Integer> uniqueDocs = new HashSet<>();
        while (uniqueDocs.size() < numDocs) {
            uniqueDocs.add(random().nextInt(maxDoc));
        }
        final int[] docs = new int[numDocs];
        int i = 0;
        for (int doc : uniqueDocs) {
            docs[i++] = doc;
        }
        Arrays.sort(docs);
        final float[] scores1 = new float[numDocs];
        for (i = 0; i < numDocs; ++i) {
            scores1[i] = random().nextFloat();
        }
        final float[] scores2 = new float[numDocs];
        for (i = 0; i < numDocs; ++i) {
            scores2[i] = random().nextFloat();
        }

        HybridQueryScorer queryScorer = new HybridQueryScorer(
            Arrays.asList(
                scorerWithTwoPhaseIterator(docs, scores1, fakeWeight(new MatchAllDocsQuery()), maxDoc),
                scorerWithTwoPhaseIterator(docs, scores2, fakeWeight(new MatchNoDocsQuery()), maxDoc)
            )
        );

        int doc = -1;
        int idx = 0;
        while (doc != DocIdSetIterator.NO_MORE_DOCS) {
            doc = queryScorer.iterator().nextDoc();
            if (idx == docs.length) {
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, doc);
            } else {
                assertEquals(docs[idx], doc);
                assertEquals(scores1[idx] + scores2[idx], queryScorer.score(), DELTA_FOR_SCORE_ASSERTION);
            }
            idx++;
        }
    }

    @SneakyThrows
    public void testScore_whenMultipleSubScorers_thenSumScores() {
        // Create mock scorers with iterators
        Scorer scorer1 = mock(Scorer.class);
        DocIdSetIterator iterator1 = mock(DocIdSetIterator.class);
        when(scorer1.iterator()).thenReturn(iterator1);
        when(scorer1.docID()).thenReturn(1);
        when(scorer1.score()).thenReturn(0.5f);

        Scorer scorer2 = mock(Scorer.class);
        DocIdSetIterator iterator2 = mock(DocIdSetIterator.class);
        when(scorer2.iterator()).thenReturn(iterator2);
        when(scorer2.docID()).thenReturn(1);
        when(scorer2.score()).thenReturn(0.3f);

        // Create DisiWrapper list
        DisiWrapper wrapper1 = new DisiWrapper(scorer1, false);
        wrapper1.next = new DisiWrapper(scorer2, false);

        HybridQueryScorer hybridScorer = new HybridQueryScorer(Arrays.asList(scorer1, scorer2));
        float score = hybridScorer.score(wrapper1);

        assertEquals("Combined score should be sum of individual scores", 0.8f, score, DELTA_FOR_SCORE_ASSERTION);
    }

    @SneakyThrows
    public void testScore_whenNoMoreDocs_thenReturnZero() {
        // Create mock scorer
        Scorer scorer = mock(Scorer.class);
        DocIdSetIterator iterator = mock(DocIdSetIterator.class);
        when(scorer.iterator()).thenReturn(iterator);

        // Setup iterator behavior
        when(iterator.docID()).thenReturn(DocIdSetIterator.NO_MORE_DOCS);
        when(iterator.cost()).thenReturn(1L);

        // Create TwoPhaseIterator if needed
        TwoPhaseIterator twoPhase = mock(TwoPhaseIterator.class);
        DocIdSetIterator approximation = mock(DocIdSetIterator.class);
        when(approximation.docID()).thenReturn(DocIdSetIterator.NO_MORE_DOCS);
        when(approximation.cost()).thenReturn(1L);
        when(twoPhase.approximation()).thenReturn(approximation);
        when(scorer.twoPhaseIterator()).thenReturn(twoPhase);

        // Create wrapper
        DisiWrapper wrapper = new DisiWrapper(scorer, false);

        // Create HybridQueryScorer
        HybridQueryScorer hybridScorer = new HybridQueryScorer(Collections.singletonList(scorer));

        // Test score method
        float score = hybridScorer.score(wrapper);

        // Verify
        assertEquals("Score should be 0.0 for NO_MORE_DOCS", 0.0f, score, DELTA_FOR_SCORE_ASSERTION);
    }

    @SneakyThrows
    public void testGetSubMatches_whenNoScorers_thenReturnNull() {
        // Create a scorer with a two-phase iterator that doesn't match
        Scorer scorer = mock(Scorer.class);
        DocIdSetIterator iterator = mock(DocIdSetIterator.class);
        TwoPhaseIterator twoPhase = mock(TwoPhaseIterator.class);
        when(twoPhase.matches()).thenReturn(false);
        when(scorer.twoPhaseIterator()).thenReturn(twoPhase);
        when(scorer.iterator()).thenReturn(iterator);
        when(scorer.docID()).thenReturn(0);  // Set a valid docID

        HybridQueryScorer hybridScorer = new HybridQueryScorer(Collections.singletonList(scorer), ScoreMode.TOP_SCORES);

        DisiWrapper result = hybridScorer.getSubMatches();
        assertNull("Should return null when no matches are available", result);
    }

    @SneakyThrows
    public void testGetSubMatches_whenTwoPhaseIteratorPresent_thenReturnWrapper() {
        // Create scorer with iterator
        Scorer scorer = mock(Scorer.class);
        DocIdSetIterator iterator = mock(DocIdSetIterator.class);

        // Setup iterator behavior with AtomicInteger for state tracking
        AtomicInteger currentDoc = new AtomicInteger(-1);

        when(iterator.docID()).thenAnswer(inv -> currentDoc.get());
        when(iterator.cost()).thenReturn(1L);
        when(iterator.nextDoc()).thenAnswer(inv -> {
            if (currentDoc.get() == -1) {
                currentDoc.set(0);
                return 0;
            }
            return DocIdSetIterator.NO_MORE_DOCS;
        });

        when(scorer.iterator()).thenReturn(iterator);
        when(scorer.docID()).thenAnswer(inv -> currentDoc.get());

        // Create and setup TwoPhaseIterator
        TwoPhaseIterator twoPhase = mock(TwoPhaseIterator.class);
        DocIdSetIterator approximation = mock(DocIdSetIterator.class);

        // Setup approximation behavior
        when(approximation.docID()).thenAnswer(inv -> currentDoc.get());
        when(approximation.cost()).thenReturn(1L);
        when(approximation.nextDoc()).thenAnswer(inv -> {
            if (currentDoc.get() == -1) {
                currentDoc.set(0);
                return 0;
            }
            return DocIdSetIterator.NO_MORE_DOCS;
        });

        when(twoPhase.approximation()).thenReturn(approximation);
        when(scorer.twoPhaseIterator()).thenReturn(twoPhase);
        when(twoPhase.matches()).thenReturn(true);

        // Create HybridQueryScorer
        HybridQueryScorer hybridScorer = new HybridQueryScorer(Collections.singletonList(scorer), ScoreMode.TOP_SCORES);

        // Initialize the scorer by moving to first doc
        DocIdSetIterator scorerIterator = hybridScorer.iterator();
        int firstDoc = scorerIterator.nextDoc();

        // Verify initial state
        assertEquals("First doc should be 0", 0, firstDoc);
        assertEquals("Iterator should be at doc 0", 0, scorerIterator.docID());

        // Get submatches
        DisiWrapper result = hybridScorer.getSubMatches();

        // Verify
        assertNotNull("Should not be null when twoPhase is present", result);
        assertTrue("Should be instance of HybridDisiWrapper", result instanceof HybridDisiWrapper);
        assertNotNull("TwoPhaseView should not be null", result.twoPhaseView);
        assertEquals("Should be at doc 0", 0, result.doc);

        // Verify the two-phase iterator
        TwoPhaseIterator resultTwoPhase = result.twoPhaseView;
        assertNotNull("Two-phase iterator should not be null", resultTwoPhase);
        assertTrue("Should match", resultTwoPhase.matches());
    }

    @SneakyThrows
    public void testAdvanceShallow_whenTargetProvided_thenReturnTarget() {
        // Create scorer
        Scorer scorer = mock(Scorer.class);
        DocIdSetIterator iterator = mock(DocIdSetIterator.class);
        when(scorer.iterator()).thenReturn(iterator);

        // Create and setup TwoPhaseIterator
        TwoPhaseIterator twoPhase = mock(TwoPhaseIterator.class);
        DocIdSetIterator approximation = mock(DocIdSetIterator.class);
        when(twoPhase.approximation()).thenReturn(approximation);
        when(scorer.twoPhaseIterator()).thenReturn(twoPhase);
        when(twoPhase.matches()).thenReturn(true);

        // Setup initial state
        AtomicInteger currentDoc = new AtomicInteger(-1);

        // Setup iterator behavior
        when(iterator.docID()).thenAnswer(inv -> currentDoc.get());
        when(approximation.docID()).thenAnswer(inv -> currentDoc.get());

        // Setup nextDoc behavior
        when(iterator.nextDoc()).thenAnswer(inv -> {
            currentDoc.set(0);
            return 0;
        });

        when(approximation.nextDoc()).thenAnswer(inv -> {
            currentDoc.set(0);
            return 0;
        });

        // Setup advance behavior
        int target = 5;
        when(approximation.advance(target)).thenAnswer(inv -> {
            currentDoc.set(target);
            return target;
        });

        when(iterator.advance(target)).thenAnswer(inv -> {
            currentDoc.set(target);
            return target;
        });

        // Setup costs
        when(iterator.cost()).thenReturn(1L);
        when(approximation.cost()).thenReturn(1L);

        // Create hybrid scorer with custom advanceShallow implementation
        HybridQueryScorer hybridScorer = new HybridQueryScorer(Collections.singletonList(scorer), ScoreMode.TOP_SCORES) {
            @Override
            public float score() throws IOException {
                return 1.0f;
            }

            @Override
            public int advanceShallow(int target) throws IOException {
                DisiWrapper lead = getSubMatches();
                if (lead != null && lead.twoPhaseView != null) {
                    DocIdSetIterator approx = lead.twoPhaseView.approximation();
                    int result = approx.advance(target);
                    return result;
                }
                return 0;
            }
        };

        // Initialize scorer
        DocIdSetIterator scorerIterator = hybridScorer.iterator();

        // Move to first doc
        int firstDoc = scorerIterator.nextDoc();
        assertEquals("Should be at first doc", 0, scorerIterator.docID());

        // Test advanceShallow
        int result = hybridScorer.advanceShallow(target);

        // Verify
        assertEquals("AdvanceShallow should return the target", target, result);
        verify(approximation).advance(target);
        assertEquals("Current doc should be at target", target, currentDoc.get());
    }

    @SneakyThrows
    public void testScore_whenMultipleQueries_thenCombineScores() {
        // Create mock scorers for different queries
        Scorer boolScorer = mock(Scorer.class);
        DocIdSetIterator boolIterator = mock(DocIdSetIterator.class);
        when(boolScorer.iterator()).thenReturn(boolIterator);
        when(boolScorer.docID()).thenReturn(1);
        when(boolScorer.score()).thenReturn(0.7f);

        Scorer neuralScorer = mock(Scorer.class);
        DocIdSetIterator neuralIterator = mock(DocIdSetIterator.class);
        when(neuralScorer.iterator()).thenReturn(neuralIterator);
        when(neuralScorer.docID()).thenReturn(1);
        when(neuralScorer.score()).thenReturn(0.9f);

        // Create DisiWrapper chain
        DisiWrapper boolWrapper = new DisiWrapper(boolScorer, false);
        DisiWrapper neuralWrapper = new DisiWrapper(neuralScorer, false);
        boolWrapper.next = neuralWrapper;

        HybridQueryScorer hybridScorer = new HybridQueryScorer(Arrays.asList(boolScorer, neuralScorer), ScoreMode.COMPLETE);
        float combinedScore = hybridScorer.score(boolWrapper);

        assertEquals("Combined score should be sum of bool and neural scores", 1.6f, combinedScore, DELTA_FOR_SCORE_ASSERTION);
    }

    @SneakyThrows
    public void testInitialization_whenValidScorer_thenSuccessful() {
        // Create scorer with iterator
        Scorer scorer = mock(Scorer.class);
        DocIdSetIterator iterator = mock(DocIdSetIterator.class);

        // Setup state tracking
        AtomicInteger currentDoc = new AtomicInteger(-1);

        // Setup iterator behavior
        when(iterator.docID()).thenAnswer(inv -> currentDoc.get());
        when(iterator.cost()).thenReturn(1L);
        when(iterator.nextDoc()).thenAnswer(inv -> {
            if (currentDoc.get() == -1) {
                currentDoc.set(0);
                return 0;
            }
            return DocIdSetIterator.NO_MORE_DOCS;
        });

        when(scorer.iterator()).thenReturn(iterator);
        when(scorer.docID()).thenAnswer(inv -> currentDoc.get());

        // Create wrapper
        HybridDisiWrapper wrapper = new HybridDisiWrapper(scorer, 1);

        // Verify
        assertNotNull("Wrapper should not be null", wrapper);
        assertEquals("Initial doc should be -1", -1, wrapper.doc);
        assertNotNull("Iterator should not be null", wrapper.iterator);
        assertEquals("Cost should be 1", 1L, wrapper.cost);
    }

    @SneakyThrows
    public void testTwoPhaseIterator_withNestedTwoPhaseQuery() {
        // Create a scorer that uses two-phase iteration
        Scorer scorer = mock(Scorer.class);
        TwoPhaseIterator twoPhaseIterator = mock(TwoPhaseIterator.class);
        DocIdSetIterator approximation = mock(DocIdSetIterator.class);

        // Setup the two-phase behavior
        when(scorer.twoPhaseIterator()).thenReturn(twoPhaseIterator);
        when(twoPhaseIterator.approximation()).thenReturn(approximation);
        when(twoPhaseIterator.matches()).thenReturn(true);

        // Mock iterator() method which is needed for cost calculation
        when(scorer.iterator()).thenReturn(approximation);
        // Mock cost to avoid NPE
        when(approximation.cost()).thenReturn(1L);

        // Create wrapper
        HybridDisiWrapper wrapper = new HybridDisiWrapper(scorer, 1);

        // This would return null before PR #998
        TwoPhaseIterator wrapperTwoPhase = wrapper.twoPhaseView;
        assertNotNull("Two-phase iterator should not be null", wrapperTwoPhase);

        // Verify that the two-phase behavior is preserved
        assertTrue("Should match", wrapperTwoPhase.matches());
        assertSame("Should use same approximation", approximation, wrapperTwoPhase.approximation());
    }

    @SneakyThrows
    public void testScore_whenNeedScoresIsFalse_thenReturnsZero() {
        List<Scorer> scorers = new ArrayList<>();
        Scorer mockScorer = mock(Scorer.class);
        DocIdSetIterator mockIterator = mock(DocIdSetIterator.class);

        when(mockScorer.iterator()).thenReturn(mockIterator);
        when(mockIterator.cost()).thenReturn(1L);
        scorers.add(mockScorer);

        HybridQueryScorer scorer = new HybridQueryScorer(scorers, ScoreMode.COMPLETE_NO_SCORES);

        float score = scorer.score();

        assertEquals(0.0f, score, 0.0f);
    }

    protected static Scorer scorerWithTwoPhaseIterator(final int[] docs, final float[] scores, Weight weight, int maxDoc) {
        final DocIdSetIterator iterator = DocIdSetIterator.all(maxDoc);
        return new Scorer() {

            int lastScoredDoc = -1;

            public DocIdSetIterator iterator() {
                return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
            }

            @Override
            public int docID() {
                return iterator.docID();
            }

            @Override
            public float score() {
                assertNotEquals("score() called twice on doc " + docID(), lastScoredDoc, docID());
                lastScoredDoc = docID();
                final int idx = Arrays.binarySearch(docs, docID());
                return scores[idx];
            }

            @Override
            public float getMaxScore(int upTo) {
                return Float.MAX_VALUE;
            }

            @Override
            public TwoPhaseIterator twoPhaseIterator() {
                return new TwoPhaseIterator(iterator) {

                    @Override
                    public boolean matches() {
                        return Arrays.binarySearch(docs, iterator.docID()) >= 0;
                    }

                    @Override
                    public float matchCost() {
                        return 10;
                    }
                };
            }
        };
    }

    private Pair<int[], float[]> generateDocuments(int maxDocId) {
        final int numDocs = RandomizedTest.randomIntBetween(1, maxDocId / 2);
        final int[] docs = new int[numDocs];
        final Set<Integer> uniqueDocs = new HashSet<>();
        while (uniqueDocs.size() < numDocs) {
            uniqueDocs.add(random().nextInt(maxDocId));
        }
        int i = 0;
        for (int doc : uniqueDocs) {
            docs[i++] = doc;
        }
        Arrays.sort(docs);
        final float[] scores = new float[numDocs];
        for (int j = 0; j < numDocs; ++j) {
            scores[j] = random().nextFloat();
        }
        return new ImmutablePair<>(docs, scores);
    }

    private void testWithQuery(int[] docs, float[] scores, HybridQueryScorer hybridQueryScorer) throws IOException {
        int doc = -1;
        int numOfActualDocs = 0;
        while (doc != NO_MORE_DOCS) {
            int target = doc + 1;
            doc = hybridQueryScorer.iterator().nextDoc();
            int idx = Arrays.binarySearch(docs, target);
            idx = (idx >= 0) ? idx : (-1 - idx);
            if (idx == docs.length) {
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, doc);
            } else {
                assertEquals(docs[idx], doc);
                assertEquals(scores[idx], hybridQueryScorer.score(), 0f);
                numOfActualDocs++;
            }
        }
        assertEquals(docs.length, numOfActualDocs);
    }
}
