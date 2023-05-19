/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.SneakyThrows;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

import com.carrotsearch.randomizedtesting.RandomizedTest;

public class HybridQueryScorerTests extends LuceneTestCase {

    @SneakyThrows
    public void testWithRandomDocuments_whenOneSubScorer_thenReturnSuccessfully() {
        int maxDocId = TestUtil.nextInt(random(), 10, 10_000);
        Pair<int[], float[]> docsAndScores = generateDocuments(maxDocId);
        int[] docs = docsAndScores.getLeft();
        float[] scores = docsAndScores.getRight();

        Weight weight = mock(Weight.class);

        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(
            weight,
            new Scorer[] { scorer(docsAndScores.getLeft(), docsAndScores.getRight(), fakeWeight(new MatchAllDocsQuery())) }
        );

        testWithQuery(docs, scores, hybridQueryScorer);
    }

    @SneakyThrows
    public void testWithRandomDocumentsAndHybridScores_whenMultipleScorers_thenReturnSuccessfully() {
        int maxDocId1 = TestUtil.nextInt(random(), 10, 10_000);
        Pair<int[], float[]> docsAndScores1 = generateDocuments(maxDocId1);
        int[] docs1 = docsAndScores1.getLeft();
        float[] scores1 = docsAndScores1.getRight();
        int maxDocId2 = TestUtil.nextInt(random(), 10, 10_000);
        Pair<int[], float[]> docsAndScores2 = generateDocuments(maxDocId2);
        int[] docs2 = docsAndScores2.getLeft();
        float[] scores2 = docsAndScores2.getRight();

        Weight weight = mock(Weight.class);

        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(
            weight,
            new Scorer[] {
                scorer(docs1, scores1, fakeWeight(new MatchAllDocsQuery())),
                scorer(docs2, scores2, fakeWeight(new MatchNoDocsQuery())) }
        );
        int doc = -1;
        int numOfActualDocs = 0;
        Set<Integer> uniqueDocs1 = Arrays.stream(docs1).boxed().collect(Collectors.toSet());
        Set<Integer> uniqueDocs2 = Arrays.stream(docs2).boxed().collect(Collectors.toSet());
        while (doc != NO_MORE_DOCS) {
            doc = hybridQueryScorer.iterator().nextDoc();
            if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                continue;
            }
            float[] actualTotalScores = hybridQueryScorer.hybridScores();
            float actualTotalScore = 0.0f;
            for (float score : actualTotalScores) {
                actualTotalScore += score;
            }
            float expectedScore = 0.0f;
            if (uniqueDocs1.contains(doc)) {
                int idx = Arrays.binarySearch(docs1, doc);
                expectedScore += scores1[idx];
            }
            if (uniqueDocs2.contains(doc)) {
                int idx = Arrays.binarySearch(docs2, doc);
                expectedScore += scores2[idx];
            }
            assertEquals(expectedScore, actualTotalScore, 0.001f);
            numOfActualDocs++;
        }

        int totalUniqueCount = uniqueDocs1.size();
        for (int n : uniqueDocs2) {
            if (!uniqueDocs1.contains(n)) {
                totalUniqueCount++;
            }
        }
        assertEquals(totalUniqueCount, numOfActualDocs);
    }

    @SneakyThrows
    public void testWithRandomDocumentsAndCombinedScore_whenMultipleScorers_thenReturnSuccessfully() {
        int maxDocId1 = TestUtil.nextInt(random(), 10, 10_000);
        Pair<int[], float[]> docsAndScores1 = generateDocuments(maxDocId1);
        int[] docs1 = docsAndScores1.getLeft();
        float[] scores1 = docsAndScores1.getRight();
        int maxDocId2 = TestUtil.nextInt(random(), 10, 10_000);
        Pair<int[], float[]> docsAndScores2 = generateDocuments(maxDocId2);
        int[] docs2 = docsAndScores2.getLeft();
        float[] scores2 = docsAndScores2.getRight();

        Weight weight = mock(Weight.class);

        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(
            weight,
            new Scorer[] {
                scorer(docs1, scores1, fakeWeight(new MatchAllDocsQuery())),
                scorer(docs2, scores2, fakeWeight(new MatchNoDocsQuery())) }
        );
        int doc = -1;
        int numOfActualDocs = 0;
        Set<Integer> uniqueDocs1 = Arrays.stream(docs1).boxed().collect(Collectors.toSet());
        Set<Integer> uniqueDocs2 = Arrays.stream(docs2).boxed().collect(Collectors.toSet());
        while (doc != NO_MORE_DOCS) {
            doc = hybridQueryScorer.iterator().nextDoc();
            if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                continue;
            }
            float expectedScore = 0.0f;
            if (uniqueDocs1.contains(doc)) {
                int idx = Arrays.binarySearch(docs1, doc);
                expectedScore += scores1[idx];
            }
            if (uniqueDocs2.contains(doc)) {
                int idx = Arrays.binarySearch(docs2, doc);
                expectedScore += scores2[idx];
            }
            assertEquals(expectedScore, hybridQueryScorer.score(), 0.001f);
            numOfActualDocs++;
        }

        int totalUniqueCount = uniqueDocs1.size();
        for (int n : uniqueDocs2) {
            if (!uniqueDocs1.contains(n)) {
                totalUniqueCount++;
            }
        }
        assertEquals(totalUniqueCount, numOfActualDocs);
    }

    @SneakyThrows
    public void testWithRandomDocuments_whenMultipleScorersAndSomeScorersEmpty_thenReturnSuccessfully() {
        int maxDocId = TestUtil.nextInt(random(), 10, 10_000);
        Pair<int[], float[]> docsAndScores = generateDocuments(maxDocId);
        int[] docs = docsAndScores.getLeft();
        float[] scores = docsAndScores.getRight();

        Weight weight = mock(Weight.class);

        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(
            weight,
            new Scorer[] { null, scorer(docs, scores, fakeWeight(new MatchAllDocsQuery())), null }
        );

        testWithQuery(docs, scores, hybridQueryScorer);
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

    private static Weight fakeWeight(Query query) {
        return new Weight(query) {

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                return null;
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                return null;
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }

    private static DocIdSetIterator iterator(final int... docs) {
        return new DocIdSetIterator() {

            int i = -1;

            @Override
            public int nextDoc() throws IOException {
                if (i + 1 == docs.length) {
                    return NO_MORE_DOCS;
                } else {
                    return docs[++i];
                }
            }

            @Override
            public int docID() {
                return i < 0 ? -1 : i == docs.length ? NO_MORE_DOCS : docs[i];
            }

            @Override
            public long cost() {
                return docs.length;
            }

            @Override
            public int advance(int target) throws IOException {
                return slowAdvance(target);
            }
        };
    }

    private static Scorer scorer(final int[] docs, final float[] scores, Weight weight) {
        final DocIdSetIterator iterator = iterator(docs);
        return new Scorer(weight) {

            int lastScoredDoc = -1;

            public DocIdSetIterator iterator() {
                return iterator;
            }

            @Override
            public int docID() {
                return iterator.docID();
            }

            @Override
            public float score() throws IOException {
                assertNotEquals("score() called twice on doc " + docID(), lastScoredDoc, docID());
                lastScoredDoc = docID();
                final int idx = Arrays.binarySearch(docs, docID());
                return scores[idx];
            }

            @Override
            public float getMaxScore(int upTo) throws IOException {
                return Float.MAX_VALUE;
            }
        };
    }
}
