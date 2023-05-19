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

import lombok.SneakyThrows;

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
        final int maxDocId = TestUtil.nextInt(random(), 10, 10_000);
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

        Weight weight = mock(Weight.class);

        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(
            weight,
            new Scorer[] { scorer(docs, scores, fakeWeight(new MatchAllDocsQuery())) }
        );
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
        assertEquals(numDocs, numOfActualDocs);
    }

    @SneakyThrows
    public void testWithRandomDocuments_whenMultipleScorers_thenReturnSuccessfully() {
        final int maxDocId1 = TestUtil.nextInt(random(), 2, 10_000);
        final int numDocs1 = RandomizedTest.randomIntBetween(1, maxDocId1 / 2);
        final int[] docs1 = new int[numDocs1];
        final Set<Integer> uniqueDocs1 = new HashSet<>();
        while (uniqueDocs1.size() < numDocs1) {
            uniqueDocs1.add(random().nextInt(maxDocId1));
        }
        int i = 0;
        for (int doc : uniqueDocs1) {
            docs1[i++] = doc;
        }
        Arrays.sort(docs1);
        final float[] scores1 = new float[numDocs1];
        for (int j = 0; j < numDocs1; ++j) {
            scores1[j] = random().nextFloat();
        }

        final int maxDocId2 = TestUtil.nextInt(random(), 2, 10_000);
        final int numDocs2 = RandomizedTest.randomIntBetween(1, maxDocId2 / 2);
        final int[] docs2 = new int[numDocs2];
        final Set<Integer> uniqueDocs2 = new HashSet<>();
        while (uniqueDocs2.size() < numDocs2) {
            uniqueDocs2.add(random().nextInt(maxDocId2));
        }
        i = 0;
        for (int doc : uniqueDocs2) {
            docs2[i++] = doc;
        }
        Arrays.sort(docs2);
        final float[] scores2 = new float[numDocs2];
        for (int j = 0; j < numDocs2; ++j) {
            scores2[j] = random().nextFloat();
        }

        Set<Integer> uniqueDocsAll = new HashSet<>();
        uniqueDocsAll.addAll(uniqueDocs1);
        uniqueDocsAll.addAll(uniqueDocs2);

        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(
            mock(Weight.class),
            new Scorer[] {
                scorer(docs1, scores1, fakeWeight(new MatchAllDocsQuery())),
                scorer(docs2, scores2, fakeWeight(new MatchNoDocsQuery())) }
        );
        int doc = -1;
        int numOfActualDocs = 0;
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
        assertEquals(uniqueDocsAll.size(), numOfActualDocs);
    }

    @SneakyThrows
    public void testWithRandomDocuments_whenMultipleScorersAndSomeScorersEmpty_thenReturnSuccessfully() {
        final int maxDocId = TestUtil.nextInt(random(), 10, 10_000);
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

        Weight weight = mock(Weight.class);

        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(
            weight,
            new Scorer[] { null, scorer(docs, scores, fakeWeight(new MatchAllDocsQuery())), null }
        );
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
        assertEquals(numDocs, numOfActualDocs);
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
