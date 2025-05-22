/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.Getter;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Bulk scorer for hybrid query
 */
public class HybridBulkScorer extends BulkScorer {
    private static final int SHIFT = 12;
    private static final int WINDOW_SIZE = 1 << SHIFT;
    private static final int MASK = WINDOW_SIZE - 1;

    private final long cost;
    private final Scorer[] scorers;
    @Getter
    private final HybridSubQueryScorer hybridSubQueryScorer;
    private final boolean needsScores;
    @Getter
    private final FixedBitSet matching;
    @Getter
    private final float[][] windowScores;
    private final HybridQueryDocIdStream hybridQueryDocIdStream;
    @Getter
    private final int maxDoc;
    private int[] docIds;

    /**
     * Constructor for HybridBulkScorer
     * @param scorers list of scorers for each sub query
     * @param needsScores whether the scorer needs scores
     * @param maxDoc maximum document id
     */
    public HybridBulkScorer(List<Scorer> scorers, boolean needsScores, int maxDoc) {
        long cost = 0;
        int numOfQueries = scorers.size();
        this.scorers = new Scorer[numOfQueries];
        for (int subQueryIndex = 0; subQueryIndex < numOfQueries; subQueryIndex++) {
            Scorer scorer = scorers.get(subQueryIndex);
            if (Objects.isNull(scorer)) {
                continue;
            }
            cost += scorer.iterator().cost();
            this.scorers[subQueryIndex] = scorer;
        }
        this.cost = cost;
        this.hybridSubQueryScorer = new HybridSubQueryScorer(numOfQueries);
        this.needsScores = needsScores;
        this.matching = new FixedBitSet(WINDOW_SIZE);
        this.windowScores = new float[this.scorers.length][WINDOW_SIZE];
        this.maxDoc = maxDoc;
        this.hybridQueryDocIdStream = new HybridQueryDocIdStream(this);
        this.docIds = new int[numOfQueries];
        Arrays.fill(docIds, DocIdSetIterator.NO_MORE_DOCS);
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
        collector.setScorer(hybridSubQueryScorer);
        // making sure we are not going over the global limit defined by maxDoc
        max = Math.min(max, maxDoc);
        // advance all scorers to the segment's minimum doc id
        advance(min, scorers);
        while (allDocIdsUsed(docIds, max) == false) {
            scoreWindow(collector, acceptDocs, min, max, docIds);
        }
        return getNextDocIdCandidate(docIds);
    }

    private void scoreWindow(LeafCollector collector, Bits acceptDocs, int min, int max, int[] docIds) throws IOException {
        // find the first document ID below the maximum threshold to establish the next scoring window boundary
        int topDoc = -1;
        for (int docId : docIds) {
            if (docId < max) {
                topDoc = docId;
                break;
            }
        }

        final int windowBase = topDoc & ~MASK; // take the next match (at random) and find the window where it belongs
        final int windowMin = Math.max(min, windowBase);
        final int windowMax = Math.min(max, windowBase + WINDOW_SIZE);
        // collect doc ids and scores for this window using leaf collector
        scoreWindowIntoBitSetWithSubqueryScorers(collector, acceptDocs, max, docIds, windowMin, windowMax, windowBase);
    }

    /**
     * Collect scores for the window using segment level leaf collector
     * @param collector leaf collector for the segment
     * @param acceptDocs bitset with live docs
     * @param max max doc id
     * @param docIds last used doc ids per scorer
     * @param windowMin min doc id of this collector window
     * @param windowMax max doc id of this collector window
     * @param windowBase offset for this collector window
     * @throws IOException
     */
    private void scoreWindowIntoBitSetWithSubqueryScorers(
        LeafCollector collector,
        Bits acceptDocs,
        int max,
        int[] docIds,
        int windowMin,
        int windowMax,
        int windowBase
    ) throws IOException {
        for (int subQueryIndex = 0; subQueryIndex < scorers.length; subQueryIndex++) {
            if (Objects.isNull(scorers[subQueryIndex]) || docIds[subQueryIndex] >= max) {
                continue;
            }
            DocIdSetIterator it = scorers[subQueryIndex].iterator();
            int doc = docIds[subQueryIndex];
            if (doc < windowMin) {
                doc = it.advance(windowMin);
            }
            while (doc < windowMax) {
                if (Objects.isNull(acceptDocs) || acceptDocs.get(doc)) {
                    int d = doc & MASK;
                    if (needsScores) {
                        float score = scorers[subQueryIndex].score();
                        // collect score only in case it's gt competitive score
                        if (score > hybridSubQueryScorer.getMinScores()[subQueryIndex]) {
                            matching.set(d);
                            windowScores[subQueryIndex][d] = score;
                        }
                    } else {
                        matching.set(d);
                    }
                }
                doc = it.nextDoc();
            }
            docIds[subQueryIndex] = doc;
        }

        hybridQueryDocIdStream.setBase(windowBase);
        collector.collect(hybridQueryDocIdStream);

        resetWindowState();
    }

    /**
     * Advance all scorers to the next document that is >= min
     */
    private void advance(int min, Scorer[] scorers) throws IOException {
        for (int subQueryIndex = 0; subQueryIndex < scorers.length; subQueryIndex++) {
            if (Objects.isNull(scorers[subQueryIndex])) {
                continue;
            }
            DocIdSetIterator it = scorers[subQueryIndex].iterator();
            int doc = it.docID();
            if (doc < min) {
                doc = it.advance(min);
            }
            docIds[subQueryIndex] = doc;
        }
    }

    private boolean allDocIdsUsed(int[] docsIds, int max) {
        for (int docId : docsIds) {
            if (docId < max) {
                return false;
            }
        }
        return true;
    }

    private int getNextDocIdCandidate(final int[] docsIds) {
        int nextDoc = -1;
        for (int doc : docsIds) {
            if (doc != DocIdSetIterator.NO_MORE_DOCS) {
                nextDoc = Math.max(nextDoc, doc);
            }
        }
        return nextDoc == -1 ? DocIdSetIterator.NO_MORE_DOCS : nextDoc;
    }

    /**
     * Reset the internal state for the next window of documents
     */
    private void resetWindowState() {
        matching.clear();

        for (float[] windowScore : windowScores) {
            Arrays.fill(windowScore, 0.0f);
        }
    }

    @Override
    public long cost() {
        return cost;
    }
}
