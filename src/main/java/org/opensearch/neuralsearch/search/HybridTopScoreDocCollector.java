/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search;

import java.io.IOException;
import java.util.Locale;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.HitQueue;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.neuralsearch.query.HybridQueryScorer;

/**
 * Collects the TopDocs after executing hybrid query. Uses HybridQueryTopDocs as DTO to handle each sub query results
 */
@Log4j2
public class HybridTopScoreDocCollector implements Collector {
    int docBase;
    float minCompetitiveScore;
    final HitsThresholdChecker hitsThresholdChecker;
    ScoreDoc pqTop;
    protected TotalHits.Relation totalHitsRelation = TotalHits.Relation.EQUAL_TO;
    protected int[] totalHits;
    public static final TopDocs EMPTY_TOPDOCS = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
    int numOfHits;

    @Getter
    PriorityQueue<ScoreDoc>[] compoundScores;

    public HybridTopScoreDocCollector(int numHits, HitsThresholdChecker hitsThresholdChecker) {
        numOfHits = numHits;
        this.hitsThresholdChecker = hitsThresholdChecker;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        docBase = context.docBase;
        minCompetitiveScore = 0f;

        return new TopScoreDocCollector.ScorerLeafCollector() {
            HybridQueryScorer compoundQueryScorer;

            @Override
            public void setScorer(Scorable scorer) throws IOException {
                super.setScorer(scorer);
                updateMinCompetitiveScore(scorer);
                compoundQueryScorer = (HybridQueryScorer) scorer;
            }

            @Override
            public void collect(int doc) throws IOException {
                float[] subScoresByQuery = compoundQueryScorer.hybridScores();
                // iterate over results for each query
                if (compoundScores == null) {
                    compoundScores = new PriorityQueue[subScoresByQuery.length];
                    for (int i = 0; i < subScoresByQuery.length; i++) {
                        compoundScores[i] = new HitQueue(numOfHits, true);
                    }
                    totalHits = new int[subScoresByQuery.length];
                }
                for (int i = 0; i < subScoresByQuery.length; i++) {
                    float score = subScoresByQuery[i];
                    // if score is 0.0 there is no hits for that sub-query
                    if (score == 0) {
                        continue;
                    }
                    totalHits[i]++;
                    PriorityQueue<ScoreDoc> pq = compoundScores[i];
                    ScoreDoc topDoc = pq.top();
                    topDoc.doc = doc + docBase;
                    topDoc.score = score;
                    pq.updateTop();
                }
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return hitsThresholdChecker.scoreMode();
    }

    protected void updateMinCompetitiveScore(Scorable scorer) throws IOException {
        if (hitsThresholdChecker.isThresholdReached() && pqTop != null && pqTop.score != Float.NEGATIVE_INFINITY) { // -Infinity is the
                                                                                                                    // boundary score
            // we have multiple identical doc id and collect in doc id order, we need next float
            float localMinScore = Math.nextUp(pqTop.score);
            if (localMinScore > minCompetitiveScore) {
                scorer.setMinCompetitiveScore(localMinScore);
                totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
                minCompetitiveScore = localMinScore;
            }
        }
    }

    public TopDocs[] topDocs() {
        TopDocs[] topDocs = new TopDocs[compoundScores.length];
        for (int i = 0; i < compoundScores.length; i++) {
            int qTopSize = totalHits[i];
            TopDocs topDocsPerQuery = topDocsPerQuery(0, Math.min(qTopSize, compoundScores[i].size()), compoundScores[i], qTopSize);
            topDocs[i] = topDocsPerQuery;
        }
        return topDocs;
    }

    TopDocs topDocsPerQuery(int start, int howMany, PriorityQueue<ScoreDoc> pq, int totalHits) {
        int size = howMany;

        if (howMany < 0) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Number of hits requested must be greater than 0 but value was %d", howMany)
            );
        }

        if (start < 0) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Expected value of starting position is between 0 and %d, got %d", size, start)
            );
        }

        if (start >= size || howMany == 0) {
            return EMPTY_TOPDOCS;
        }

        howMany = Math.min(size - start, howMany);
        ScoreDoc[] results = new ScoreDoc[howMany];
        // pq's pop() returns the 'least' element in the queue, therefore need
        // to discard the first ones, until we reach the requested range.
        for (int i = pq.size() - start - howMany; i > 0; i--) {
            pq.pop();
        }

        // Get the requested results from pq.
        populateResults(results, howMany, pq);

        return new TopDocs(new TotalHits(totalHits, totalHitsRelation), results);
    }

    protected void populateResults(ScoreDoc[] results, int howMany, PriorityQueue<ScoreDoc> pq) {
        for (int i = howMany - 1; i >= 0; i--) {
            results[i] = pq.pop();
        }
    }
}
