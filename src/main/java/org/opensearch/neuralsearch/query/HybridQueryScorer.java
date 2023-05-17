/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;

import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DisjunctionDISIApproximation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/**
 * Class abstracts functionality of Scorer for hybrid query. When iterating over documents in increasing
 * order of doc id, this class fills up array of scores per sub-query for each doc id. Order in array of scores
 * corresponds to order of sub-queries in an input Hybrid query.
 */
public class HybridQueryScorer extends Scorer {

    // score for each of sub-query in this hybrid query
    @Getter
    private final Scorer[] subScorers;

    private final DisiPriorityQueue subScorersPQ;

    private final DocIdSetIterator approximation;

    Map<Query, Integer> queryToIndex;

    HybridQueryScorer(Weight weight, Scorer[] subScorers) throws IOException {
        super(weight);
        this.subScorers = subScorers;
        queryToIndex = new HashMap<>();
        int idx = 0;
        int size = 0;
        for (Scorer scorer : subScorers) {
            if (scorer == null) {
                idx++;
                continue;
            }
            queryToIndex.put(scorer.getWeight().getQuery(), idx);
            idx++;
            size++;
        }
        this.subScorersPQ = new DisiPriorityQueue(size);
        for (Scorer scorer : subScorers) {
            if (scorer == null) {
                continue;
            }
            final DisiWrapper w = new DisiWrapper(scorer);
            this.subScorersPQ.add(w);
        }
        this.approximation = new DisjunctionDISIApproximation(this.subScorersPQ);
    }

    public float score() throws IOException {
        float scoreMax = 0;
        double otherScoreSum = 0;
        for (DisiWrapper w : subScorersPQ) {
            // check if this doc has match in the subQuery. If not, add score as 0.0 and continue
            if (w.scorer.docID() == DocIdSetIterator.NO_MORE_DOCS) {
                continue;
            }
            float subScore = w.scorer.score();
            if (subScore >= scoreMax) {
                otherScoreSum += scoreMax;
                scoreMax = subScore;
            } else {
                otherScoreSum += subScore;
            }
        }
        return (float) (scoreMax + otherScoreSum);
    }

    @Override
    public DocIdSetIterator iterator() {
        return approximation;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        float scoreMax = 0;
        for (Scorer scorer : subScorers) {
            if (scorer.docID() <= upTo) {
                float subScore = scorer.getMaxScore(upTo);
                if (subScore >= scoreMax) {
                    scoreMax = subScore;
                }
            }
        }

        return scoreMax;
    }

    @Override
    public int docID() {
        return subScorersPQ.top().doc;
    }
}
