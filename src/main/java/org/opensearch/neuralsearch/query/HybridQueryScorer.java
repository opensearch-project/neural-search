/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
public final class HybridQueryScorer extends Scorer {

    // score for each of sub-query in this hybrid query
    @Getter
    private final List<Scorer> subScorers;

    private final DisiPriorityQueue subScorersPQ;

    private final float[] subScores;

    private final Map<Query, Integer> queryToIndex;

    public HybridQueryScorer(Weight weight, List<Scorer> subScorers) throws IOException {
        super(weight);
        this.subScorers = Collections.unmodifiableList(subScorers);
        subScores = new float[subScorers.size()];
        this.queryToIndex = mapQueryToIndex();
        this.subScorersPQ = initializeSubScorersPQ();
    }

    /**
     * Returns the score of the current document matching the query. Score is a sum of all scores from sub-query scorers.
     * @return combined total score of all sub-scores
     * @throws IOException
     */
    @Override
    public float score() throws IOException {
        DisiWrapper topList = subScorersPQ.topList();
        float totalScore = 0.0f;
        for (DisiWrapper disiWrapper = topList; disiWrapper != null; disiWrapper = disiWrapper.next) {
            // check if this doc has match in the subQuery. If not, add score as 0.0 and continue
            if (disiWrapper.scorer.docID() == DocIdSetIterator.NO_MORE_DOCS) {
                continue;
            }
            totalScore += disiWrapper.scorer.score();
        }
        return totalScore;
    }

    /**
     * Return a DocIdSetIterator over matching documents.
     * @return DocIdSetIterator object
     */
    @Override
    public DocIdSetIterator iterator() {
        return new DisjunctionDISIApproximation(this.subScorersPQ);
    }

    /**
     * Return the maximum score that documents between the last target that this iterator was shallow-advanced to included and upTo included.
     * @param upTo upper limit for document id
     * @return max score
     * @throws IOException
     */
    @Override
    public float getMaxScore(int upTo) throws IOException {
        return subScorers.stream().filter(scorer -> scorer.docID() <= upTo).map(scorer -> {
            try {
                return scorer.getMaxScore(upTo);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).max(Float::compare).orElse(0.0f);
    }

    /**
     * Returns the doc ID that is currently being scored.
     * @return document id
     */
    @Override
    public int docID() {
        return subScorersPQ.top().doc;
    }

    /**
     * Return array of scores per sub-query for doc id that is defined by current iterator position
     * @return
     * @throws IOException
     */
    public float[] hybridScores() throws IOException {
        float[] scores = new float[subScores.length];
        DisiWrapper topList = subScorersPQ.topList();
        for (DisiWrapper disiWrapper = topList; disiWrapper != null; disiWrapper = disiWrapper.next) {
            // check if this doc has match in the subQuery. If not, add score as 0.0 and continue
            if (disiWrapper.scorer.docID() == DocIdSetIterator.NO_MORE_DOCS) {
                continue;
            }
            float subScore = disiWrapper.scorer.score();
            scores[queryToIndex.get(disiWrapper.scorer.getWeight().getQuery())] = subScore;
        }
        return scores;
    }

    private Map<Query, Integer> mapQueryToIndex() {
        Map<Query, Integer> queryToIndex = new HashMap<>();
        int idx = 0;
        for (Scorer scorer : subScorers) {
            if (scorer == null) {
                idx++;
                continue;
            }
            queryToIndex.put(scorer.getWeight().getQuery(), idx);
            idx++;
        }
        return queryToIndex;
    }

    private DisiPriorityQueue initializeSubScorersPQ() {
        Objects.requireNonNull(queryToIndex, "should not be null");
        Objects.requireNonNull(subScorers, "should not be null");
        DisiPriorityQueue subScorersPQ = new DisiPriorityQueue(queryToIndex.size());
        for (Scorer scorer : subScorers) {
            if (scorer == null) {
                continue;
            }
            final DisiWrapper w = new DisiWrapper(scorer);
            subScorersPQ.add(w);
        }
        return subScorersPQ;
    }
}
