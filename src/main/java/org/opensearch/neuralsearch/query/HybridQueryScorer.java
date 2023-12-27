/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DisjunctionDISIApproximation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import lombok.Getter;

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

    private final Map<Query, List<Integer>> queryToIndex;

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
        return subScorers.stream().filter(Objects::nonNull).filter(scorer -> scorer.docID() <= upTo).map(scorer -> {
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
            Scorer scorer = disiWrapper.scorer;
            if (scorer.docID() == DocIdSetIterator.NO_MORE_DOCS) {
                continue;
            }
            Query query = scorer.getWeight().getQuery();
            List<Integer> indexes = queryToIndex.get(query);
            // we need to find the index of first sub-query that hasn't been updated yet
            int index = indexes.stream()
                .mapToInt(idx -> idx)
                .filter(index1 -> Float.compare(scores[index1], 0.0f) == 0)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("cannot collect score for subquery"));
            scores[index] = scorer.score();
        }
        return scores;
    }

    private Map<Query, List<Integer>> mapQueryToIndex() {
        Map<Query, List<Integer>> queryToIndex = new HashMap<>();
        int idx = 0;
        for (Scorer scorer : subScorers) {
            if (scorer == null) {
                idx++;
                continue;
            }
            Query query = scorer.getWeight().getQuery();
            queryToIndex.putIfAbsent(query, new ArrayList<>());
            queryToIndex.get(query).add(idx);
            idx++;
        }
        return queryToIndex;
    }

    private DisiPriorityQueue initializeSubScorersPQ() {
        Objects.requireNonNull(queryToIndex, "should not be null");
        Objects.requireNonNull(subScorers, "should not be null");
        // we need to count this way in order to include all identical sub-queries
        int numOfSubQueries = queryToIndex.values().stream().map(List::size).reduce(0, Integer::sum);
        DisiPriorityQueue subScorersPQ = new DisiPriorityQueue(numOfSubQueries);
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
