/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DisjunctionDISIApproximation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

import lombok.Getter;
import org.apache.lucene.util.PriorityQueue;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

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

    private final DocIdSetIterator approximation;
    HybridScorePropagator disjunctionBlockPropagator;
    private final TwoPhase twoPhase;

    public HybridQueryScorer(Weight weight, List<Scorer> subScorers) throws IOException {
        this(weight, subScorers, ScoreMode.TOP_SCORES);
    }

    public HybridQueryScorer(Weight weight, List<Scorer> subScorers, ScoreMode scoreMode) throws IOException {
        super(weight);
        // max
        this.subScorers = Collections.unmodifiableList(subScorers);
        // custom
        subScores = new float[subScorers.size()];
        this.queryToIndex = mapQueryToIndex();
        // base
        this.subScorersPQ = initializeSubScorersPQ();
        // base
        boolean needsScores = scoreMode != ScoreMode.COMPLETE_NO_SCORES;
        this.approximation = new HybridDisjunctionDISIApproximation(this.subScorersPQ);
        // max
        if (scoreMode == ScoreMode.TOP_SCORES) {
            this.disjunctionBlockPropagator = new HybridScorePropagator(subScorers);
        } else {
            this.disjunctionBlockPropagator = null;
        }
        // base
        boolean hasApproximation = false;
        float sumMatchCost = 0;
        long sumApproxCost = 0;
        // Compute matchCost as the average over the matchCost of the subScorers.
        // This is weighted by the cost, which is an expected number of matching documents.
        for (DisiWrapper w : subScorersPQ) {
            long costWeight = (w.cost <= 1) ? 1 : w.cost;
            sumApproxCost += costWeight;
            if (w.twoPhaseView != null) {
                hasApproximation = true;
                sumMatchCost += w.matchCost * costWeight;
            }
        }
        if (!hasApproximation) { // no sub scorer supports approximations
            twoPhase = null;
        } else {
            final float matchCost = sumMatchCost / sumApproxCost;
            twoPhase = new TwoPhase(approximation, matchCost, subScorersPQ, needsScores);
        }
    }

    @Override
    public int advanceShallow(int target) throws IOException {
        if (disjunctionBlockPropagator != null) {
            return disjunctionBlockPropagator.advanceShallow(target);
        }
        return super.advanceShallow(target);
    }

    /**
     * Returns the score of the current document matching the query. Score is a sum of all scores from sub-query scorers.
     * @return combined total score of all sub-scores
     * @throws IOException
     */
    @Override
    public float score() throws IOException {
        return score(getSubMatches());
    }

    private float score(DisiWrapper topList) throws IOException {
        float totalScore = 0.0f;
        for (DisiWrapper disiWrapper = topList; disiWrapper != null; disiWrapper = disiWrapper.next) {
            // check if this doc has match in the subQuery. If not, add score as 0.0 and continue
            if (disiWrapper.scorer.docID() == NO_MORE_DOCS) {
                continue;
            }
            totalScore += disiWrapper.scorer.score();
        }
        return totalScore;
    }

    DisiWrapper getSubMatches() throws IOException {
        if (twoPhase == null) {
            return subScorersPQ.topList();
        } else {
            return twoPhase.getSubMatches();
        }
    }

    /**
     * Return a DocIdSetIterator over matching documents.
     * @return DocIdSetIterator object
     */
    @Override
    public DocIdSetIterator iterator() {
        if (twoPhase != null) {
            return TwoPhaseIterator.asDocIdSetIterator(twoPhase);
        } else {
            return approximation;
        }
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {
        return twoPhase;
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

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
        if (disjunctionBlockPropagator != null) {
            disjunctionBlockPropagator.setMinCompetitiveScore(minScore);
        }

        for (Scorer scorer : subScorers) {
            if (Objects.nonNull(scorer)) {
                scorer.setMinCompetitiveScore(minScore);
            }
        }
    }

    /**
     * Returns the doc ID that is currently being scored.
     * @return document id
     */
    @Override
    public int docID() {
        if (subScorersPQ.size() == 0) {
            return NO_MORE_DOCS;
        }
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
            // we need to find the index of first sub-query that hasn't been set yet. Such score will have initial value of "0.0"
            int index = indexes.stream()
                .mapToInt(idx -> idx)
                .filter(idx -> Float.compare(scores[idx], 0.0f) == 0)
                .findFirst()
                .orElseThrow(
                    () -> new IllegalStateException(
                        String.format(
                            Locale.ROOT,
                            "cannot set score for one of hybrid search subquery [%s] and document [%d]",
                            query.toString(),
                            scorer.docID()
                        )
                    )
                );
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

    @Override
    public Collection<ChildScorable> getChildren() throws IOException {
        ArrayList<ChildScorable> children = new ArrayList<>();
        for (DisiWrapper scorer = getSubMatches(); scorer != null; scorer = scorer.next) {
            children.add(new ChildScorable(scorer.scorer, "SHOULD"));
        }
        return children;
    }

    static class TwoPhase extends TwoPhaseIterator {
        private final float matchCost;
        // list of verified matches on the current doc
        DisiWrapper verifiedMatches;
        // priority queue of approximations on the current doc that have not been verified yet
        final PriorityQueue<DisiWrapper> unverifiedMatches;
        DisiPriorityQueue subScorers;
        boolean needsScores;

        private TwoPhase(DocIdSetIterator approximation, float matchCost, DisiPriorityQueue subScorers, boolean needsScores) {
            super(approximation);
            this.matchCost = matchCost;
            this.subScorers = subScorers;
            unverifiedMatches = new PriorityQueue<>(subScorers.size()) {
                @Override
                protected boolean lessThan(DisiWrapper a, DisiWrapper b) {
                    return a.matchCost < b.matchCost;
                }
            };
            this.needsScores = needsScores;
        }

        DisiWrapper getSubMatches() throws IOException {
            // iteration order does not matter
            for (DisiWrapper w : unverifiedMatches) {
                if (w.twoPhaseView.matches()) {
                    w.next = verifiedMatches;
                    verifiedMatches = w;
                }
            }
            unverifiedMatches.clear();
            return verifiedMatches;
        }

        @Override
        public boolean matches() throws IOException {
            verifiedMatches = null;
            unverifiedMatches.clear();

            for (DisiWrapper w = subScorers.topList(); w != null;) {
                DisiWrapper next = w.next;

                if (w.twoPhaseView == null) {
                    // implicitly verified, move it to verifiedMatches
                    w.next = verifiedMatches;
                    verifiedMatches = w;

                    if (!needsScores) {
                        // we can stop here
                        return true;
                    }
                } else {
                    unverifiedMatches.add(w);
                }
                w = next;
            }

            if (verifiedMatches != null) {
                return true;
            }

            // verify subs that have an two-phase iterator
            // least-costly ones first
            while (unverifiedMatches.size() > 0) {
                DisiWrapper w = unverifiedMatches.pop();
                if (w.twoPhaseView.matches()) {
                    w.next = null;
                    verifiedMatches = w;
                    return true;
                }
            }

            return false;
        }

        @Override
        public float matchCost() {
            return matchCost;
        }
    }

    static class HybridDisjunctionDISIApproximation extends DocIdSetIterator {
        final DocIdSetIterator delegate;
        final DisiPriorityQueue subIterators;

        public HybridDisjunctionDISIApproximation(DisiPriorityQueue subIterators) {
            delegate = new DisjunctionDISIApproximation(subIterators);
            this.subIterators = subIterators;
        }

        @Override
        public long cost() {
            return delegate.cost();
        }

        @Override
        public int docID() {
            if (subIterators.size() == 0) {
                return NO_MORE_DOCS;
            }
            return delegate.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            if (subIterators.size() == 0) {
                return NO_MORE_DOCS;
            }
            return delegate.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            if (subIterators.size() == 0) {
                return NO_MORE_DOCS;
            }
            return delegate.advance(target);
        }
    }
}
