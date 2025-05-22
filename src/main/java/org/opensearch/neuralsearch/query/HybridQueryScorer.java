/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DisjunctionDISIApproximation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.neuralsearch.search.HybridDisiWrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Class abstracts functionality of Scorer for hybrid query. When iterating over documents in increasing
 * order of doc id, this class fills up array of scores per sub-query for each doc id. Order in array of scores
 * corresponds to order of sub-queries in an input Hybrid query.
 */
@Log4j2
public class HybridQueryScorer extends Scorer {

    // score for each of sub-query in this hybrid query
    @Getter
    private final List<Scorer> subScorers;

    private final DisiPriorityQueue subScorersPQ;

    private final DocIdSetIterator approximation;
    private final HybridScoreBlockBoundaryPropagator disjunctionBlockPropagator;
    private final TwoPhase twoPhase;
    private final int numSubqueries;

    public HybridQueryScorer(final List<Scorer> subScorers) throws IOException {
        this(subScorers, ScoreMode.TOP_SCORES);
    }

    HybridQueryScorer(final List<Scorer> subScorers, final ScoreMode scoreMode) throws IOException {
        super();
        this.subScorers = Collections.unmodifiableList(subScorers);
        this.numSubqueries = subScorers.size();
        List<HybridDisiWrapper> hybridDisiWrappers = initializeSubScorersList();
        if (hybridDisiWrappers.isEmpty()) {
            throw new IllegalArgumentException("There must be at least 1 subScorers");
        }
        this.subScorersPQ = DisiPriorityQueue.ofMaxSize(numSubqueries);
        this.subScorersPQ.addAll(hybridDisiWrappers.toArray(new DisiWrapper[0]), 0, hybridDisiWrappers.size());
        boolean needsScores = scoreMode != ScoreMode.COMPLETE_NO_SCORES;

        this.approximation = new HybridSubqueriesDISIApproximation(hybridDisiWrappers, subScorersPQ);

        if (scoreMode == ScoreMode.TOP_SCORES) {
            this.disjunctionBlockPropagator = new HybridScoreBlockBoundaryPropagator(subScorers);
        } else {
            this.disjunctionBlockPropagator = null;
        }

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
        if (hasApproximation == false) { // no sub scorer supports approximations
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

    @VisibleForTesting
    float score(DisiWrapper topList) throws IOException {
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
            return DocIdSetIterator.NO_MORE_DOCS;
        }
        return subScorersPQ.top().doc;
    }

    private List<HybridDisiWrapper> initializeSubScorersList() {
        Objects.requireNonNull(subScorers, "should not be null");
        // we need to count this way in order to include all identical sub-queries
        List<HybridDisiWrapper> hybridDisiWrappers = new ArrayList<>();
        for (int idx = 0; idx < numSubqueries; idx++) {
            Scorer scorer = subScorers.get(idx);
            if (scorer == null) {
                continue;
            }
            final HybridDisiWrapper disiWrapper = new HybridDisiWrapper(scorer, idx);
            hybridDisiWrappers.add(disiWrapper);

        }
        return hybridDisiWrappers;
    }

    @Override
    public Collection<ChildScorable> getChildren() throws IOException {
        ArrayList<ChildScorable> children = new ArrayList<>();
        for (DisiWrapper scorer = getSubMatches(); scorer != null; scorer = scorer.next) {
            children.add(new ChildScorable(scorer.scorer, "SHOULD"));
        }
        return children;
    }

    /**
     *  Object returned by {@link Scorer#twoPhaseIterator()} to provide an approximation of a {@link DocIdSetIterator}.
     *  After calling {@link DocIdSetIterator#nextDoc()} or {@link DocIdSetIterator#advance(int)} on the iterator
     *  returned by approximation(), you need to check {@link TwoPhaseIterator#matches()} to confirm if the retrieved
     *  document ID is a match. Implementation inspired by identical class for
     *  <a href="https://github.com/apache/lucene/blob/branch_9_10/lucene/core/src/java/org/apache/lucene/search/DisjunctionScorer.java">DisjunctionScorer</a>
     */
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
            for (DisiWrapper wrapper : unverifiedMatches) {
                if (wrapper.twoPhaseView.matches()) {
                    wrapper.next = verifiedMatches;
                    verifiedMatches = wrapper;
                }
            }
            unverifiedMatches.clear();
            return verifiedMatches;
        }

        @Override
        public boolean matches() throws IOException {
            verifiedMatches = null;
            unverifiedMatches.clear();

            for (DisiWrapper wrapper = subScorers.topList(); wrapper != null;) {
                DisiWrapper next = wrapper.next;

                if (Objects.isNull(wrapper.twoPhaseView)) {
                    // implicitly verified, move it to verifiedMatches
                    wrapper.next = verifiedMatches;
                    verifiedMatches = wrapper;

                    if (needsScores == false) {
                        // we can stop here
                        return true;
                    }
                } else {
                    unverifiedMatches.add(wrapper);
                }
                wrapper = next;
            }

            if (Objects.nonNull(verifiedMatches)) {
                return true;
            }

            // verify subs that have an two-phase iterator
            // least-costly ones first
            while (unverifiedMatches.size() > 0) {
                DisiWrapper wrapper = unverifiedMatches.pop();
                if (wrapper.twoPhaseView.matches()) {
                    wrapper.next = null;
                    verifiedMatches = wrapper;
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

    /**
     * A DocIdSetIterator which is a disjunction of the approximations of the provided iterators and supports
     * sub iterators that return empty results
     */
    static class HybridSubqueriesDISIApproximation extends DocIdSetIterator {
        final DocIdSetIterator docIdSetIterator;
        final DisiPriorityQueue subIterators;

        public HybridSubqueriesDISIApproximation(
            final Collection<? extends DisiWrapper> subIterators,
            final DisiPriorityQueue subIteratorsPQ
        ) {
            docIdSetIterator = new DisjunctionDISIApproximation(subIterators, 0);
            this.subIterators = subIteratorsPQ;
        }

        @Override
        public long cost() {
            return docIdSetIterator.cost();
        }

        @Override
        public int docID() {
            if (subIterators.size() == 0) {
                return NO_MORE_DOCS;
            }
            return docIdSetIterator.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            if (subIterators.size() == 0) {
                return NO_MORE_DOCS;
            }
            return docIdSetIterator.nextDoc();
        }

        @Override
        public int advance(final int target) throws IOException {
            if (subIterators.size() == 0) {
                return NO_MORE_DOCS;
            }
            return docIdSetIterator.advance(target);
        }
    }
}
