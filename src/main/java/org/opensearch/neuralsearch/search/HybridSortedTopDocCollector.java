/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Locale;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.FieldValueHitQueue;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.common.Nullable;
import org.opensearch.neuralsearch.query.HybridQueryScorer;

@Log4j2
public abstract class HybridSortedTopDocCollector implements Collector {

    public static class SimpleFieldCollector extends HybridSortedTopDocCollector {
        final Sort sort;
        final int numHits;
        private int[] totalHits;
        // private final int numHits;
        @Getter
        private FieldValueHitQueue<FieldValueHitQueue.Entry>[] compoundScores;
        final HitsThresholdChecker hitsThresholdChecker;
        @Getter
        @Setter
        private TotalHits.Relation totalHitsRelation = TotalHits.Relation.EQUAL_TO;
        private static final TopFieldDocs EMPTY_TOPDOCS = new TopFieldDocs(
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[0],
            new SortField[0]
        );
        int docBase;
        final MaxScoreAccumulator minScoreAcc;
        FieldValueHitQueue.Entry bottom = null;
        boolean queueFull[];
        Boolean searchSortPartOfIndexSort = null;

        public SimpleFieldCollector(
            int numHits,
            HitsThresholdChecker hitsThresholdChecker,
            Sort sort,
            MaxScoreAccumulator minScoreAcc
        ) {
            this.sort = sort;
            this.numHits = numHits;
            this.hitsThresholdChecker = hitsThresholdChecker;
            this.minScoreAcc = minScoreAcc;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            docBase = context.docBase;

            return new LeafCollector() {
                LeafFieldComparator comparators[];
                int reverseMul;
                HybridQueryScorer compoundQueryScorer;
                boolean collectedAllCompetitiveHits = false;
                // FieldComparator<?> firstComparator;
                boolean canSetMinScore;
                float minCompetitiveScore;

                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    if (scorer instanceof HybridQueryScorer) {
                        log.debug("passed scorer is of type HybridQueryScorer, saving it for collecting documents and scores");
                        compoundQueryScorer = (HybridQueryScorer) scorer;
                    } else {
                        compoundQueryScorer = getHybridQueryScorer(scorer);
                        if (Objects.isNull(compoundQueryScorer)) {
                            log.error(
                                String.format(
                                    Locale.ROOT,
                                    "cannot find scorer of type HybridQueryScorer in a hierarchy of scorer %s",
                                    scorer
                                )
                            );
                        }
                    }
                }

                private HybridQueryScorer getHybridQueryScorer(final Scorable scorer) throws IOException {
                    if (scorer == null) {
                        return null;
                    }
                    if (scorer instanceof HybridQueryScorer) {
                        return (HybridQueryScorer) scorer;
                    }
                    for (Scorable.ChildScorable childScorable : scorer.getChildren()) {
                        HybridQueryScorer hybridQueryScorer = getHybridQueryScorer(childScorable.child);
                        if (Objects.nonNull(hybridQueryScorer)) {
                            log.debug(
                                String.format(
                                    Locale.ROOT,
                                    "found hybrid query scorer, it's child of scorer %s",
                                    childScorable.child.getClass().getSimpleName()
                                )
                            );
                            return hybridQueryScorer;
                        }
                    }
                    return null;
                }

                @Override
                public void collect(int doc) throws IOException {
                    if (Objects.isNull(compoundQueryScorer)) {
                        throw new IllegalArgumentException("scorers are null for all sub-queries in hybrid query");
                    }
                    float[] subScoresByQuery = compoundQueryScorer.hybridScores();
                    // iterate over results for each query
                    if (compoundScores == null) {
                        compoundScores = new FieldValueHitQueue[subScoresByQuery.length];
                        this.comparators = new LeafFieldComparator[subScoresByQuery.length];
                        queueFull = new boolean[subScoresByQuery.length];
                        for (int i = 0; i < subScoresByQuery.length; i++) {
                            // compoundScores[i] = new HitQueue(numHits, true);
                            compoundScores[i] = FieldValueHitQueue.create(sort.getSort(), numHits);
                            FieldComparator<?> firstComparator = compoundScores[i].getComparators()[0];
                            if (searchSortPartOfIndexSort == null) {
                                final Sort indexSort = context.reader().getMetaData().getSort();
                                searchSortPartOfIndexSort = canEarlyTerminate(sort, indexSort);
                                if (searchSortPartOfIndexSort) {
                                    firstComparator.disableSkipping();
                                }
                            }
                            int reverseMul = compoundScores[i].getReverseMul()[0];
                            if (firstComparator.getClass().equals(FieldComparator.RelevanceComparator.class)
                                && reverseMul == 1 // if the natural sort is preserved (sort by descending relevance)
                                && hitsThresholdChecker.getTotalHitsThreshold() != Integer.MAX_VALUE) {
                                canSetMinScore = true;
                            } else {
                                canSetMinScore = false;
                            }
                            compoundScores[i].getComparators()[0].setSingleSort();
                            LeafFieldComparator[] comparators = compoundScores[i].getComparators(context);
                            int[] reverseMuls = compoundScores[i].getReverseMul();
                            if (comparators.length == 1) {
                                this.reverseMul = reverseMuls[0];
                                this.comparators[i] = comparators[0];
                            } else {
                                this.reverseMul = 1;
                                this.comparators[i] = new MultiLeafFieldComparator(comparators, reverseMuls);
                            }
                            this.comparators[i].setScorer(compoundQueryScorer);
                        }
                        totalHits = new int[subScoresByQuery.length];
                    }
                    for (int i = 0; i < subScoresByQuery.length; i++) {
                        minCompetitiveScore = 0f;
                        float score = subScoresByQuery[i];
                        // if score is 0.0 there is no hits for that sub-query
                        if (score == 0) {
                            continue;
                        }
                        totalHits[i]++;
                        hitsThresholdChecker.incrementHitCount();
                        if (minScoreAcc != null && (totalHits[i] & minScoreAcc.modInterval) == 0) {
                            updateGlobalMinCompetitiveScore(compoundQueryScorer);
                        }
                        if (scoreMode().isExhaustive() == false
                            && getTotalHitsRelation() == TotalHits.Relation.EQUAL_TO
                            && hitsThresholdChecker.isThresholdReached()) {
                            // for the first time hitsThreshold is reached, notify comparator about this
                            comparators[i].setHitsThresholdReached();
                            setTotalHitsRelation(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
                        }

                        if (queueFull[i]) {
                            if (thresholdCheck(doc, i, compoundScores[i])) {
                                return;
                            }
                            collectCompetitiveHit(doc, compoundScores[i], i, score);
                        } else {
                            collectAnyHit(doc, totalHits[i], compoundScores[i], i, score);
                        }

                    }

                }

                void collectAnyHit(
                    int doc,
                    int hitsCollected,
                    FieldValueHitQueue<FieldValueHitQueue.Entry> compoundScore,
                    int i,
                    float score
                ) throws IOException {
                    // Startup transient: queue hasn't gathered numHits yet
                    int slot = hitsCollected - 1;
                    // Copy hit into queue
                    comparators[i].copy(slot, doc);
                    add(slot, doc, compoundScore, i, score);
                    if (queueFull[i]) {
                        comparators[i].setBottom(bottom.slot);
                        updateMinCompetitiveScore(compoundQueryScorer, compoundScore.getComparators()[0], i);
                    }
                }

                void collectCompetitiveHit(int doc, FieldValueHitQueue<FieldValueHitQueue.Entry> compoundScore, int i, float score)
                    throws IOException {
                    // This hit is competitive - replace bottom element in queue & adjustTop
                    comparators[i].copy(bottom.slot, doc);
                    updateBottom(doc, compoundScore);
                    comparators[i].setBottom(bottom.slot);
                    updateMinCompetitiveScore(compoundQueryScorer, compoundScore.getComparators()[0], i);
                }

                boolean thresholdCheck(int doc, int i, FieldValueHitQueue<FieldValueHitQueue.Entry> compoundScore) throws IOException {
                    if (collectedAllCompetitiveHits || reverseMul * comparators[i].compareBottom(doc) <= 0) {
                        // since docs are visited in doc Id order, if compare is 0, it means
                        // this document is larger than anything else in the queue, and
                        // therefore not competitive.
                        if (searchSortPartOfIndexSort) {
                            if (hitsThresholdChecker.isThresholdReached()) {
                                setTotalHitsRelation(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
                                throw new CollectionTerminatedException();
                            } else {
                                collectedAllCompetitiveHits = true;
                            }
                        } else if (getTotalHitsRelation() == TotalHits.Relation.EQUAL_TO) {
                            // we can start setting the min competitive score if the
                            // threshold is reached for the first time here.
                            updateMinCompetitiveScore(compoundQueryScorer, compoundScore.getComparators()[0], i);
                        }
                        return true;
                    }
                    return false;
                }

                protected void updateGlobalMinCompetitiveScore(Scorable scorer) throws IOException {
                    assert minScoreAcc != null;
                    if (canSetMinScore && hitsThresholdChecker.isThresholdReached()) {
                        // we can start checking the global maximum score even
                        // if the local queue is not full because the threshold
                        // is reached.
                        MaxScoreAccumulator.DocAndScore maxMinScore = minScoreAcc.get();
                        if (maxMinScore != null && maxMinScore.score > minCompetitiveScore) {
                            scorer.setMinCompetitiveScore(maxMinScore.score);
                            minCompetitiveScore = maxMinScore.score;
                            totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
                        }
                    }
                }

                protected void updateMinCompetitiveScore(Scorable scorer, FieldComparator<?> firstComparator, int i) throws IOException {
                    if (canSetMinScore && queueFull[i] && hitsThresholdChecker.isThresholdReached()) {
                        assert bottom != null;
                        float minScore = (float) firstComparator.value(bottom.slot);
                        if (minScore > minCompetitiveScore) {
                            scorer.setMinCompetitiveScore(minScore);
                            minCompetitiveScore = minScore;
                            totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
                            if (minScoreAcc != null) {
                                minScoreAcc.accumulate(docBase, minScore);
                            }
                        }
                    }
                }

                void add(int slot, int doc, FieldValueHitQueue<FieldValueHitQueue.Entry> compoundScore, int i, float score) {
                    FieldValueHitQueue.Entry bottomEntry = new FieldValueHitQueue.Entry(slot, docBase + doc);
                    bottomEntry.score = score;
                    bottom = compoundScore.add(bottomEntry);
                    // The queue is full either when totalHits == numHits (in SimpleFieldCollector), in which case
                    // slot = totalHits - 1, or when hitsCollected == numHits (in PagingFieldCollector this is hits
                    // on the current page) and slot = hitsCollected - 1.
                    assert slot < numHits;
                    queueFull[i] = slot == numHits - 1;
                }

                void updateBottom(int doc, FieldValueHitQueue<FieldValueHitQueue.Entry> compoundScore) {
                    // bottom.score is already set to Float.NaN in add().
                    bottom.doc = docBase + doc;
                    bottom = compoundScore.updateTop();
                }

                boolean canEarlyTerminate(Sort searchSort, Sort indexSort) {
                    return canEarlyTerminateOnDocId(searchSort) || canEarlyTerminateOnPrefix(searchSort, indexSort);
                }

                private boolean canEarlyTerminateOnDocId(Sort searchSort) {
                    final SortField[] fields1 = searchSort.getSort();
                    return SortField.FIELD_DOC.equals(fields1[0]);
                }

                private boolean canEarlyTerminateOnPrefix(Sort searchSort, Sort indexSort) {
                    if (indexSort != null) {
                        final SortField[] fields1 = searchSort.getSort();
                        final SortField[] fields2 = indexSort.getSort();
                        // early termination is possible if fields1 is a prefix of fields2
                        if (fields1.length > fields2.length) {
                            return false;
                        }
                        return Arrays.asList(fields1).equals(Arrays.asList(fields2).subList(0, fields1.length));
                    } else {
                        return false;
                    }
                }
            };
        }

        @Override
        public ScoreMode scoreMode() {
            return hitsThresholdChecker.scoreMode();
        }

        public List<TopFieldDocs> topDocs() {
            if (compoundScores == null) {
                return new ArrayList<>();
            }
            final List<TopFieldDocs> topFieldDocs = IntStream.range(0, compoundScores.length)
                .mapToObj(
                    i -> topDocsPerQuery(
                        0,
                        Math.min(totalHits[i], compoundScores[i].size()),
                        compoundScores[i],
                        totalHits[i],
                        sort.getSort()
                    )
                )
                .collect(Collectors.toList());
            return topFieldDocs;
        }

        private TopFieldDocs topDocsPerQuery(
            int start,
            int howMany,
            PriorityQueue<FieldValueHitQueue.Entry> pq,
            int totalHits,
            SortField[] sortFields
        ) {
            if (howMany < 0) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Number of hits requested must be greater than 0 but value was %d", howMany)
                );
            }

            if (start < 0) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Expected value of starting position is between 0 and %d, got %d", howMany, start)
                );
            }

            if (start >= howMany || howMany == 0) {
                return EMPTY_TOPDOCS;
            }

            int size = howMany - start;
            ScoreDoc[] results = new ScoreDoc[size];
            // pq's pop() returns the 'least' element in the queue, therefore need
            // to discard the first ones, until we reach the requested range.
            for (int i = pq.size() - start - size; i > 0; i--) {
                pq.pop();
            }

            // Get the requested results from pq.
            populateResults(results, size, pq);

            return new TopFieldDocs(new TotalHits(totalHits, totalHitsRelation), results, sortFields);
        }

        protected void populateResults(ScoreDoc[] results, int howMany, PriorityQueue<FieldValueHitQueue.Entry> pq) {
            FieldValueHitQueue<FieldValueHitQueue.Entry> queue = (FieldValueHitQueue<FieldValueHitQueue.Entry>) pq;
            for (int i = howMany - 1; i >= 0 && pq.size() > 0; i--) {
                // adding to array if index is within [0..array_length - 1]
                if (i < results.length) {
                    FieldValueHitQueue.Entry entry = queue.pop();
                    final int n = queue.getComparators().length;
                    final Object[] fields = new Object[n];
                    for (int j = 0; j < n; ++j) {
                        fields[j] = queue.getComparators()[j].value(entry.slot);
                    }

                    results[i] = new FieldDoc(entry.doc, entry.score, fields);
                }
            }
        }
    }


    public static class PagingFieldCollector extends HybridSortedTopDocCollector{

        final Sort sort;
        final int numHits;
        private int[] totalHits;
        // private final int numHits;
        @Getter
        private FieldValueHitQueue<FieldValueHitQueue.Entry>[] compoundScores;
        final HitsThresholdChecker hitsThresholdChecker;
        @Getter
        @Setter
        private TotalHits.Relation totalHitsRelation = TotalHits.Relation.EQUAL_TO;
        private static final TopFieldDocs EMPTY_TOPDOCS = new TopFieldDocs(
                new TotalHits(0, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[0],
                new SortField[0]
        );
        int docBase;
        final MaxScoreAccumulator minScoreAcc;
        FieldValueHitQueue.Entry bottom = null;
        boolean queueFull[];
        Boolean searchSortPartOfIndexSort = null;
        final FieldDoc after;

        public PagingFieldCollector( int numHits,
                              HitsThresholdChecker hitsThresholdChecker,
                              Sort sort,
                              MaxScoreAccumulator minScoreAcc,
                              @Nullable FieldDoc after){
            this.sort = sort;
            this.numHits = numHits;
            this.hitsThresholdChecker = hitsThresholdChecker;
            this.minScoreAcc = minScoreAcc;
            this.after=after;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            docBase = context.docBase;
            final int afterDoc = after.doc - docBase;
            return new LeafCollector() {
                LeafFieldComparator comparators[];
                int reverseMul;
                HybridQueryScorer compoundQueryScorer;
                boolean collectedAllCompetitiveHits = false;
                // FieldComparator<?> firstComparator;
                boolean canSetMinScore;
                float minCompetitiveScore;

                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    if (scorer instanceof HybridQueryScorer) {
                        log.debug("passed scorer is of type HybridQueryScorer, saving it for collecting documents and scores");
                        compoundQueryScorer = (HybridQueryScorer) scorer;
                    } else {
                        compoundQueryScorer = getHybridQueryScorer(scorer);
                        if (Objects.isNull(compoundQueryScorer)) {
                            log.error(
                                    String.format(
                                            Locale.ROOT,
                                            "cannot find scorer of type HybridQueryScorer in a hierarchy of scorer %s",
                                            scorer
                                    )
                            );
                        }
                    }
                }

                private HybridQueryScorer getHybridQueryScorer(final Scorable scorer) throws IOException {
                    if (scorer == null) {
                        return null;
                    }
                    if (scorer instanceof HybridQueryScorer) {
                        return (HybridQueryScorer) scorer;
                    }
                    for (Scorable.ChildScorable childScorable : scorer.getChildren()) {
                        HybridQueryScorer hybridQueryScorer = getHybridQueryScorer(childScorable.child);
                        if (Objects.nonNull(hybridQueryScorer)) {
                            log.debug(
                                    String.format(
                                            Locale.ROOT,
                                            "found hybrid query scorer, it's child of scorer %s",
                                            childScorable.child.getClass().getSimpleName()
                                    )
                            );
                            return hybridQueryScorer;
                        }
                    }
                    return null;
                }

                @Override
                public void collect(int doc) throws IOException {
                    if (Objects.isNull(compoundQueryScorer)) {
                        throw new IllegalArgumentException("scorers are null for all sub-queries in hybrid query");
                    }
                    float[] subScoresByQuery = compoundQueryScorer.hybridScores();
                    // iterate over results for each query
                    if (compoundScores == null) {
                        compoundScores = new FieldValueHitQueue[subScoresByQuery.length];
                        this.comparators = new LeafFieldComparator[subScoresByQuery.length];
                        queueFull = new boolean[subScoresByQuery.length];
                        for (int i = 0; i < subScoresByQuery.length; i++) {
                            // compoundScores[i] = new HitQueue(numHits, true);
                            compoundScores[i] = FieldValueHitQueue.create(sort.getSort(), numHits);
                            FieldComparator<?> firstComparator = compoundScores[i].getComparators()[0];
                            if (searchSortPartOfIndexSort == null) {
                                final Sort indexSort = context.reader().getMetaData().getSort();
                                searchSortPartOfIndexSort = canEarlyTerminate(sort, indexSort);
                                if (searchSortPartOfIndexSort) {
                                    firstComparator.disableSkipping();
                                }
                            }
                            int reverseMul = compoundScores[i].getReverseMul()[0];
                            if (firstComparator.getClass().equals(FieldComparator.RelevanceComparator.class)
                                    && reverseMul == 1 // if the natural sort is preserved (sort by descending relevance)
                                    && hitsThresholdChecker.getTotalHitsThreshold() != Integer.MAX_VALUE) {
                                canSetMinScore = true;
                            } else {
                                canSetMinScore = false;
                            }
                            LeafFieldComparator[] comparators = compoundScores[i].getComparators(context);
                            int[] reverseMuls = compoundScores[i].getReverseMul();
                            if (comparators.length == 1) {
                                this.reverseMul = reverseMuls[0];
                                this.comparators[i] = comparators[0];
                            } else {
                                this.reverseMul = 1;
                                this.comparators[i] = new MultiLeafFieldComparator(comparators, reverseMuls);
                            }
                            this.comparators[i].setScorer(compoundQueryScorer);
                        }
                        totalHits = new int[subScoresByQuery.length];
                    }
                    for (int i = 0; i < subScoresByQuery.length; i++) {
                        minCompetitiveScore = 0f;
                        float score = subScoresByQuery[i];
                        // if score is 0.0 there is no hits for that sub-query
                        if (score == 0) {
                            continue;
                        }
                        totalHits[i]++;
                        hitsThresholdChecker.incrementHitCount();
                        if (minScoreAcc != null && (totalHits[i] & minScoreAcc.modInterval) == 0) {
                            updateGlobalMinCompetitiveScore(compoundQueryScorer);
                        }
                        if (scoreMode().isExhaustive() == false
                                && getTotalHitsRelation() == TotalHits.Relation.EQUAL_TO
                                && hitsThresholdChecker.isThresholdReached()) {
                            // for the first time hitsThreshold is reached, notify comparator about this
                            comparators[i].setHitsThresholdReached();
                            setTotalHitsRelation(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
                        }

                        if (queueFull[i]) {
                            if (thresholdCheck(doc, i, compoundScores[i])) {
                                return;
                            }
                            collectCompetitiveHit(doc, compoundScores[i], i, score);
                        } else {
                            collectAnyHit(doc, totalHits[i], compoundScores[i], i, score);
                        }

                    }

                }

                void collectAnyHit(
                        int doc,
                        int hitsCollected,
                        FieldValueHitQueue<FieldValueHitQueue.Entry> compoundScore,
                        int i,
                        float score
                ) throws IOException {
                    // Startup transient: queue hasn't gathered numHits yet
                    int slot = hitsCollected - 1;
                    // Copy hit into queue
                    comparators[i].copy(slot, doc);
                    add(slot, doc, compoundScore, i, score);
                    if (queueFull[i]) {
                        comparators[i].setBottom(bottom.slot);
                        updateMinCompetitiveScore(compoundQueryScorer, compoundScore.getComparators()[0], i);
                    }
                }

                void collectCompetitiveHit(int doc, FieldValueHitQueue<FieldValueHitQueue.Entry> compoundScore, int i, float score)
                        throws IOException {
                    // This hit is competitive - replace bottom element in queue & adjustTop
                    comparators[i].copy(bottom.slot, doc);
                    updateBottom(doc, compoundScore);
                    comparators[i].setBottom(bottom.slot);
                    updateMinCompetitiveScore(compoundQueryScorer, compoundScore.getComparators()[0], i);
                }

                boolean thresholdCheck(int doc, int i, FieldValueHitQueue<FieldValueHitQueue.Entry> compoundScore) throws IOException {
                    if (collectedAllCompetitiveHits || reverseMul * comparators[i].compareBottom(doc) <= 0) {
                        // since docs are visited in doc Id order, if compare is 0, it means
                        // this document is larger than anything else in the queue, and
                        // therefore not competitive.
                        if (searchSortPartOfIndexSort) {
                            if (hitsThresholdChecker.isThresholdReached()) {
                                setTotalHitsRelation(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
                                throw new CollectionTerminatedException();
                            } else {
                                collectedAllCompetitiveHits = true;
                            }
                        } else if (getTotalHitsRelation() == TotalHits.Relation.EQUAL_TO) {
                            // we can start setting the min competitive score if the
                            // threshold is reached for the first time here.
                            updateMinCompetitiveScore(compoundQueryScorer, compoundScore.getComparators()[0], i);
                        }
                        return true;
                    }
                    return false;
                }

                protected void updateGlobalMinCompetitiveScore(Scorable scorer) throws IOException {
                    assert minScoreAcc != null;
                    if (canSetMinScore && hitsThresholdChecker.isThresholdReached()) {
                        // we can start checking the global maximum score even
                        // if the local queue is not full because the threshold
                        // is reached.
                        MaxScoreAccumulator.DocAndScore maxMinScore = minScoreAcc.get();
                        if (maxMinScore != null && maxMinScore.score > minCompetitiveScore) {
                            scorer.setMinCompetitiveScore(maxMinScore.score);
                            minCompetitiveScore = maxMinScore.score;
                            totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
                        }
                    }
                }

                protected void updateMinCompetitiveScore(Scorable scorer, FieldComparator<?> firstComparator, int i) throws IOException {
                    if (canSetMinScore && queueFull[i] && hitsThresholdChecker.isThresholdReached()) {
                        assert bottom != null;
                        float minScore = (float) firstComparator.value(bottom.slot);
                        if (minScore > minCompetitiveScore) {
                            scorer.setMinCompetitiveScore(minScore);
                            minCompetitiveScore = minScore;
                            totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
                            if (minScoreAcc != null) {
                                minScoreAcc.accumulate(docBase, minScore);
                            }
                        }
                    }
                }

                void add(int slot, int doc, FieldValueHitQueue<FieldValueHitQueue.Entry> compoundScore, int i, float score) {
                    FieldValueHitQueue.Entry bottomEntry = new FieldValueHitQueue.Entry(slot, docBase + doc);
                    bottomEntry.score = score;
                    bottom = compoundScore.add(bottomEntry);
                    // The queue is full either when totalHits == numHits (in SimpleFieldCollector), in which case
                    // slot = totalHits - 1, or when hitsCollected == numHits (in PagingFieldCollector this is hits
                    // on the current page) and slot = hitsCollected - 1.
                    assert slot < numHits;
                    queueFull[i] = slot == numHits - 1;
                }

                void updateBottom(int doc, FieldValueHitQueue<FieldValueHitQueue.Entry> compoundScore) {
                    // bottom.score is already set to Float.NaN in add().
                    bottom.doc = docBase + doc;
                    bottom = compoundScore.updateTop();
                }

                boolean canEarlyTerminate(Sort searchSort, Sort indexSort) {
                    return canEarlyTerminateOnDocId(searchSort) || canEarlyTerminateOnPrefix(searchSort, indexSort);
                }

                private boolean canEarlyTerminateOnDocId(Sort searchSort) {
                    final SortField[] fields1 = searchSort.getSort();
                    return SortField.FIELD_DOC.equals(fields1[0]);
                }

                private boolean canEarlyTerminateOnPrefix(Sort searchSort, Sort indexSort) {
                    if (indexSort != null) {
                        final SortField[] fields1 = searchSort.getSort();
                        final SortField[] fields2 = indexSort.getSort();
                        // early termination is possible if fields1 is a prefix of fields2
                        if (fields1.length > fields2.length) {
                            return false;
                        }
                        return Arrays.asList(fields1).equals(Arrays.asList(fields2).subList(0, fields1.length));
                    } else {
                        return false;
                    }
                }
            };
        }

        @Override
        public ScoreMode scoreMode() {
            return hitsThresholdChecker.scoreMode();
        }

        public List<TopFieldDocs> topDocs() {
            if (compoundScores == null) {
                return new ArrayList<>();
            }
            final List<TopFieldDocs> topFieldDocs = IntStream.range(0, compoundScores.length)
                    .mapToObj(
                            i -> topDocsPerQuery(
                                    0,
                                    Math.min(totalHits[i], compoundScores[i].size()),
                                    compoundScores[i],
                                    totalHits[i],
                                    sort.getSort()
                            )
                    )
                    .collect(Collectors.toList());
            return topFieldDocs;
        }

        private TopFieldDocs topDocsPerQuery(
                int start,
                int howMany,
                PriorityQueue<FieldValueHitQueue.Entry> pq,
                int totalHits,
                SortField[] sortFields
        ) {
            if (howMany < 0) {
                throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Number of hits requested must be greater than 0 but value was %d", howMany)
                );
            }

            if (start < 0) {
                throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Expected value of starting position is between 0 and %d, got %d", howMany, start)
                );
            }

            if (start >= howMany || howMany == 0) {
                return EMPTY_TOPDOCS;
            }

            int size = howMany - start;
            ScoreDoc[] results = new ScoreDoc[size];
            // pq's pop() returns the 'least' element in the queue, therefore need
            // to discard the first ones, until we reach the requested range.
            for (int i = pq.size() - start - size; i > 0; i--) {
                pq.pop();
            }

            // Get the requested results from pq.
            populateResults(results, size, pq);

            return new TopFieldDocs(new TotalHits(totalHits, totalHitsRelation), results, sortFields);
        }

        protected void populateResults(ScoreDoc[] results, int howMany, PriorityQueue<FieldValueHitQueue.Entry> pq) {
            FieldValueHitQueue<FieldValueHitQueue.Entry> queue = (FieldValueHitQueue<FieldValueHitQueue.Entry>) pq;
            for (int i = howMany - 1; i >= 0 && pq.size() > 0; i--) {
                // adding to array if index is within [0..array_length - 1]
                if (i < results.length) {
                    FieldValueHitQueue.Entry entry = queue.pop();
                    final int n = queue.getComparators().length;
                    final Object[] fields = new Object[n];
                    for (int j = 0; j < n; ++j) {
                        fields[j] = queue.getComparators()[j].value(entry.slot);
                    }

                    results[i] = new FieldDoc(entry.doc, entry.score, fields);
                }
            }
        }
    }
}
