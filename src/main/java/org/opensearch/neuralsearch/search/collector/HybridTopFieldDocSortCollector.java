/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.ArrayList;

import com.google.common.annotations.VisibleForTesting;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.FieldValueHitQueue;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.common.Nullable;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;
import org.opensearch.neuralsearch.search.lucene.MultiLeafFieldComparator;

/*
 Collects the TopFieldDocs after executing hybrid query. Uses HybridQueryTopDocs as DTO to handle each sub query results.
 The individual query results are sorted as per the sort criteria sent in the search request.
 */
@Log4j2
public abstract class HybridTopFieldDocSortCollector implements HybridSearchCollector {
    private final int numHits;
    private final HitsThresholdChecker hitsThresholdChecker;
    private final Sort sort;
    @Nullable
    private FieldDoc after;
    private FieldComparator<?> firstComparator;
    // the array stores bottom elements of the min heap of sorted hits for each sub query
    @Getter(AccessLevel.PACKAGE)
    @VisibleForTesting
    private FieldValueHitQueue.Entry fieldValueLeafTrackers[];
    @Getter
    private int totalHits;
    protected int docBase;
    protected LeafFieldComparator comparators[];
    @Getter
    @Setter
    private TotalHits.Relation totalHitsRelation = TotalHits.Relation.EQUAL_TO;
    /*
      reverseMul is used to set the direction of the sorting when creating comparators.
      In threshold check reverseMul is used in comparison logic.
      It modifies the comparison of either reverse or maintain the natural order depending on its value.
      This ensures that the compareBottom method adjusts the order based on whether you want ascending or descending sorting.
     */
    protected int reverseMul;
    protected FieldValueHitQueue<FieldValueHitQueue.Entry>[] compoundScores;
    protected boolean queueFull[];
    @Getter
    protected float maxScore = 0.0f;
    protected int[] collectedHits;
    private boolean needsInitialization = true;

    // searchSortPartOfIndexSort is used to evaluate whether to perform index sort or not.
    private Boolean searchSortPartOfIndexSort = null;

    private static final TopFieldDocs EMPTY_TOP_FIELD_DOCS = new TopFieldDocs(
        new TotalHits(0, TotalHits.Relation.EQUAL_TO),
        new ScoreDoc[0],
        new SortField[0]
    );

    // Declaring the constructor private prevents extending this class by anyone
    // else. Note that the class cannot be final since it's extended by the
    // internal versions. If someone will define a constructor with any other
    // visibility, then anyone will be able to extend the class, which is not what
    // we want.
    HybridTopFieldDocSortCollector(
        final int numHits,
        final HitsThresholdChecker hitsThresholdChecker,
        final Sort sort,
        final FieldDoc after
    ) {
        this.numHits = numHits;
        this.hitsThresholdChecker = hitsThresholdChecker;
        this.sort = sort;
        this.after = after;
    }

    /**
     * HybridCollectorManager fetches the topDocs in the reduce method.
     * @return List of TopFieldDocs which represents results of Top Docs of individual subquery.
     */
    public List<TopFieldDocs> topDocs() {
        if (compoundScores == null) {
            return new ArrayList<>();
        }

        List<TopFieldDocs> topFieldDocs = new ArrayList<>();
        for (int subQueryNumber = 0; subQueryNumber < compoundScores.length; subQueryNumber++) {
            topFieldDocs.add(
                topDocsPerQuery(
                    0,
                    Math.min(collectedHits[subQueryNumber], compoundScores[subQueryNumber].size()),
                    compoundScores[subQueryNumber],
                    collectedHits[subQueryNumber],
                    sort.getSort()
                )
            );
        }
        return topFieldDocs;
    }

    @Override
    public ScoreMode scoreMode() {
        return hitsThresholdChecker.scoreMode();
    }

    protected abstract class HybridTopDocSortLeafCollector extends HybridLeafCollector {
        private boolean collectedAllCompetitiveHits = false;

        /**
         1. initializeComparators method needs to be initialized once per shard.
         2. Also, after initializing for every segment the comparators has to be refreshed.
        Therefore, to do the above two things lazily we have to use a flag initializeLeafComparatorsPerSegmentOnce which is set to true when a leafCollector is initialized per segment.
        Later, in the collect method when number of sub-queries has been found then initialize the comparators(1) or (2) refresh the comparators and set the flag to false.
        */
        private boolean initializeLeafComparatorsPerSegmentOnce;

        public HybridTopDocSortLeafCollector() {
            this.initializeLeafComparatorsPerSegmentOnce = true;
        }

        /*
        Increment total hit count and validate if threshold is reached.
         */
        protected void incrementTotalHitCount() throws IOException {
            totalHits++;
            hitsThresholdChecker.incrementHitCount();
            if (scoreMode().isExhaustive() == false
                && getTotalHitsRelation() == TotalHits.Relation.EQUAL_TO
                && hitsThresholdChecker.isThresholdReached()) {
                // for the first time hitsThreshold is reached, notify all comparators about this
                for (LeafFieldComparator comparator : comparators) {
                    comparator.setHitsThresholdReached();
                }
                setTotalHitsRelation(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
            }
        }

        /*
        Collect hit and add the value of the sort field in the comparator.
         */
        protected void collectHit(int doc, int hitsCollected, int subQueryNumber, float score) throws IOException {
            // Startup transient: queue hasn't gathered numHits yet
            int slot = hitsCollected - 1;
            // Copy hit into queue
            if (numHits > 0) {
                comparators[subQueryNumber].copy(slot, doc);
                add(slot, doc, compoundScores[subQueryNumber], subQueryNumber, score);
                if (queueFull[subQueryNumber]) {
                    comparators[subQueryNumber].setBottom(fieldValueLeafTrackers[subQueryNumber].slot);
                }
            } else {
                queueFull[subQueryNumber] = true;
            }
        }

        /*
         * This hit is competitive - replace bottom element in queue & adjustTop
         */
        protected void collectCompetitiveHit(int doc, int subQueryNumber) throws IOException {
            // This hit is competitive - replace bottom element in queue & adjustTop
            if (numHits > 0) {
                comparators[subQueryNumber].copy(fieldValueLeafTrackers[subQueryNumber].slot, doc);
                updateBottom(doc, compoundScores[subQueryNumber], subQueryNumber);
                comparators[subQueryNumber].setBottom(fieldValueLeafTrackers[subQueryNumber].slot);
            }
        }

        protected boolean thresholdCheck(int doc, int subQueryNumber) throws IOException {
            if (collectedAllCompetitiveHits || reverseMul * comparators[subQueryNumber].compareBottom(doc) <= 0) {
                // since docs are visited in doc Id order, if compare is 0, it means
                // this document is larger than anything else in the queue, and
                // therefore not competitive.
                if (searchSortPartOfIndexSort) {
                    if (hitsThresholdChecker.isThresholdReached()) {
                        setTotalHitsRelation(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
                        log.info("Terminating collection as hits threshold is reached");
                        throw new CollectionTerminatedException();
                    } else {
                        collectedAllCompetitiveHits = true;
                    }
                }
                return true;
            }
            return false;
        }

        /*
        The method initializes once per search request.
         */
        protected void initializePriorityQueuesWithComparators(LeafReaderContext context, int numberOfSubQueries) throws IOException {
            if (needsInitialization) {
                compoundScores = new FieldValueHitQueue[numberOfSubQueries];
                comparators = new LeafFieldComparator[numberOfSubQueries];
                queueFull = new boolean[numberOfSubQueries];
                collectedHits = new int[numberOfSubQueries];
                for (int i = 0; i < numberOfSubQueries; i++) {
                    initializeLeafFieldComparators(context, i);
                }
                fieldValueLeafTrackers = new FieldValueHitQueue.Entry[numberOfSubQueries];
                needsInitialization = false;
            }
            if (initializeLeafComparatorsPerSegmentOnce) {
                for (int i = 0; i < numberOfSubQueries; i++) {
                    initializeComparators(context, i);
                }
                initializeLeafComparatorsPerSegmentOnce = false;
            }
        }

        private void initializeLeafFieldComparators(LeafReaderContext context, int subQueryNumber) throws IOException {
            compoundScores[subQueryNumber] = FieldValueHitQueue.create(sort.getSort(), numHits);
            firstComparator = compoundScores[subQueryNumber].getComparators()[0];

            // Optimize the sort
            if (compoundScores[subQueryNumber].getComparators().length == 1) {
                firstComparator.setSingleSort();
            }

            if (after != null) {
                setAfterFieldValueInFieldCompartor(subQueryNumber);
            }
        }

        /* This method initializes the comparators per segment
         */
        private void initializeComparators(LeafReaderContext context, int subQueryNumber) throws IOException {
            // as all segments are sorted in the same way, enough to check only the 1st segment for indexSort
            if (searchSortPartOfIndexSort == null) {
                Sort indexSort = context.reader().getMetaData().sort();
                searchSortPartOfIndexSort = canEarlyTerminate(sort, indexSort);
                if (searchSortPartOfIndexSort) {
                    firstComparator.disableSkipping();
                }
            }

            LeafFieldComparator[] leafFieldComparators = compoundScores[subQueryNumber].getComparators(context);
            int[] reverseMuls = compoundScores[subQueryNumber].getReverseMul();
            if (leafFieldComparators.length == 1) {
                reverseMul = reverseMuls[0];
                comparators[subQueryNumber] = leafFieldComparators[0];
            } else {
                reverseMul = 1;
                comparators[subQueryNumber] = new MultiLeafFieldComparator(leafFieldComparators, reverseMuls);
            }
            comparators[subQueryNumber].setScorer(compoundQueryScorer);
        }

        private void setAfterFieldValueInFieldCompartor(int subQueryNumber) {
            FieldComparator<?>[] fieldComparators = compoundScores[subQueryNumber].getComparators();
            for (int k = 0; k < fieldComparators.length; k++) {
                @SuppressWarnings("unchecked")
                FieldComparator<Object> fieldComparator = (FieldComparator<Object>) fieldComparators[k];
                fieldComparator.setTopValue(after.fields[k]);
            }
        }
    }

    /*
     TopFieldDocs per subquery
    */
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
            return EMPTY_TOP_FIELD_DOCS;
        }

        int size = howMany - start;
        ScoreDoc[] results = new ScoreDoc[size];

        // Get the requested results from pq.
        populateResults(results, size, pq);

        return new TopFieldDocs(new TotalHits(totalHits, totalHitsRelation), results, sortFields);
    }

    /*
      Results are converted in the FieldDocs and the value of the field on which the sorting is applied has been added in the FieldDoc.
     */
    private void populateResults(ScoreDoc[] results, int howMany, PriorityQueue<FieldValueHitQueue.Entry> pq) {
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

    // Add the entry in the Priority queue
    private void add(int slot, int doc, FieldValueHitQueue<FieldValueHitQueue.Entry> compoundScore, int subQueryNumber, float score) {
        FieldValueHitQueue.Entry bottomEntry = new FieldValueHitQueue.Entry(slot, docBase + doc);
        bottomEntry.score = score;
        fieldValueLeafTrackers[subQueryNumber] = compoundScore.add(bottomEntry);
        // The queue is full either when totalHits == numHits (in SimpleFieldCollector), in which case
        // slot = totalHits - 1, or when hitsCollected == numHits (in PagingFieldCollector this is hits
        // on the current page) and slot = hitsCollected - 1.
        assert slot < numHits;
        boolean isQueueFull = false;
        if (slot == (numHits - 1)) {
            isQueueFull = true;
        }
        queueFull[subQueryNumber] = isQueueFull;
    }

    private void updateBottom(int doc, FieldValueHitQueue<FieldValueHitQueue.Entry> compoundScore, int subQueryIndex) {
        fieldValueLeafTrackers[subQueryIndex].doc = docBase + doc;
        fieldValueLeafTrackers[subQueryIndex] = compoundScore.updateTop();
    }

    private boolean canEarlyTerminate(Sort searchSort, Sort indexSort) {
        return canEarlyTerminateOnDocId(searchSort) || canEarlyTerminateOnPrefix(searchSort, indexSort);
    }

    private boolean canEarlyTerminateOnDocId(Sort searchSort) {
        final SortField[] fields1 = searchSort.getSort();
        return SortField.FIELD_DOC.equals(fields1[0]);
    }

    private boolean canEarlyTerminateOnPrefix(Sort searchSort, Sort indexSort) {
        if (indexSort != null) {
            final SortField[] searchSortField = searchSort.getSort();
            final SortField[] indexSortField = indexSort.getSort();
            // early termination is possible if fields1 is a prefix of fields2
            if (searchSortField.length > indexSortField.length) {
                return false;
            }
            // Compare fields1 and the corresponding prefix of fields2
            for (int i = 0; i < searchSortField.length; i++) {
                if (!searchSortField[i].equals(indexSortField[i])) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
