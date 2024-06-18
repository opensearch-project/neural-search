/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

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
import org.opensearch.neuralsearch.query.HybridQueryScorer;
import org.opensearch.common.Nullable;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;
import org.opensearch.neuralsearch.search.MultiLeafFieldComparator;

/*
 Collects the TopFieldDocs after executing hybrid query. Uses HybridQueryTopDocs as DTO to handle each sub query results.
 The individual query results are sorted as per the sort criteria sent in the search request.
 */
@Log4j2
public abstract class HybridTopFieldDocSortCollector implements Collector {
    /*
      numhits maintain the size of the result to be collected.
     */
    final int numHits;
    /*
      hitsThresholdChecker is used to get score mode and check the threshold for collecting the hits.
     */
    final HitsThresholdChecker hitsThresholdChecker;
    /*
      docBase holds the starting point of doc Iteration in the segment.
     */
    int docBase;
    /*
      comparators collect the value of the field on which sorting criteria is applied and returns the result accordingly.
     */
    LeafFieldComparator comparators[];
    /*
      reverseMul is used to set the direction of the sorting when creating comparators.
      In threshold check reverseMul is used in comparison logic.
      It modifies the comparison of either reverse or maintain the natural order depending on its value.
      This ensures that the compareBottom method adjusts the order based on whether you want ascending or descending sorting.
     */
    int reverseMul;
    FieldComparator<?> firstComparator;
    FieldValueHitQueue.Entry bottom = null;
    /*
      List of Priority Queues to hold entries which has higher score but sorted as per the sorting criteria.
     */
    FieldValueHitQueue<FieldValueHitQueue.Entry>[] compoundScores;
    boolean queueFull[];
    @Getter
    private int totalHits;
    @Getter
    float maxScore = 0.0f;
    int[] collectedHits;
    int numberOfSubQueries = 0;
    @Getter
    @Setter
    private TotalHits.Relation totalHitsRelation = TotalHits.Relation.EQUAL_TO;
    /*
       searchSortPartOfIndexSort is used to evaluate whether to perform index sort or not.
     */
    private Boolean searchSortPartOfIndexSort = null;

    private static final TopFieldDocs EMPTY_TOPDOCS = new TopFieldDocs(
        new TotalHits(0, TotalHits.Relation.EQUAL_TO),
        new ScoreDoc[0],
        new SortField[0]
    );

    // Declaring the constructor private prevents extending this class by anyone
    // else. Note that the class cannot be final since it's extended by the
    // internal versions. If someone will define a constructor with any other
    // visibility, then anyone will be able to extend the class, which is not what
    // we want.
    public HybridTopFieldDocSortCollector(final int numHits, final HitsThresholdChecker hitsThresholdChecker) {
        this.numHits = numHits;
        this.hitsThresholdChecker = hitsThresholdChecker;
    }

    // Add the entry in the Priority queue
    private void add(int slot, int doc, FieldValueHitQueue<FieldValueHitQueue.Entry> compoundScore, int subQueryNumber, float score) {
        FieldValueHitQueue.Entry bottomEntry = new FieldValueHitQueue.Entry(slot, docBase + doc);
        bottomEntry.score = score;
        bottom = compoundScore.add(bottomEntry);
        // The queue is full either when totalHits == numHits (in SimpleFieldCollector), in which case
        // slot = totalHits - 1, or when hitsCollected == numHits (in PagingFieldCollector this is hits
        // on the current page) and slot = hitsCollected - 1.
        assert slot < numHits;
        queueFull[subQueryNumber] = slot == numHits - 1;
    }

    private void updateBottom(int doc, FieldValueHitQueue<FieldValueHitQueue.Entry> compoundScore) {
        bottom.doc = docBase + doc;
        bottom = compoundScore.updateTop();
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

    public List<TopFieldDocs> topDocs(final FieldValueHitQueue<FieldValueHitQueue.Entry>[] compoundScores, final Sort sort) {
        if (compoundScores == null) {
            return new ArrayList<>();
        }
        final List<TopFieldDocs> topFieldDocs = IntStream.range(0, compoundScores.length)
            .mapToObj(
                i -> topDocsPerQuery(
                    0,
                    Math.min(collectedHits[i], compoundScores[i].size()),
                    compoundScores[i],
                    collectedHits[i],
                    sort.getSort()
                )
            )
            .collect(Collectors.toList());
        return topFieldDocs;
    }

    public abstract class HybridTopDocSortLeafCollector implements LeafCollector {
        HybridQueryScorer compoundQueryScorer;
        boolean collectedAllCompetitiveHits = false;

        @Nullable
        FieldDoc after;
        final Sort sort;
        boolean initializePerSegment;

        public HybridTopDocSortLeafCollector(Sort sort, @Nullable FieldDoc after) {
            this.sort = sort;
            this.after = after;
            this.initializePerSegment = true;
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            if (scorer instanceof HybridQueryScorer) {
                log.debug("passed scorer is of type HybridQueryScorer, saving it for collecting documents and scores");
                compoundQueryScorer = (HybridQueryScorer) scorer;
            } else {
                compoundQueryScorer = getHybridQueryScorer(scorer);
                if (Objects.isNull(compoundQueryScorer)) {
                    log.error(
                        String.format(Locale.ROOT, "cannot find scorer of type HybridQueryScorer in a hierarchy of scorer %s", scorer)
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

        /*
        The method initializes once per search request.
         */
        void initializePriorityQueuesWithComparators(LeafReaderContext context, int length) throws IOException {
            if (compoundScores == null) {
                numberOfSubQueries = length;
                compoundScores = new FieldValueHitQueue[length];
                comparators = new LeafFieldComparator[length];
                queueFull = new boolean[length];
                collectedHits = new int[length];
                for (int i = 0; i < length; i++) {
                    initializeLeafFieldComparators(context, i);
                }
            }
            if (initializePerSegment) {
                for (int i = 0; i < length; i++) {
                    initializeComparators(context, i);
                }
            } else {
                initializePerSegment = false;
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
                Sort indexSort = context.reader().getMetaData().getSort();
                searchSortPartOfIndexSort = canEarlyTerminate(sort, indexSort);
                log.info("searchSortPartOfIndexSort " + searchSortPartOfIndexSort);
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

        /*
        Increment total hit count and validate if threshold is reached.
         */
        void incrementTotalHitCount() throws IOException {
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
        void collectHit(int doc, int hitsCollected, int subQueryNumber, float score) throws IOException {
            // Startup transient: queue hasn't gathered numHits yet
            int slot = hitsCollected - 1;
            // Copy hit into queue
            comparators[subQueryNumber].copy(slot, doc);
            add(slot, doc, compoundScores[subQueryNumber], subQueryNumber, score);
            if (queueFull[subQueryNumber]) {
                comparators[subQueryNumber].setBottom(bottom.slot);
            }
        }

        /*
        // This hit is competitive - replace bottom element in queue & adjustTop
         */
        void collectCompetitiveHit(int doc, int subQueryNumber) throws IOException {
            comparators[subQueryNumber].copy(bottom.slot, doc);
            updateBottom(doc, compoundScores[subQueryNumber]);
            comparators[subQueryNumber].setBottom(bottom.slot);
        }

        boolean thresholdCheck(int doc, int subQueryNumber) throws IOException {
            if (collectedAllCompetitiveHits || reverseMul * comparators[subQueryNumber].compareBottom(doc) <= 0) {
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
                }
                return true;
            }
            return false;
        }

    }

    @Override
    public ScoreMode scoreMode() {
        return hitsThresholdChecker.scoreMode();
    }

    /*
      TopFieldDocs per subquery
     */
    protected TopFieldDocs topDocsPerQuery(
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

    /*
      Results are converted in the FieldDocs and the value of the field on which the sorting is applied has been added in the FieldDoc.
     */
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
