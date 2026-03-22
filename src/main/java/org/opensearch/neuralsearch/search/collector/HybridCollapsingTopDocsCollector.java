/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import lombok.NonNull;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.FieldValueHitQueue;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.search.grouping.GroupSelector;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.neuralsearch.query.HybridSubQueryScorer;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;
import org.opensearch.neuralsearch.search.lucene.MultiLeafFieldComparator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Collects the CollapseTopFieldDocs based on a collapse field passed in a search request containing a hybrid query.
 *
 * <p>This collector uses a flat per-sub-query {@link FieldValueHitQueue} of size {@code numHits},
 * mirroring the pattern used by {@link HybridTopScoreDocCollector}. Each entry in the queue carries
 * docId, score, sort field values, and the associated group (collapse) value.
 *
 * <p>When sorting by score, evicted entries propagate their scores as minimum thresholds to
 * {@link HybridSubQueryScorer}, enabling {@code HybridBulkScorer} to skip non-competitive
 * documents early.
 *
 * <p>The actual collapse deduplication (one result per group) does NOT happen here — it happens
 * downstream in the normalization pipeline (CollapseDataCollector and related classes). This
 * collector's job is to collect the top {@code numHits} competitive docs per sub-query with
 * their group values attached.
 */

@Log4j2
public class HybridCollapsingTopDocsCollector<T> implements HybridSearchCollector {
    protected final String collapseField;
    private int totalHitCount;
    private float maxScore = 0.0f;
    private final Sort sort;
    private final GroupSelector<T> groupSelector;
    private int docBase;
    private final int numHits;
    private final boolean isSortByScore;
    @Setter
    TotalHits.Relation totalHitsRelation = TotalHits.Relation.EQUAL_TO;
    private final HitsThresholdChecker hitsThresholdChecker;

    // Flat per-sub-query queues — one FieldValueHitQueue of size numHits per sub-query
    private FieldValueHitQueue<FieldValueHitQueue.Entry>[] subQueryQueues;
    // groupValueBySlot[subQuery][slot] = the collapse group value for that slot
    private Object[][] groupValueBySlot;
    // Per-sub-query bottom entry tracker (the weakest entry currently in the queue)
    private FieldValueHitQueue.Entry[] bottomEntries;
    // Whether each sub-query's queue has reached capacity
    private boolean[] queueFull;
    // Per-sub-query leaf comparators — re-initialized per segment
    private LeafFieldComparator[] leafComparators;
    // Per-sub-query reverse multipliers for sort direction
    private int[] reverseMuls;
    // Total docs collected per sub-query (for totalHits reporting)
    private int[] collectedHitsPerSubQuery;
    // Per-sub-query min score thresholds (only used when sorting by score)
    private float[] minScoreThresholds;

    HybridCollapsingTopDocsCollector(
        GroupSelector<T> groupSelector,
        String collapseField,
        @NonNull Sort groupSort,
        int topNGroups,
        HitsThresholdChecker hitsThresholdChecker
    ) {
        this.groupSelector = groupSelector;
        this.collapseField = collapseField;
        this.sort = groupSort;

        boolean sortByScore = false;
        for (SortField sf : groupSort.getSort()) {
            if (SortField.Type.SCORE.equals(sf.getType())) {
                sortByScore = true;
                break;
            }
        }
        this.isSortByScore = sortByScore;
        this.numHits = topNGroups;
        this.hitsThresholdChecker = hitsThresholdChecker;
    }

    /**
     * Creates a HybridCollapsingTopDocsCollector for keyword fields.
     */
    public static HybridCollapsingTopDocsCollector<?> createKeyword(
        String collapseField,
        MappedFieldType fieldType,
        Sort sort,
        int topNGroups,
        HitsThresholdChecker hitsThresholdChecker
    ) {
        return new HybridCollapsingTopDocsCollector<>(
            new CollapseDocSourceGroupSelector.Keyword(fieldType),
            collapseField,
            sort,
            topNGroups,
            hitsThresholdChecker
        );
    }

    /**
     * Creates a HybridCollapsingTopDocsCollector for numeric fields.
     */
    public static HybridCollapsingTopDocsCollector<?> createNumeric(
        String collapseField,
        MappedFieldType fieldType,
        Sort sort,
        int topNGroups,
        HitsThresholdChecker hitsThresholdChecker
    ) {
        return new HybridCollapsingTopDocsCollector<>(
            new CollapseDocSourceGroupSelector.Numeric(fieldType),
            collapseField,
            sort,
            topNGroups,
            hitsThresholdChecker
        );
    }

    /**
     * Returns the collected top documents, including collapse values and sort fields, grouped by sub-query.
     * Pops each sub-query's queue, builds FieldDoc[] with sort field values from comparators,
     * and looks up group value from groupValueBySlot[sub][entry.slot].
     */
    @Override
    public List<CollapseTopFieldDocs> topDocs() throws IOException {
        List<CollapseTopFieldDocs> topDocsList = new ArrayList<>();
        if (subQueryQueues == null) {
            return topDocsList;
        }

        for (int subQuery = 0; subQuery < subQueryQueues.length; subQuery++) {
            FieldValueHitQueue<FieldValueHitQueue.Entry> queue = subQueryQueues[subQuery];
            int totalHitsForSubQuery = collectedHitsPerSubQuery[subQuery];

            if (totalHitsForSubQuery == 0 || queue.size() == 0) {
                topDocsList.add(
                    new CollapseTopFieldDocs(
                        collapseField,
                        new TotalHits(0, totalHitsRelation),
                        new FieldDoc[0],
                        sort.getSort(),
                        new Object[0]
                    )
                );
                continue;
            }

            int size = queue.size();
            FieldComparator<?>[] comparators = queue.getComparators();
            int numComparators = comparators.length;

            // Pop all entries from the queue (they come out weakest-first from the min-heap)
            FieldDoc[] fieldDocs = new FieldDoc[size];
            Object[] collapseValues = new Object[size];

            for (int i = size - 1; i >= 0; i--) {
                FieldValueHitQueue.Entry entry = queue.pop();
                Object[] fields = new Object[numComparators];
                for (int k = 0; k < numComparators; k++) {
                    fields[k] = comparators[k].value(entry.slot);
                }
                fieldDocs[i] = new FieldDoc(entry.doc, entry.score, fields);

                // Look up the group value stored for this slot
                Object groupValue = groupValueBySlot[subQuery][entry.slot];
                if (groupValue instanceof BytesRef) {
                    collapseValues[i] = BytesRef.deepCopyOf((BytesRef) groupValue);
                } else {
                    collapseValues[i] = groupValue;
                }
            }

            topDocsList.add(
                new CollapseTopFieldDocs(
                    collapseField,
                    new TotalHits(totalHitsForSubQuery, totalHitsRelation),
                    fieldDocs,
                    sort.getSort(),
                    collapseValues
                )
            );
        }
        return topDocsList;
    }

    @Override
    public int getTotalHits() {
        return totalHitCount;
    }

    @Override
    public float getMaxScore() {
        return maxScore;
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        docBase = context.docBase;
        groupSelector.setNextReader(context);

        return new HybridLeafCollector() {
            private boolean leafComparatorsInitialized = false;

            @Override
            public void setScorer(Scorable scorer) throws IOException {
                super.setScorer(scorer);
                if (isSortByScore && Objects.isNull(minScoreThresholds)) {
                    minScoreThresholds = new float[getCompoundQueryScorer().getNumOfSubQueries()];
                    Arrays.fill(minScoreThresholds, Float.MIN_VALUE);
                }
            }

            @Override
            public void collect(int doc) throws IOException {
                // In profiler mode, populate scores from HybridQueryScorer before reading them
                populateScoresFromHybridQueryScorer();

                HybridSubQueryScorer compoundQueryScorer = getCompoundQueryScorer();
                if (Objects.isNull(compoundQueryScorer)) {
                    return;
                }

                // Get the collapse group value for this document
                groupSelector.advanceTo(doc);

                float[] subScoresByQuery = compoundQueryScorer.getSubQueryScores();
                ensureQueuesInitialized(subScoresByQuery.length);
                ensureLeafComparatorsInitialized(subScoresByQuery.length, context, compoundQueryScorer);

                updateHitCount();

                for (int subQuery = 0; subQuery < subScoresByQuery.length; subQuery++) {
                    float score = subScoresByQuery[subQuery];
                    // Skip sub-queries with no match
                    if (score == 0) {
                        continue;
                    }

                    // Skip non-competitive docs when sorting by score
                    if (isSortByScore && (score <= 0 || score < minScoreThresholds[subQuery])) {
                        continue;
                    }

                    collectedHitsPerSubQuery[subQuery]++;
                    maxScore = Math.max(score, maxScore);

                    if (queueFull[subQuery]) {
                        // Queue is full — compare with bottom and replace if competitive
                        updateExistingEntry(compoundQueryScorer, subQuery, doc, score);
                    } else {
                        // Queue not full — add directly
                        addNewEntry(subQuery, doc, score);
                    }
                }
            }

            @SuppressWarnings("unchecked")
            private void ensureQueuesInitialized(int numSubQueries) throws IOException {
                if (subQueryQueues != null) {
                    return;
                }
                subQueryQueues = new FieldValueHitQueue[numSubQueries];
                groupValueBySlot = new Object[numSubQueries][numHits];
                bottomEntries = new FieldValueHitQueue.Entry[numSubQueries];
                queueFull = new boolean[numSubQueries];
                leafComparators = new LeafFieldComparator[numSubQueries];
                reverseMuls = new int[numSubQueries];
                collectedHitsPerSubQuery = new int[numSubQueries];

                for (int i = 0; i < numSubQueries; i++) {
                    subQueryQueues[i] = FieldValueHitQueue.create(sort.getSort(), numHits);
                }
            }

            private void ensureLeafComparatorsInitialized(
                int numSubQueries,
                LeafReaderContext ctx,
                HybridSubQueryScorer compoundQueryScorer
            ) throws IOException {
                if (leafComparatorsInitialized) {
                    return;
                }
                leafComparatorsInitialized = true;

                for (int subQuery = 0; subQuery < numSubQueries; subQuery++) {
                    LeafFieldComparator[] leafFieldComparators = subQueryQueues[subQuery].getComparators(ctx);
                    int[] queueReverseMuls = subQueryQueues[subQuery].getReverseMul();

                    LeafFieldComparator comparator;
                    int reverseMul;

                    if (leafFieldComparators.length == 1) {
                        reverseMul = queueReverseMuls[0];
                        LeafFieldComparator actual = leafFieldComparators[0];
                        comparator = isSortByScore ? new HybridLeafFieldComparator(actual) : actual;
                    } else {
                        reverseMul = 1;
                        comparator = new MultiLeafFieldComparator(leafFieldComparators, queueReverseMuls);
                    }

                    comparator.setScorer(compoundQueryScorer);
                    leafComparators[subQuery] = comparator;
                    reverseMuls[subQuery] = reverseMul;
                }
            }

            private void updateExistingEntry(HybridSubQueryScorer compoundQueryScorer, int subQuery, int doc, float score)
                throws IOException {
                LeafFieldComparator comparator = leafComparators[subQuery];
                float scoreOfLastTopEntry = 0;

                // For score-based sorting, set the individual sub-query score on the wrapper
                if (isSortByScore) {
                    assert comparator instanceof HybridLeafFieldComparator;
                    scoreOfLastTopEntry = ((HybridLeafFieldComparator) comparator).getCurrentSubQueryScore();
                    ((HybridLeafFieldComparator) comparator).setCurrentSubQueryScore(score);
                }

                boolean accepted = false;
                try {
                    accepted = reverseMuls[subQuery] * comparator.compareBottom(doc) > 0;
                } finally {
                    if (!accepted && isSortByScore) {
                        ((HybridLeafFieldComparator) comparator).setCurrentSubQueryScore(scoreOfLastTopEntry);
                    }
                }

                if (accepted) {
                    FieldValueHitQueue.Entry bottom = bottomEntries[subQuery];
                    float evictedScore = bottom.score;

                    comparator.copy(bottom.slot, doc);
                    bottom.doc = docBase + doc;
                    bottom.score = score;

                    // Store group value for this slot (already deep-copied by copyValue() above)
                    groupValueBySlot[subQuery][bottom.slot] = groupSelector.copyValue();

                    // Update the queue and get the new bottom
                    bottomEntries[subQuery] = subQueryQueues[subQuery].updateTop();
                    comparator.setBottom(bottomEntries[subQuery].slot);

                    // Update minScore from the evicted entry's score
                    if (isSortByScore) {
                        minScoreThresholds[subQuery] = Math.max(minScoreThresholds[subQuery], evictedScore);
                        compoundQueryScorer.getMinScores()[subQuery] = Math.max(compoundQueryScorer.getMinScores()[subQuery], evictedScore);
                    }
                }
            }

            private void addNewEntry(int subQuery, int doc, float score) throws IOException {
                LeafFieldComparator comparator = leafComparators[subQuery];
                int slot = subQueryQueues[subQuery].size();

                if (isSortByScore) {
                    assert comparator instanceof HybridLeafFieldComparator;
                    ((HybridLeafFieldComparator) comparator).setCurrentSubQueryScore(score);
                }

                comparator.copy(slot, doc);

                FieldValueHitQueue.Entry entry = new FieldValueHitQueue.Entry(slot, docBase + doc);
                entry.score = score;

                // Store group value for this slot
                groupValueBySlot[subQuery][slot] = groupSelector.copyValue();

                bottomEntries[subQuery] = subQueryQueues[subQuery].add(entry);

                // Check if queue just became full
                if (subQueryQueues[subQuery].size() == numHits) {
                    queueFull[subQuery] = true;
                    comparator.setBottom(bottomEntries[subQuery].slot);
                }
            }

            /**
             * Increments the total hit count and checks if the threshold has been reached.
             * If the threshold is reached, sets the total hits relation to GREATER_THAN_OR_EQUAL_TO
             * and throws CollectionTerminatedException to stop collection.
             */
            private void updateHitCount() {
                totalHitCount++;
                hitsThresholdChecker.incrementHitCount();
                if (hitsThresholdChecker.isThresholdReached()) {
                    setTotalHitsRelation(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
                    throw new CollectionTerminatedException();
                }
            }
        };
    }
}
