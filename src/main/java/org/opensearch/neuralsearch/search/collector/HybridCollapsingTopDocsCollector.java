/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.FieldValueHitQueue;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.ScoreDoc;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Collects the CollapseTopFieldDocs based on a collapse field passed in a search request containing a hybrid query.
 */
@Log4j2
public class HybridCollapsingTopDocsCollector<T> implements HybridSearchCollector, Collector {
    protected final String collapseField;
    private int totalHitCount;
    private float maxScore = 0.0f;
    private Sort sort;
    private final GroupSelector<T> groupSelector;
    private final int[] reversed;
    private final boolean needsScores = true;
    private int docBase;
    private final HashMap<T, FieldValueHitQueue<FieldValueHitQueue.Entry>[]> groupQueueMap;
    private HashMap<T, int[]> collectedHitsPerSubQueryMap;
    private HashMap<T, FieldValueHitQueue.Entry[]> fieldValueLeafTrackersMap;
    private HashMap<T, LeafFieldComparator[]> comparatorsMap;
    private HashMap<T, FieldComparator<?>> firstComparatorMap;
    private HashMap<T, Integer> reverseMulMap;
    private HashMap<T, boolean[]> queueFullMap;
    private final int numHits;
    @Setter
    TotalHits.Relation totalHitsRelation = TotalHits.Relation.EQUAL_TO;
    private HitsThresholdChecker hitsThresholdChecker;

    HybridCollapsingTopDocsCollector(
        GroupSelector<T> groupSelector,
        String collapseField,
        Sort groupSort,
        int topNGroups,
        HitsThresholdChecker hitsThresholdChecker
    ) {
        this.groupSelector = groupSelector;
        this.collapseField = collapseField;
        this.sort = groupSort;
        if (topNGroups < 1) {
            throw new IllegalArgumentException("topNGroups must be >= 1 (got " + topNGroups + ")");
        } else {
            SortField[] sortFields = groupSort.getSort();
            this.reversed = new int[sortFields.length];

            for (int i = 0; i < sortFields.length; ++i) {
                SortField sortField = sortFields[i];
                this.reversed[i] = sortField.getReverse() ? -1 : 1;
            }

            this.groupQueueMap = new HashMap<>();
            this.collectedHitsPerSubQueryMap = new HashMap<>();
            this.fieldValueLeafTrackersMap = new HashMap<>();
            this.comparatorsMap = new HashMap<>();
            this.firstComparatorMap = new HashMap<>();
            this.reverseMulMap = new HashMap<>();
            this.queueFullMap = new HashMap<>();

            this.numHits = topNGroups;
            this.hitsThresholdChecker = hitsThresholdChecker;
        }
    }

    /**
     * Creates a HybridCollapsingTopDocsCollector for keyword fields.
     *
     * @param collapseField The field to collapse on
     * @param fieldType The mapped field type
     * @param sort The sort criteria to apply
     * @param topNGroups The number of top groups to collect
     * @param hitsThresholdChecker Checker for hits threshold
     * @return A new HybridCollapsingTopDocsCollector instance for keyword fields
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
     *
     * @param collapseField The field to collapse on
     * @param fieldType The mapped field type
     * @param sort The sort criteria to apply
     * @param topNGroups The number of top groups to collect
     * @param hitsThresholdChecker Checker for hits threshold
     * @return A new HybridCollapsingTopDocsCollector instance for numeric fields
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
     *
     * @return A list of CollapseTopFieldDocs
     * @throws IOException If an I/O error occurs
     */
    @Override
    public List<CollapseTopFieldDocs> topDocs() throws IOException {
        List<CollapseTopFieldDocs> topDocsList = new ArrayList<>();
        if (collectedHitsPerSubQueryMap.isEmpty()) {
            return topDocsList;
        }
        // Get num sub queries, there is probably a better way to do this
        int numSubQueries = 0;
        for (Map.Entry<T, int[]> entry : collectedHitsPerSubQueryMap.entrySet()) {
            numSubQueries = entry.getValue().length;
            break;
        }

        for (int i = 0; i < numSubQueries; i++) {
            ArrayList<ScoreDoc> fieldDocs = new ArrayList<>();
            ArrayList<T> collapseValues = new ArrayList<>();
            int hitsOnCurrentSubQuery = 0;
            for (T groupValue : groupQueueMap.keySet()) {
                FieldValueHitQueue<FieldValueHitQueue.Entry> priorityQueue = groupQueueMap.get(groupValue)[i];
                final int n = priorityQueue.getComparators().length;

                for (int j = 0; j < numHits; j++) {
                    if (priorityQueue.size() > 0) {
                        FieldValueHitQueue.Entry queueEntry = priorityQueue.pop();

                        final Object[] fields = new Object[n];
                        for (int k = 0; k < n; ++k) {
                            fields[k] = priorityQueue.getComparators()[k].value(queueEntry.slot);
                        }
                        fieldDocs.add(new FieldDoc(queueEntry.doc, queueEntry.score, fields));
                        collapseValues.add(groupValue instanceof BytesRef ? (T) BytesRef.deepCopyOf((BytesRef) groupValue) : groupValue);
                    } else {
                        // Break if queue is empty
                        break;
                    }
                }
                hitsOnCurrentSubQuery += collectedHitsPerSubQueryMap.get(groupValue)[i];
            }
            topDocsList.add(
                new CollapseTopFieldDocs(
                    collapseField,
                    new TotalHits(hitsOnCurrentSubQuery, totalHitsRelation),
                    fieldDocs.toArray(new FieldDoc[0]),
                    sort.getSort(),
                    collapseValues.toArray(new Object[0])
                )
            );
        }
        return topDocsList;
    }

    /**
     * Returns the total number of hits across all groups.
     *
     * @return The total hit count
     */
    @Override
    public int getTotalHits() {
        return totalHitCount;
    }

    /**
     * Returns the maximum score across all collected documents.
     *
     * @return The maximum score
     */
    @Override
    public float getMaxScore() {
        return maxScore;
    }

    /**
     * Returns the score mode for this collector.
     *
     * @return The ScoreMode indicating how scores should be computed
     */
    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE;
    }

    /**
     * Creates a LeafCollector for collecting documents in a segment.
     *
     * @param context The context for the current leaf reader
     * @return A LeafCollector for the segment
     * @throws IOException If an I/O error occurs
     */
    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        docBase = context.docBase;
        groupSelector.setNextReader(context);
        return new HybridLeafCollector() {
            HashMap<T, Boolean> initializeLeafComparatorsPerSegmentOnceMap;

            {
                initializeLeafComparatorsPerSegmentOnceMap = new HashMap<>();
            }

            /**
             * Collects a document and processes it based on its group value, scores, and sort.
             *
             * @param doc The document ID
             * @throws IOException If an I/O error occurs
             */
            @Override
            public void collect(int doc) throws IOException {
                HybridSubQueryScorer compoundQueryScorer = getCompoundQueryScorer();
                if (Objects.isNull(compoundQueryScorer)) {
                    return;
                }

                groupSelector.advanceTo(doc);
                T groupValue = groupSelector.currentValue();
                assert groupValue != null;
                float[] subScoresByQuery = compoundQueryScorer.getSubQueryScores();
                initializeQueueIfNeeded(groupValue, subScoresByQuery.length);
                initializeLeafComparatorsIfNeeded(groupValue, subScoresByQuery.length, compoundQueryScorer);

                updateHitCount();

                for (int i = 0; i < subScoresByQuery.length; i++) {
                    float score = subScoresByQuery[i];

                    if (isQueueFull(groupValue, i)) {
                        updateExistingEntry(groupValue, i, doc);
                    } else {
                        addNewEntry(groupValue, i, doc, score);
                    }
                }
            }

            private void initializeQueueIfNeeded(T groupValue, int subQueryCount) throws IOException {
                if (groupQueueMap.get(groupValue) == null) {
                    initializeQueue(subQueryCount);
                }
            }

            private void initializeLeafComparatorsIfNeeded(T groupValue, int numSubQueries, HybridSubQueryScorer compoundQueryScorer)
                throws IOException {
                if (initializeLeafComparatorsPerSegmentOnceMap.get(groupValue) == null) {
                    initializeLeafComparators(groupValue, numSubQueries, compoundQueryScorer);
                }
            }

            private void initializeLeafComparators(T groupValue, int numSubQueries, HybridSubQueryScorer compoundQueryScorer)
                throws IOException {
                LeafFieldComparator[] comparators = comparatorsMap.get(groupValue);
                FieldValueHitQueue<FieldValueHitQueue.Entry>[] compoundScores = groupQueueMap.get(groupValue);

                for (int subQueryNumber = 0; subQueryNumber < numSubQueries; subQueryNumber++) {
                    LeafFieldComparator[] leafFieldComparators = compoundScores[subQueryNumber].getComparators(context);
                    int[] reverseMuls = compoundScores[subQueryNumber].getReverseMul();

                    if (leafFieldComparators.length == 1) {
                        reverseMulMap.put(groupSelector.copyValue(), reverseMuls[0]);
                        comparators[subQueryNumber] = leafFieldComparators[0];
                    } else {
                        reverseMulMap.put(groupSelector.copyValue(), 1);
                        comparators[subQueryNumber] = new MultiLeafFieldComparator(leafFieldComparators, reverseMuls);
                    }
                    comparators[subQueryNumber].setScorer(compoundQueryScorer);
                }

                comparatorsMap.put(groupSelector.copyValue(), comparators);
                initializeLeafComparatorsPerSegmentOnceMap.put(groupSelector.copyValue(), false);
            }

            private void updateHitCount() throws CollectionTerminatedException {
                totalHitCount++;
                hitsThresholdChecker.incrementHitCount();

                if (hitsThresholdChecker.isThresholdReached()) {
                    setTotalHitsRelation(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
                    log.info("Terminating collection early as specified hits threshold is reached");
                    throw new CollectionTerminatedException();
                }
            }

            private boolean isQueueFull(T groupValue, int index) {
                boolean[] queueFullArray = queueFullMap.get(groupValue);
                return queueFullArray[index];
            }

            private void updateExistingEntry(T groupValue, int index, int doc) throws IOException {
                LeafFieldComparator[] comparators = comparatorsMap.get(groupValue);
                if (reverseMulMap.get(groupValue) * comparators[index].compareBottom(doc) > 0) {
                    FieldValueHitQueue.Entry[] fieldValueLeafTrackers = fieldValueLeafTrackersMap.get(groupValue);
                    FieldValueHitQueue<FieldValueHitQueue.Entry>[] compoundScores = groupQueueMap.get(groupValue);

                    comparators[index].copy(fieldValueLeafTrackers[index].slot, doc);
                    fieldValueLeafTrackers[index].doc = docBase + doc;
                    fieldValueLeafTrackers[index] = compoundScores[index].updateTop();
                    comparators[index].setBottom(fieldValueLeafTrackers[index].slot);

                    updateMaps(comparators, fieldValueLeafTrackers, compoundScores);
                }
            }

            private void addNewEntry(T groupValue, int subQueryNumber, int doc, float score) throws IOException {
                int[] collectedHitsForCurrentSubQuery = collectedHitsPerSubQueryMap.get(groupValue);
                int slot = collectedHitsForCurrentSubQuery[subQueryNumber];

                collectedHitsForCurrentSubQuery[subQueryNumber]++;
                collectedHitsPerSubQueryMap.put(groupValue, collectedHitsForCurrentSubQuery);

                FieldValueHitQueue<FieldValueHitQueue.Entry>[] compoundScores = groupQueueMap.get(groupValue);
                maxScore = Math.max(score, maxScore);

                FieldValueHitQueue.Entry[] fieldValueLeafTrackers = fieldValueLeafTrackersMap.get(groupValue);
                LeafFieldComparator[] comparators = comparatorsMap.get(groupValue);

                comparators[subQueryNumber].copy(slot, doc);

                FieldValueHitQueue.Entry bottomEntry = new FieldValueHitQueue.Entry(slot, docBase + doc);
                bottomEntry.score = score;

                fieldValueLeafTrackers[subQueryNumber] = compoundScores[subQueryNumber].add(bottomEntry);

                updateMaps(comparators, fieldValueLeafTrackers, compoundScores);

                if (slot == (numHits - 1)) {
                    boolean[] queueFullArray = queueFullMap.get(groupValue);
                    queueFullArray[subQueryNumber] = true;
                    queueFullMap.put(groupSelector.copyValue(), queueFullArray);
                }
            }

            private void updateMaps(
                LeafFieldComparator[] comparators,
                FieldValueHitQueue.Entry[] fieldValueLeafTrackers,
                FieldValueHitQueue<FieldValueHitQueue.Entry>[] compoundScores
            ) throws IOException {
                comparatorsMap.put(groupSelector.copyValue(), comparators);
                fieldValueLeafTrackersMap.put(groupSelector.copyValue(), fieldValueLeafTrackers);
                groupQueueMap.put(groupSelector.copyValue(), compoundScores);
            }

            private void initializeQueue(int numSubQueries) throws IOException {
                FieldValueHitQueue<FieldValueHitQueue.Entry>[] compoundScores = new FieldValueHitQueue[numSubQueries];
                for (int i = 0; i < numSubQueries; i++) {
                    compoundScores[i] = FieldValueHitQueue.create(sort.getSort(), numHits);
                    firstComparatorMap.put(groupSelector.copyValue(), compoundScores[i].getComparators()[0]);
                }
                groupQueueMap.put(groupSelector.copyValue(), compoundScores);
                collectedHitsPerSubQueryMap.put(groupSelector.copyValue(), new int[numSubQueries]);
                comparatorsMap.put(groupSelector.copyValue(), new LeafFieldComparator[numSubQueries]);
                fieldValueLeafTrackersMap.put(groupSelector.copyValue(), new FieldValueHitQueue.Entry[numSubQueries]);
                queueFullMap.put(groupSelector.copyValue(), new boolean[numSubQueries]);
            }
        };
    }
}
