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
import org.apache.lucene.util.PriorityQueue;
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
    private final Map<T, FieldValueHitQueue<FieldValueHitQueue.Entry>[]> groupQueueMap;
    private Map<T, int[]> collectedHitsPerSubQueryMap;
    private Map<T, FieldValueHitQueue.Entry[]> fieldValueLeafTrackersMap;
    private Map<T, LeafFieldComparator[]> comparatorsMap;
    private Map<T, FieldComparator<?>> firstComparatorMap;
    private Map<T, Integer> reverseMulMap;
    private Map<T, boolean[]> queueFullMap;
    private final int numHits;
    private final int docsPerGroupPerSubQuery;
    @Setter
    TotalHits.Relation totalHitsRelation = TotalHits.Relation.EQUAL_TO;
    private HitsThresholdChecker hitsThresholdChecker;

    HybridCollapsingTopDocsCollector(
        GroupSelector<T> groupSelector,
        String collapseField,
        @NonNull Sort groupSort,
        int topNGroups,
        HitsThresholdChecker hitsThresholdChecker,
        int docsPerGroupPerSubQuery
    ) {
        this.groupSelector = groupSelector;
        this.collapseField = collapseField;
        this.sort = groupSort;
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
        // If docsPerGroupPerSubQuery is not larger than 0, use the size for hybrid search without collapse
        this.docsPerGroupPerSubQuery = docsPerGroupPerSubQuery > 0 ? docsPerGroupPerSubQuery : topNGroups;
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
        HitsThresholdChecker hitsThresholdChecker,
        int docsPerGroupPerSubQuery
    ) {
        return new HybridCollapsingTopDocsCollector<>(
            new CollapseDocSourceGroupSelector.Keyword(fieldType),
            collapseField,
            sort,
            topNGroups,
            hitsThresholdChecker,
            docsPerGroupPerSubQuery
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
        HitsThresholdChecker hitsThresholdChecker,
        int docsPerGroupPerSubQuery
    ) {
        return new HybridCollapsingTopDocsCollector<>(
            new CollapseDocSourceGroupSelector.Numeric(fieldType),
            collapseField,
            sort,
            topNGroups,
            hitsThresholdChecker,
            docsPerGroupPerSubQuery
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
        int numSubQueries = collectedHitsPerSubQueryMap.values().iterator().next().length;

        for (int subQueryNumber = 0; subQueryNumber < numSubQueries; subQueryNumber++) {
            GroupPriorityQueue<T> topGroupsQueue = new GroupPriorityQueue<>(numHits);

            // Calculate total hits for current subquery
            int totalHitsForSubQuery = 0;
            for (int[] hits : collectedHitsPerSubQueryMap.values()) {
                totalHitsForSubQuery += hits[subQueryNumber];
            }

            // Collect top N groups
            for (Map.Entry<T, FieldValueHitQueue<FieldValueHitQueue.Entry>[]> entry : groupQueueMap.entrySet()) {
                T groupValue = entry.getKey();
                FieldValueHitQueue<FieldValueHitQueue.Entry> queue = entry.getValue()[subQueryNumber];
                if (queue.size() > 0) {
                    topGroupsQueue.insertWithOverflow(new GroupEntry<>(groupValue, queue));
                }
            }

            ArrayList<ScoreDoc> fieldDocs = new ArrayList<>();
            ArrayList<T> collapseValues = new ArrayList<>();

            // Pop entries in reverse order to maintain sorting
            GroupEntry<T>[] topGroups = new GroupEntry[topGroupsQueue.size()];
            for (int j = topGroupsQueue.size() - 1; j >= 0; j--) {
                topGroups[j] = topGroupsQueue.pop();
            }

            // Process the top groups and include all docs from each group
            for (GroupEntry<T> groupEntry : topGroups) {
                T groupValue = groupEntry.groupValue;
                FieldValueHitQueue<FieldValueHitQueue.Entry> priorityQueue = groupEntry.queue;
                final int n = priorityQueue.getComparators().length;

                // Get all entries from the priority queue
                FieldValueHitQueue.Entry[] entries = new FieldValueHitQueue.Entry[priorityQueue.size()];
                for (int i = priorityQueue.size() - 1; i >= 0; i--) {
                    entries[i] = priorityQueue.pop();
                }

                // Add all entries from this group
                for (FieldValueHitQueue.Entry queueEntry : entries) {
                    final Object[] fields = new Object[n];
                    for (int k = 0; k < n; ++k) {
                        fields[k] = priorityQueue.getComparators()[k].value(queueEntry.slot);
                    }
                    fieldDocs.add(new FieldDoc(queueEntry.doc, queueEntry.score, fields));
                    collapseValues.add(groupValue instanceof BytesRef ? (T) BytesRef.deepCopyOf((BytesRef) groupValue) : groupValue);
                }
            }

            topDocsList.add(
                new CollapseTopFieldDocs(
                    collapseField,
                    new TotalHits(totalHitsForSubQuery, totalHitsRelation),
                    fieldDocs.toArray(new FieldDoc[0]),
                    sort.getSort(),
                    collapseValues.toArray(new Object[0])
                )
            );
        }
        return topDocsList;
    }

    private static class GroupEntry<T> {
        final T groupValue;
        final FieldValueHitQueue<FieldValueHitQueue.Entry> queue;

        GroupEntry(T groupValue, FieldValueHitQueue<FieldValueHitQueue.Entry> queue) {
            this.groupValue = groupValue;
            this.queue = queue;
        }
    }

    private class GroupPriorityQueue<T> extends PriorityQueue<GroupEntry<T>> {
        GroupPriorityQueue(int maxSize) {
            super(maxSize);
        }

        @Override
        protected boolean lessThan(GroupEntry<T> a, GroupEntry<T> b) {
            FieldValueHitQueue<FieldValueHitQueue.Entry> queueA = a.queue;
            FieldValueHitQueue<FieldValueHitQueue.Entry> queueB = b.queue;

            if (queueA.size() == 0 && queueB.size() == 0) return false;
            if (queueA.size() == 0) return true;
            if (queueB.size() == 0) return false;

            FieldValueHitQueue.Entry entryA = queueA.top();
            FieldValueHitQueue.Entry entryB = queueB.top();

            FieldComparator<?>[] comparators = queueA.getComparators();
            int[] reverseMul = queueA.getReverseMul();

            for (int i = 0; i < comparators.length; i++) {
                FieldComparator<Object> comparator = (FieldComparator<Object>) comparators[i];
                Object valueA = comparator.value(entryA.slot);
                Object valueB = comparator.value(entryB.slot);

                int comparison = comparator.compareValues(valueA, valueB);
                if (comparison != 0) {
                    return reverseMul[i] * comparison < 0;
                }
            }

            // If all comparisons are equal, use score as a tie-breaker.
            return entryB.score < entryA.score;
        }
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
            Map<T, Boolean> initializeLeafComparatorsPerSegmentOnceMap;

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

                // Gets the collapse group value associated with the current document
                groupSelector.advanceTo(doc);
                // currentValue() returns a potentially reusable reference - safe for immediate use
                // but NOT safe for storing as map keys or persisting across document iterations
                T groupValue = groupSelector.currentValue();

                float[] subScoresByQuery = compoundQueryScorer.getSubQueryScores();
                initializeQueueIfNeeded(groupValue, subScoresByQuery.length);
                initializeLeafComparatorsIfNeeded(groupValue, subScoresByQuery.length, compoundQueryScorer);

                updateHitCount();

                for (int subQueryNumber = 0; subQueryNumber < subScoresByQuery.length; subQueryNumber++) {
                    float score = subScoresByQuery[subQueryNumber];
                    // if score is 0.0 there is no hits for that sub-query
                    if (score == 0) {
                        continue;
                    }

                    // Retrieve the array of collected hits for the current group
                    // Use groupValue for immediate map lookups - safe because we're not storing it
                    int[] collectedHitsForCurrentSubQuery = collectedHitsPerSubQueryMap.get(groupValue);
                    int slot = collectedHitsForCurrentSubQuery[subQueryNumber];

                    // Increment the hit count for the current subquery
                    collectedHitsForCurrentSubQuery[subQueryNumber]++;
                    collectedHitsPerSubQueryMap.put(groupValue, collectedHitsForCurrentSubQuery);

                    // If the priority queue is full, replace the lowest scoring document per the comparator.
                    // If the priority queue is not full, add the entry to the queue.
                    if (isQueueFull(groupValue, subQueryNumber)) {
                        updateExistingEntry(groupValue, subQueryNumber, doc);
                    } else {
                        addNewEntry(groupValue, subQueryNumber, doc, score, slot);
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
                        // Use copyValue() as map key - creates persistent copy safe from mutation
                        // Example: If groupValue is BytesRef("A"), copyValue() creates independent BytesRef("A")
                        // Without this, the next document's BytesRef("B") would overwrite our map key
                        reverseMulMap.put(groupSelector.copyValue(), reverseMuls[0]);
                        comparators[subQueryNumber] = leafFieldComparators[0];
                    } else {
                        reverseMulMap.put(groupSelector.copyValue(), 1);
                        comparators[subQueryNumber] = new MultiLeafFieldComparator(leafFieldComparators, reverseMuls);
                    }
                    comparators[subQueryNumber].setScorer(compoundQueryScorer);
                }

                // Use copyValue() as map key to ensure persistence across document iterations
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
                // Retrieve the array of comparators for the current group
                LeafFieldComparator[] comparators = comparatorsMap.get(groupValue);

                // Check if the current document should replace the bottom entry in the queue
                // The comparison is multiplied by reverseMul to handle ascending/descending order
                if (reverseMulMap.get(groupValue) * comparators[index].compareBottom(doc) > 0) {
                    // Retrieve the leaf trackers and compound scores for the current group
                    FieldValueHitQueue.Entry[] fieldValueLeafTrackers = fieldValueLeafTrackersMap.get(groupValue);
                    FieldValueHitQueue<FieldValueHitQueue.Entry>[] compoundScores = groupQueueMap.get(groupValue);

                    // Copy the current document's data to the slot of the bottom entry
                    comparators[index].copy(fieldValueLeafTrackers[index].slot, doc);

                    // Update the document ID in the leaf tracker
                    fieldValueLeafTrackers[index].doc = docBase + doc;

                    // Update the top entry in the compound scores and get the new bottom entry
                    fieldValueLeafTrackers[index] = compoundScores[index].updateTop();

                    // Set the new bottom entry in the comparator
                    comparators[index].setBottom(fieldValueLeafTrackers[index].slot);

                    // Update related maps with the new information
                    updateMaps(comparators, fieldValueLeafTrackers, compoundScores);
                }
            }

            private void addNewEntry(T groupValue, int subQueryNumber, int doc, float score, int slot) throws IOException {
                // Retrieve the compound scores for the current group
                FieldValueHitQueue<FieldValueHitQueue.Entry>[] compoundScores = groupQueueMap.get(groupValue);
                // Update the maximum score if necessary
                maxScore = Math.max(score, maxScore);

                // Retrieve leaf trackers and comparators for the current group
                FieldValueHitQueue.Entry[] fieldValueLeafTrackers = fieldValueLeafTrackersMap.get(groupValue);
                LeafFieldComparator[] comparators = comparatorsMap.get(groupValue);

                // Copy the document data to the appropriate slot in the comparator
                comparators[subQueryNumber].copy(slot, doc);

                // Create a new entry with the current slot, document, and score
                FieldValueHitQueue.Entry bottomEntry = new FieldValueHitQueue.Entry(slot, docBase + doc);
                bottomEntry.score = score;

                // Add the new entry to the compound scores and update the leaf tracker
                fieldValueLeafTrackers[subQueryNumber] = compoundScores[subQueryNumber].add(bottomEntry);

                // Update related maps with the new information
                updateMaps(comparators, fieldValueLeafTrackers, compoundScores);

                // Check if the queue is full for this subquery
                if (slot == (docsPerGroupPerSubQuery - 1)) {
                    boolean[] queueFullArray = queueFullMap.get(groupValue);
                    queueFullArray[subQueryNumber] = true;
                    // Use copyValue() as map key to persist the queue full state
                    queueFullMap.put(groupSelector.copyValue(), queueFullArray);
                }
            }

            private void updateMaps(
                LeafFieldComparator[] comparators,
                FieldValueHitQueue.Entry[] fieldValueLeafTrackers,
                FieldValueHitQueue<FieldValueHitQueue.Entry>[] compoundScores
            ) throws IOException {
                // Use copyValue() as map keys to ensure data persists correctly across document iterations
                // This prevents map corruption when groupSelector reuses internal objects (e.g., BytesRef)
                comparatorsMap.put(groupSelector.copyValue(), comparators);
                fieldValueLeafTrackersMap.put(groupSelector.copyValue(), fieldValueLeafTrackers);
                groupQueueMap.put(groupSelector.copyValue(), compoundScores);
            }

            private void initializeQueue(int numSubQueries) throws IOException {
                FieldValueHitQueue<FieldValueHitQueue.Entry>[] compoundScores = new FieldValueHitQueue[numSubQueries];
                for (int i = 0; i < numSubQueries; i++) {
                    compoundScores[i] = FieldValueHitQueue.create(sort.getSort(), docsPerGroupPerSubQuery);
                    // Use copyValue() as map key - critical for BytesRef types where the same instance is reused
                    // Example: Doc1 has groupValue BytesRef("A"), Doc2 has groupValue BytesRef("B")
                    // Without copyValue(), both map entries would point to the same mutating BytesRef object
                    firstComparatorMap.put(groupSelector.copyValue(), compoundScores[i].getComparators()[0]);
                }
                // Use copyValue() for all map keys to create independent, persistent copies
                // This ensures map integrity when groupSelector reuses internal objects across documents
                groupQueueMap.put(groupSelector.copyValue(), compoundScores);
                collectedHitsPerSubQueryMap.put(groupSelector.copyValue(), new int[numSubQueries]);
                comparatorsMap.put(groupSelector.copyValue(), new LeafFieldComparator[numSubQueries]);
                fieldValueLeafTrackersMap.put(groupSelector.copyValue(), new FieldValueHitQueue.Entry[numSubQueries]);
                queueFullMap.put(groupSelector.copyValue(), new boolean[numSubQueries]);
            }
        };
    }
}
