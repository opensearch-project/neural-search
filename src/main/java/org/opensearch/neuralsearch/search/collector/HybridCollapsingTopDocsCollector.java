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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

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
    private final boolean needsScores;
    private int docBase;
    private final ConcurrentHashMap<T, FieldValueHitQueue<FieldValueHitQueue.Entry>[]> groupQueueMap;
    private ConcurrentHashMap<T, int[]> collectedHitsPerSubQueryMap;
    private ConcurrentHashMap<T, FieldValueHitQueue.Entry[]> fieldValueLeafTrackersMap;
    private ConcurrentHashMap<T, LeafFieldComparator[]> comparatorsMap;
    private ConcurrentHashMap<T, FieldComparator<?>> firstComparatorMap;
    private ConcurrentHashMap<T, Integer> reverseMulMap;
    private ConcurrentHashMap<T, boolean[]> queueFullMap;
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
            this.needsScores = groupSort.needsScores();
            SortField[] sortFields = groupSort.getSort();
            this.reversed = new int[sortFields.length];

            for (int i = 0; i < sortFields.length; ++i) {
                SortField sortField = sortFields[i];
                this.reversed[i] = sortField.getReverse() ? -1 : 1;
            }

            this.groupQueueMap = new ConcurrentHashMap<>();
            this.collectedHitsPerSubQueryMap = new ConcurrentHashMap<>();
            this.fieldValueLeafTrackersMap = new ConcurrentHashMap<>();
            this.comparatorsMap = new ConcurrentHashMap<>();
            this.firstComparatorMap = new ConcurrentHashMap<>();
            this.reverseMulMap = new ConcurrentHashMap<>();
            this.queueFullMap = new ConcurrentHashMap<>();

            this.numHits = topNGroups;
            this.hitsThresholdChecker = hitsThresholdChecker;
        }
    }

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
        return this.needsScores ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        docBase = context.docBase;
        groupSelector.setNextReader(context);
        return new HybridLeafCollector() {
            ConcurrentHashMap<T, Boolean> initializeLeafComparatorsPerSegmentOnceMap;

            {
                initializeLeafComparatorsPerSegmentOnceMap = new ConcurrentHashMap<>();
            }

            @Override
            public void collect(int doc) throws IOException {
                HybridSubQueryScorer compoundQueryScorer = getCompoundQueryScorer();
                if (Objects.isNull(compoundQueryScorer)) {
                    return;
                }
                groupSelector.advanceTo(doc);
                T groupValue = groupSelector.currentValue();
                assert groupValue != null;
                FieldValueHitQueue<FieldValueHitQueue.Entry>[] group = groupQueueMap.get(groupValue);
                float[] subScoresByQuery = compoundQueryScorer.getSubQueryScores();
                if (group == null) {
                    initializeQueue(subScoresByQuery.length);
                }

                if (initializeLeafComparatorsPerSegmentOnceMap.get(groupValue) == null) {
                    int numSubQueries = subScoresByQuery.length;
                    LeafFieldComparator[] comparators = comparatorsMap.get(groupValue);
                    FieldValueHitQueue<FieldValueHitQueue.Entry>[] compoundScores = groupQueueMap.get(groupValue);
                    for (int i = 0; i < numSubQueries; i++) {
                        LeafFieldComparator[] leafFieldComparators = compoundScores[i].getComparators(context);
                        int[] reverseMuls = compoundScores[i].getReverseMul();
                        if (leafFieldComparators.length == 1) {
                            reverseMulMap.put(groupSelector.copyValue(), reverseMuls[0]);
                            comparators[i] = leafFieldComparators[0];
                        } else {
                            reverseMulMap.put(groupSelector.copyValue(), 1);
                            comparators[i] = new MultiLeafFieldComparator(leafFieldComparators, reverseMuls);
                        }
                        comparators[i].setScorer(compoundQueryScorer);
                    }
                    comparatorsMap.put(groupSelector.copyValue(), comparators);
                    initializeLeafComparatorsPerSegmentOnceMap.put(groupSelector.copyValue(), false);
                }
                totalHitCount++;
                hitsThresholdChecker.incrementHitCount();

                if (hitsThresholdChecker.isThresholdReached()) {
                    setTotalHitsRelation(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
                    log.info("Terminating collection early as specified hits threshold is reached");
                    throw new CollectionTerminatedException();
                }

                for (int i = 0; i < subScoresByQuery.length; i++) {
                    float score = subScoresByQuery[i];
                    if (score == 0) {
                        continue;
                    }

                    boolean[] queueFullArray = queueFullMap.get(groupValue);
                    if (queueFullArray[i]) {
                        LeafFieldComparator[] comparators = comparatorsMap.get(groupValue);
                        if (reverseMulMap.get(groupValue) * comparators[i].compareBottom(doc) > 0) {
                            FieldValueHitQueue.Entry[] fieldValueLeafTrackers = fieldValueLeafTrackersMap.get(groupValue);
                            FieldValueHitQueue<FieldValueHitQueue.Entry>[] compoundScores = groupQueueMap.get(groupValue);
                            comparators[i].copy(fieldValueLeafTrackers[i].slot, doc);

                            fieldValueLeafTrackers[i].doc = docBase + doc;
                            fieldValueLeafTrackers[i] = compoundScores[i].updateTop();
                            comparators[i].setBottom(fieldValueLeafTrackers[i].slot);

                            comparatorsMap.put(groupSelector.copyValue(), comparators);
                            fieldValueLeafTrackersMap.put(groupSelector.copyValue(), fieldValueLeafTrackers);
                            groupQueueMap.put(groupSelector.copyValue(), compoundScores);
                        }
                    } else {
                        int[] collectedHitsForCurrentSubQuery = collectedHitsPerSubQueryMap.get(groupValue);
                        int slot = collectedHitsForCurrentSubQuery[i];

                        collectedHitsForCurrentSubQuery[i]++;
                        collectedHitsPerSubQueryMap.put(groupValue, collectedHitsForCurrentSubQuery);

                        FieldValueHitQueue<FieldValueHitQueue.Entry>[] compoundScores = groupQueueMap.get(groupValue);
                        maxScore = Math.max(score, maxScore);

                        FieldValueHitQueue.Entry[] fieldValueLeafTrackers = fieldValueLeafTrackersMap.get(groupValue);

                        LeafFieldComparator[] comparators = comparatorsMap.get(groupValue);
                        comparators[i].copy(slot, doc);

                        FieldValueHitQueue.Entry bottomEntry = new FieldValueHitQueue.Entry(slot, docBase + doc);
                        bottomEntry.score = score;

                        fieldValueLeafTrackers[i] = compoundScores[i].add(bottomEntry);

                        fieldValueLeafTrackersMap.put(groupSelector.copyValue(), fieldValueLeafTrackers);
                        groupQueueMap.put(groupSelector.copyValue(), compoundScores);

                        if (slot == (numHits - 1)) {
                            queueFullArray[i] = true;
                            queueFullMap.put(groupSelector.copyValue(), queueFullArray);
                        }
                    }
                }
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
