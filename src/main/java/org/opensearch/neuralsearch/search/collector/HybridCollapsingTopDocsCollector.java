/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.FieldValueHitQueue;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.search.grouping.GroupSelector;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.query.HybridQueryScorer;
import org.opensearch.neuralsearch.search.lucene.MultiLeafFieldComparator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Log4j2
public class HybridCollapsingTopDocsCollector<T> implements HybridSearchCollector, Collector {
    protected final String collapseField;
    private int totalHitCount;
    private float maxScore = 0.0f;
    private Sort sort;
    private final GroupSelector<BytesRef> groupSelector;
    private final int[] reversed;
    private final boolean needsScores;
    private int docBase;
    private final ConcurrentHashMap<BytesRef, HybridCollectedSearchGroup<BytesRef>> groupMap;
    private final ConcurrentHashMap<BytesRef, FieldValueHitQueue<FieldValueHitQueue.Entry>[]> groupQueueMap;
    private ConcurrentHashMap<BytesRef, int[]> collectedHitsPerSubQueryMap;
    private ConcurrentHashMap<BytesRef, FieldValueHitQueue.Entry[]> fieldValueLeafTrackersMap;
    private ConcurrentHashMap<BytesRef, LeafFieldComparator[]> comparatorsMap;
    private ConcurrentHashMap<BytesRef, FieldComparator<?>> firstComparatorMap;
    private ConcurrentHashMap<BytesRef, Integer> reverseMulMap;
    private ConcurrentHashMap<BytesRef, boolean[]> queueFullMap;
    private final int numHits;

    public HybridCollapsingTopDocsCollector(GroupSelector<BytesRef> groupSelector, String collapseField, Sort groupSort, int topNGroups) {
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

            this.groupMap = new ConcurrentHashMap<>();
            this.groupQueueMap = new ConcurrentHashMap<>();
            this.collectedHitsPerSubQueryMap = new ConcurrentHashMap<>();
            this.fieldValueLeafTrackersMap = new ConcurrentHashMap<>();
            this.comparatorsMap = new ConcurrentHashMap<>();
            this.firstComparatorMap = new ConcurrentHashMap<>();
            this.reverseMulMap = new ConcurrentHashMap<>();
            this.queueFullMap = new ConcurrentHashMap<>();

            this.numHits = topNGroups;
        }
    }

    @Override
    public List<? extends TopDocs> topDocs() throws IOException {
        List<CollapseTopFieldDocs> topDocsList = new ArrayList<>();
        if (collectedHitsPerSubQueryMap.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        // Get num sub queries, there is probably a better way to do this
        int numSubQueries = 0;
        for (Map.Entry<BytesRef, int[]> entry : collectedHitsPerSubQueryMap.entrySet()) {
            numSubQueries = entry.getValue().length;
            break;
        }

        for (int i = 0; i < numSubQueries; i++) {
            ArrayList<ScoreDoc> fieldDocs = new ArrayList<>();
            ArrayList<BytesRef> collapseValues = new ArrayList<>();
            int hitsOnCurrentSubQuery = 0;
            for (Map.Entry<BytesRef, HybridCollectedSearchGroup<BytesRef>> entry : groupMap.entrySet()) {
                BytesRef groupValue = entry.getKey();
                FieldValueHitQueue<FieldValueHitQueue.Entry> priorityQueue = groupQueueMap.get(groupValue)[i];
                final int n = priorityQueue.getComparators().length;

                // Hard coded 10 for now
                for (int j = 0; j < 10; j++) {
                    if (priorityQueue.size() > 0) {
                        FieldValueHitQueue.Entry queueEntry = priorityQueue.pop();

                        final Object[] fields = new Object[n];
                        for (int k = 0; k < n; ++k) {
                            fields[k] = priorityQueue.getComparators()[k].value(queueEntry.slot);
                        }
                        fieldDocs.add(new FieldDoc(queueEntry.doc, queueEntry.score, fields));
                        collapseValues.add(BytesRef.deepCopyOf(groupValue));
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
                    new TotalHits(hitsOnCurrentSubQuery, TotalHits.Relation.EQUAL_TO),
                    fieldDocs.toArray(new FieldDoc[0]),
                    sort.getSort(),
                    collapseValues.toArray(new BytesRef[0])
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
        return new LeafCollector() {
            HybridQueryScorer compoundQueryScorer;
            ConcurrentHashMap<BytesRef, Boolean> initializeLeafComparatorsPerSegmentOnceMap;

            {
                initializeLeafComparatorsPerSegmentOnceMap = new ConcurrentHashMap<>();
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
                    HybridQueryScorer hybridQueryScorer = getHybridQueryScorer(childScorable.child());
                    if (Objects.nonNull(hybridQueryScorer)) {
                        log.debug(
                            String.format(
                                Locale.ROOT,
                                "found hybrid query scorer, it's child of scorer %s",
                                childScorable.child().getClass().getSimpleName()
                            )
                        );
                        return hybridQueryScorer;
                    }
                }
                return null;
            }

            @Override
            public void collect(int doc) throws IOException {
                groupSelector.advanceTo(doc);
                BytesRef groupValue = groupSelector.currentValue();
                HybridCollectedSearchGroup group = groupMap.get(groupValue);
                float[] subScoresByQuery = compoundQueryScorer.hybridScores();
                if (group == null) {
                    initializeQueue(groupValue, subScoresByQuery.length);

                    HybridCollectedSearchGroup newGroup = new HybridCollectedSearchGroup();
                    newGroup.groupValue = groupSelector.copyValue();
                    newGroup.topDoc = docBase + doc;
                    groupMap.put(groupSelector.copyValue(), newGroup);
                }

                if (initializeLeafComparatorsPerSegmentOnceMap.get(groupValue) == null) {
                    int numSubQueries = subScoresByQuery.length;
                    LeafFieldComparator[] comparators = comparatorsMap.get(groupValue);
                    FieldValueHitQueue<FieldValueHitQueue.Entry>[] compoundScores = groupQueueMap.get(groupValue);
                    for (int i = 0; i < numSubQueries; i++) {
                        LeafFieldComparator[] leafFieldComparators = compoundScores[i].getComparators(context);
                        int[] reverseMuls = compoundScores[i].getReverseMul();
                        if (leafFieldComparators.length == 1) {
                            reverseMulMap.put(BytesRef.deepCopyOf(groupValue), reverseMuls[0]);
                            comparators[i] = leafFieldComparators[0];
                        } else {
                            reverseMulMap.put(BytesRef.deepCopyOf(groupValue), 1);
                            comparators[i] = new MultiLeafFieldComparator(leafFieldComparators, reverseMuls);
                        }
                        comparators[i].setScorer(compoundQueryScorer);
                    }
                    comparatorsMap.put(BytesRef.deepCopyOf(groupValue), comparators);
                    initializeLeafComparatorsPerSegmentOnceMap.put(BytesRef.deepCopyOf(groupValue), false);
                }
                totalHitCount++;

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

                            comparatorsMap.put(BytesRef.deepCopyOf(groupValue), comparators);
                            fieldValueLeafTrackersMap.put(BytesRef.deepCopyOf(groupValue), fieldValueLeafTrackers);
                            groupQueueMap.put(BytesRef.deepCopyOf(groupValue), compoundScores);
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

                        fieldValueLeafTrackersMap.put(BytesRef.deepCopyOf(groupValue), fieldValueLeafTrackers);
                        groupQueueMap.put(BytesRef.deepCopyOf(groupValue), compoundScores);

                        if (slot == (numHits - 1)) {
                            queueFullArray[i] = true;
                            queueFullMap.put(BytesRef.deepCopyOf(groupValue), queueFullArray);
                        }
                    }
                }
            }

            private void initializeQueue(BytesRef groupValue, int numSubQueries) {
                FieldValueHitQueue<FieldValueHitQueue.Entry>[] compoundScores = new FieldValueHitQueue[numSubQueries];
                for (int i = 0; i < numSubQueries; i++) {
                    compoundScores[i] = FieldValueHitQueue.create(sort.getSort(), numHits);
                    firstComparatorMap.put(BytesRef.deepCopyOf(groupValue), compoundScores[i].getComparators()[0]);
                }
                groupQueueMap.put(BytesRef.deepCopyOf(groupValue), compoundScores);
                collectedHitsPerSubQueryMap.put(BytesRef.deepCopyOf(groupValue), new int[numSubQueries]);
                comparatorsMap.put(BytesRef.deepCopyOf(groupValue), new LeafFieldComparator[numSubQueries]);
                fieldValueLeafTrackersMap.put(BytesRef.deepCopyOf(groupValue), new FieldValueHitQueue.Entry[numSubQueries]);
                queueFullMap.put(BytesRef.deepCopyOf(groupValue), new boolean[numSubQueries]);
            }
        };
    }
}
