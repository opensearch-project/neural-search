/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.HitQueue;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Pruning;
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
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.neuralsearch.query.HybridQueryScorer;

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
    private final FieldComparator<?>[] comparators;
    private final int[] reversed;
    private final boolean needsScores;
    private int docBase;
    private final ConcurrentHashMap<BytesRef, HybridCollectedSearchGroup<BytesRef>> groupMap;
    private final ConcurrentHashMap<BytesRef, PriorityQueue<ScoreDoc>[]> groupQueueMap;
    private PriorityQueue<HybridCollectedSearchGroup> groupQueue;
    private ConcurrentHashMap<BytesRef, int[]> collectedHitsPerSubQueryMap;
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
            this.comparators = new FieldComparator[sortFields.length];
            this.reversed = new int[sortFields.length];

            for (int i = 0; i < sortFields.length; ++i) {
                SortField sortField = sortFields[i];
                this.comparators[i] = sortField.getComparator(topNGroups + 1, Pruning.NONE);
                this.reversed[i] = sortField.getReverse() ? -1 : 1;
            }

            this.groupMap = new ConcurrentHashMap<>();
            this.groupQueueMap = new ConcurrentHashMap<>();
            this.collectedHitsPerSubQueryMap = new ConcurrentHashMap<>();

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
            ArrayList<ScoreDoc> scoreDocs = new ArrayList<>();
            ArrayList<BytesRef> collapseValues = new ArrayList<>();
            int hitsOnCurrentSubQuery = 0;
            for (Map.Entry<BytesRef, HybridCollectedSearchGroup<BytesRef>> entry : groupMap.entrySet()) {
                BytesRef groupValue = entry.getKey();
                PriorityQueue<ScoreDoc> priorityQueue = groupQueueMap.get(groupValue)[i];

                // Hard coded 10 for now
                for (int j = 0; j < 10; j++) {
                    if (priorityQueue.size() > 0) {
                        scoreDocs.add(priorityQueue.pop());
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
                    scoreDocs.toArray(new ScoreDoc[0]),
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

                for (int i = 0; i < subScoresByQuery.length; i++) {
                    float score = subScoresByQuery[i];
                    if (score == 0) {
                        continue;
                    }

                    int[] collectedHitsForCurrentSubQuery = collectedHitsPerSubQueryMap.get(groupValue);
                    collectedHitsForCurrentSubQuery[i]++;
                    collectedHitsPerSubQueryMap.put(groupValue, collectedHitsForCurrentSubQuery);

                    PriorityQueue<ScoreDoc> pq = groupQueueMap.get(groupValue)[i];
                    ScoreDoc currentDoc = new ScoreDoc(doc + docBase, score);
                    maxScore = Math.max(currentDoc.score, maxScore);
                    pq.insertWithOverflow(currentDoc);
                }
                totalHitCount++;
            }

            private void initializeQueue(BytesRef groupValue, int numSubQueries) {
                PriorityQueue<ScoreDoc>[] compoundScores = new PriorityQueue[numSubQueries];
                for (int i = 0; i < numSubQueries; i++) {
                    compoundScores[i] = new HitQueue(numHits, false);
                }
                groupQueueMap.put(BytesRef.deepCopyOf(groupValue), compoundScores);
                collectedHitsPerSubQueryMap.put(BytesRef.deepCopyOf(groupValue), new int[numSubQueries]);
            }
        };
    }
}
