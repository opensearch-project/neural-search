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
import org.apache.lucene.search.HitQueue;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.LeafFieldComparator;
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
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.neuralsearch.query.HybridQueryScorer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

@Log4j2
public class HybridCollapsingTopDocsCollector<T> implements HybridSearchCollector, Collector {
    protected final String collapseField;
    private int totalHitCount;
    private float maxScore = 0.0f;
    private Sort sort;
    private final GroupSelector<BytesRef> groupSelector;
    private final FieldComparator<?>[] comparators;
    private final LeafFieldComparator[] leafComparators;
    private final int[] reversed;
    private final boolean needsScores;
    private final int compIDXEnd;
    protected TreeSet<HybridCollectedSearchGroup<BytesRef>> orderedGroups;
    private int docBase;
    private final HashMap<BytesRef, HybridCollectedSearchGroup<BytesRef>> groupMap;
    private final HashMap<BytesRef, PriorityQueue<ScoreDoc>[]> groupQueueMap;
    private final int topNGroups;
    private PriorityQueue<HybridCollectedSearchGroup> groupQueue;
    private HashMap<BytesRef, int[]> collectedHitsPerSubQueryMap;
    private int totalHits;
    private int spareSlot;
    private final int numHits;

    public HybridCollapsingTopDocsCollector(GroupSelector<BytesRef> groupSelector, String collapseField, Sort groupSort, int topNGroups) {
        this.groupSelector = groupSelector;
        this.collapseField = collapseField;
        this.sort = groupSort;
        if (topNGroups < 1) {
            throw new IllegalArgumentException("topNGroups must be >= 1 (got " + topNGroups + ")");
        } else {
            this.topNGroups = topNGroups;
            this.needsScores = groupSort.needsScores();
            SortField[] sortFields = groupSort.getSort();
            this.comparators = new FieldComparator[sortFields.length];
            this.leafComparators = new LeafFieldComparator[sortFields.length];
            this.compIDXEnd = this.comparators.length - 1;
            this.reversed = new int[sortFields.length];

            for (int i = 0; i < sortFields.length; ++i) {
                SortField sortField = sortFields[i];
                this.comparators[i] = sortField.getComparator(topNGroups + 1, Pruning.NONE);
                this.reversed[i] = sortField.getReverse() ? -1 : 1;
            }

            this.spareSlot = topNGroups;
            this.groupMap = new HashMap<>();
            this.groupQueueMap = new HashMap<>();
            this.collectedHitsPerSubQueryMap = new HashMap<>();

            this.numHits = topNGroups;
        }
    }

    @Override
    public List<? extends TopDocs> topDocs() throws IOException {
        List<CollapseTopFieldDocs> topDocsList = new ArrayList<>();
        for (int i = 0; i < collectedHitsPerSubQueryMap.size(); i++) {
            ArrayList<ScoreDoc> scoreDocs = new ArrayList<>();
            ArrayList<BytesRef> collapseValues = new ArrayList<>();
            int hitsOnCurrentSubQuery = 0;
            for (Map.Entry<BytesRef, HybridCollectedSearchGroup<BytesRef>> entry : groupMap.entrySet()) {
                BytesRef groupValue = entry.getKey();
                PriorityQueue<ScoreDoc>[] priorityQueueList = groupQueueMap.get(groupValue);
                for (PriorityQueue<ScoreDoc> pq : priorityQueueList) {
                    for (ScoreDoc scoreDoc : pq) {
                        scoreDocs.add(scoreDoc);
                        collapseValues.add(groupValue);
                    }
                }
                hitsOnCurrentSubQuery += collectedHitsPerSubQueryMap.get(groupValue)[i];
            }
            topDocsList.add(new CollapseTopFieldDocs(collapseField, new TotalHits(hitsOnCurrentSubQuery, TotalHits.Relation.EQUAL_TO), scoreDocs.toArray(), null, collapseValues))
        }
    }

    @Override
    public int getTotalHits() {
        return totalHitCount;
    }

    @Override
    public float getMaxScore() {
        return 0;
    }

    @Override
    public ScoreMode scoreMode() {
        return this.needsScores ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    public Collection<SearchGroup<BytesRef>> getTopGroups(int groupOffset) throws IOException {
        if (groupOffset < 0) {
            throw new IllegalArgumentException("groupOffset must be >= 0 (got " + groupOffset + ")");
        } else if (this.groupMap.size() <= groupOffset) {
            return null;
        } else {
            if (this.orderedGroups == null) {
                this.buildSortedSet();
            }

            Collection<SearchGroup<BytesRef>> result = new ArrayList();
            int upto = 0;
            int sortFieldCount = this.comparators.length;
            Iterator var5 = this.orderedGroups.iterator();

            while (true) {
                HybridCollectedSearchGroup group;
                do {
                    if (!var5.hasNext()) {
                        return result;
                    }

                    group = (HybridCollectedSearchGroup) var5.next();
                } while (upto++ < groupOffset);

                SearchGroup<BytesRef> searchGroup = new SearchGroup();
                searchGroup.groupValue = (BytesRef) group.groupValue;
                searchGroup.sortValues = new Object[sortFieldCount];

                for (int sortFieldIDX = 0; sortFieldIDX < sortFieldCount; ++sortFieldIDX) {
                    searchGroup.sortValues[sortFieldIDX] = this.comparators[sortFieldIDX].value(group.comparatorSlot);
                }

                result.add(searchGroup);
            }
        }
    }

    private void buildSortedSet() throws IOException {
        Comparator<HybridCollectedSearchGroup<?>> comparator = new Comparator<HybridCollectedSearchGroup<?>>() {
            public int compare(HybridCollectedSearchGroup<?> o1, HybridCollectedSearchGroup<?> o2) {
                int compIDX = 0;

                while (true) {
                    FieldComparator<?> fc = HybridCollapsingTopDocsCollector.this.comparators[compIDX];
                    int c = HybridCollapsingTopDocsCollector.this.reversed[compIDX] * fc.compare(o1.comparatorSlot, o2.comparatorSlot);
                    if (c != 0) {
                        return c;
                    }

                    if (compIDX == HybridCollapsingTopDocsCollector.this.compIDXEnd) {
                        return o1.topDoc - o2.topDoc;
                    }

                    ++compIDX;
                }
            }
        };
        this.orderedGroups = new TreeSet(comparator);
        this.orderedGroups.addAll(this.groupMap.values());

        assert this.orderedGroups.size() > 0;

        LeafFieldComparator[] var2 = this.leafComparators;
        int var3 = var2.length;

        for (int var4 = 0; var4 < var3; ++var4) {
            LeafFieldComparator fc = var2[var4];
            fc.setBottom(((HybridCollectedSearchGroup) this.orderedGroups.last()).comparatorSlot);
        }
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
                    groupMap.put(groupValue, newGroup);
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
                groupQueueMap.put(groupValue, compoundScores);
                collectedHitsPerSubQueryMap.put(groupValue, new int[numSubQueries]);
            }
        };
    }
}
