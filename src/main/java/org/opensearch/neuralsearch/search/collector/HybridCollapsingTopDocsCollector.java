/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.search.grouping.GroupSelector;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

public class HybridCollapsingTopDocsCollector<T> extends SimpleCollector implements HybridSearchCollector {
    protected final String collapseField;
    private int totalHitCount;
    private float maxScore;
    private Sort sort;
    private final GroupSelector<BytesRef> groupSelector;
    private final int topNGroups;
    private final FieldComparator<?>[] comparators;
    private final LeafFieldComparator[] leafComparators;
    private final int[] reversed;
    private final boolean needsScores;
    private final int compIDXEnd;
    protected TreeSet<HybridCollectedSearchGroup<BytesRef>> orderedGroups;
    private int docBase;
    private int spareSlot;
    private final HashMap<BytesRef, HybridCollectedSearchGroup<BytesRef>> groupMap;
    private PriorityQueue<ScoreDoc>[] compoundScores;

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
            this.groupMap = CollectionUtil.newHashMap(topNGroups);
        }
    }

    @Override
    public List<? extends TopDocs> topDocs() throws IOException {
        Collection<SearchGroup<BytesRef>> groups = getTopGroups(0);
        List<CollapseTopFieldDocs> topDocsList = new ArrayList<>();
        if (groups == null) {
            TotalHits totalHits = new TotalHits(0L, TotalHits.Relation.EQUAL_TO);
            topDocsList.add(new CollapseTopFieldDocs(this.collapseField, totalHits, new ScoreDoc[0], this.sort.getSort(), new Object[0]));
            return topDocsList;
        } else {
            FieldDoc[] docs = new FieldDoc[groups.size()];
            Object[] collapseValues = new Object[groups.size()];
            int scorePos = -1;

            int pos;
            for (pos = 0; pos < this.sort.getSort().length; ++pos) {
                SortField sortField = this.sort.getSort()[pos];
                if (sortField.getType() == SortField.Type.SCORE) {
                    scorePos = pos;
                    break;
                }
            }

            pos = 0;
            Iterator<HybridCollectedSearchGroup<BytesRef>> it = this.orderedGroups.iterator();

            for (Iterator var7 = groups.iterator(); var7.hasNext(); ++pos) {
                SearchGroup<T> group = (SearchGroup) var7.next();

                assert it.hasNext();

                HybridCollectedSearchGroup<T> col = (HybridCollectedSearchGroup) it.next();
                float score = Float.NaN;
                if (scorePos != -1) {
                    score = (Float) group.sortValues[scorePos];
                }

                docs[pos] = new FieldDoc(col.topDoc, score, group.sortValues);
                collapseValues[pos] = group.groupValue;
            }

            TotalHits totalHits = new TotalHits((long) this.totalHitCount, TotalHits.Relation.EQUAL_TO);
            topDocsList.add(new CollapseTopFieldDocs(this.collapseField, totalHits, docs, this.sort.getSort(), collapseValues));
            return topDocsList;
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

    public void collect(int doc) throws IOException {
        this.groupSelector.advanceTo(doc);
        BytesRef groupValue = this.groupSelector.currentValue();
        HybridCollectedSearchGroup<BytesRef> group = (HybridCollectedSearchGroup) this.groupMap.get(groupValue);
        int lastComparatorSlot;
        int compIDX2;
        int var8;
        HybridCollectedSearchGroup prevLast;
        if (group == null) {
            LeafFieldComparator[] var12;
            int var14;
            LeafFieldComparator fc;
            if (this.groupMap.size() < this.topNGroups) {
                prevLast = new HybridCollectedSearchGroup();
                prevLast.groupValue = this.groupSelector.copyValue();
                prevLast.comparatorSlot = this.groupMap.size();
                prevLast.topDoc = this.docBase + doc;
                var12 = this.leafComparators;
                compIDX2 = var12.length;

                for (var14 = 0; var14 < compIDX2; ++var14) {
                    fc = var12[var14];
                    fc.copy(prevLast.comparatorSlot, doc);
                }

                this.groupMap.put((BytesRef) prevLast.groupValue, prevLast);
                /*
                if (this.groupMap.size() == this.topNGroups) {
                    this.buildSortedSet();
                }
                 */

            } else {
                prevLast = (HybridCollectedSearchGroup) this.orderedGroups.pollLast();

                assert this.orderedGroups.size() == this.topNGroups - 1;

                this.groupMap.remove(prevLast.groupValue);
                prevLast.groupValue = this.groupSelector.copyValue();
                prevLast.topDoc = this.docBase + doc;
                var12 = this.leafComparators;
                compIDX2 = var12.length;

                for (var14 = 0; var14 < compIDX2; ++var14) {
                    fc = var12[var14];
                    fc.copy(prevLast.comparatorSlot, doc);
                }

                this.groupMap.put((BytesRef) prevLast.groupValue, prevLast);
                this.orderedGroups.add(prevLast);

                assert this.orderedGroups.size() == this.topNGroups;

                lastComparatorSlot = ((HybridCollectedSearchGroup) this.orderedGroups.last()).comparatorSlot;
                LeafFieldComparator[] var15 = this.leafComparators;
                var14 = var15.length;

                for (var8 = 0; var8 < var14; ++var8) {
                    fc = var15[var8];
                    fc.setBottom(lastComparatorSlot);
                }

            }
        } else {
            int compIDX = 0;

            while (true) {
                this.leafComparators[compIDX].copy(this.spareSlot, doc);
                lastComparatorSlot = this.reversed[compIDX] * this.comparators[compIDX].compare(group.comparatorSlot, this.spareSlot);
                if (lastComparatorSlot < 0) {
                    return;
                }

                if (lastComparatorSlot > 0) {
                    for (compIDX2 = compIDX + 1; compIDX2 < this.comparators.length; ++compIDX2) {
                        this.leafComparators[compIDX2].copy(this.spareSlot, doc);
                    }

                    if (this.orderedGroups != null) {
                        prevLast = (HybridCollectedSearchGroup) this.orderedGroups.last();
                        this.orderedGroups.remove(group);

                        assert this.orderedGroups.size() == this.topNGroups - 1;
                    } else {
                        prevLast = null;
                    }

                    group.topDoc = this.docBase + doc;
                    lastComparatorSlot = this.spareSlot;
                    this.spareSlot = group.comparatorSlot;
                    group.comparatorSlot = lastComparatorSlot;
                    if (this.orderedGroups != null) {
                        this.orderedGroups.add(group);

                        assert this.orderedGroups.size() == this.topNGroups;

                        HybridCollectedSearchGroup<?> newLast = (HybridCollectedSearchGroup) this.orderedGroups.last();
                        if (group == newLast || prevLast != newLast) {
                            LeafFieldComparator[] var7 = this.leafComparators;
                            var8 = var7.length;

                            for (int var9 = 0; var9 < var8; ++var9) {
                                LeafFieldComparator fc = var7[var9];
                                fc.setBottom(newLast.comparatorSlot);
                            }
                        }
                    }

                    return;
                }

                if (compIDX == this.compIDXEnd) {
                    return;
                }

                ++compIDX;
            }
        }
        totalHitCount++;
    }

    @Override
    public ScoreMode scoreMode() {
        return this.needsScores ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    protected void doSetNextReader(LeafReaderContext readerContext) throws IOException {
        this.docBase = readerContext.docBase;

        for (int i = 0; i < this.comparators.length; ++i) {
            this.leafComparators[i] = this.comparators[i].getLeafComparator(readerContext);
        }

        this.groupSelector.setNextReader(readerContext);
    }

    public void setScorer(Scorable scorer) throws IOException {
        this.groupSelector.setScorer(scorer);
        LeafFieldComparator[] var2 = this.leafComparators;
        int var3 = var2.length;

        for (int var4 = 0; var4 < var3; ++var4) {
            LeafFieldComparator comparator = var2[var4];
            comparator.setScorer(scorer);
        }
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
}
