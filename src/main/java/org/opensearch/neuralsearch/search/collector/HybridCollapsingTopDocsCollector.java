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
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.search.grouping.GroupSelector;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.neuralsearch.query.HybridSubQueryScorer;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

/**
 * Collects the CollapseTopFieldDocs based on a collapse field passed in a search request containing a hybrid query.
 * Keeps the top {@code numHits} collapse groups per sub-query, each represented by its most competitive document
 * under the group sort, mirroring the bookkeeping of {@link org.apache.lucene.search.grouping.FirstPassGroupingCollector}.
 * The cross-shard collapse deduplication happens downstream in the normalization pipeline.
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
    private final int[] reversed;
    @Setter
    TotalHits.Relation totalHitsRelation = TotalHits.Relation.EQUAL_TO;
    private final HitsThresholdChecker hitsThresholdChecker;

    private List<SubQueryGroupCollector> subQueryCollectors;
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
        SortField[] sortFields = groupSort.getSort();
        this.reversed = new int[sortFields.length];
        for (int i = 0; i < sortFields.length; i++) {
            if (SortField.Type.SCORE.equals(sortFields[i].getType())) {
                sortByScore = true;
            }
            reversed[i] = sortFields[i].getReverse() ? -1 : 1;
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
     * Returns the collected top groups, including collapse values and sort fields, grouped by sub-query.
     * Emits one FieldDoc per surviving group (its representative document), best first.
     */
    @Override
    public List<CollapseTopFieldDocs> topDocs() throws IOException {
        List<CollapseTopFieldDocs> topDocsList = new ArrayList<>();
        if (subQueryCollectors == null) {
            return topDocsList;
        }

        for (SubQueryGroupCollector subQueryCollector : subQueryCollectors) {
            topDocsList.add(subQueryCollector.topGroups());
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

                groupSelector.advanceTo(doc);

                float[] subScoresByQuery = compoundQueryScorer.getSubQueryScores();
                ensureSubQueryCollectorsInitialized(subScoresByQuery.length);
                ensureLeafComparatorsInitialized(context, compoundQueryScorer);

                updateHitCount();

                for (int subQuery = 0; subQuery < subScoresByQuery.length; subQuery++) {
                    float score = subScoresByQuery[subQuery];
                    if (score == 0) {
                        continue;
                    }

                    if (isSortByScore && score <= 0 && score < minScoreThresholds[subQuery]) {
                        continue;
                    }

                    maxScore = Math.max(score, maxScore);
                    subQueryCollectors.get(subQuery).collect(doc, score, compoundQueryScorer);
                }
            }

            private void ensureSubQueryCollectorsInitialized(int numSubQueries) {
                if (subQueryCollectors != null) {
                    return;
                }
                subQueryCollectors = new ArrayList<>(numSubQueries);
                for (int subQuery = 0; subQuery < numSubQueries; subQuery++) {
                    subQueryCollectors.add(new SubQueryGroupCollector(subQuery));
                }
            }

            private void ensureLeafComparatorsInitialized(LeafReaderContext ctx, HybridSubQueryScorer compoundQueryScorer)
                throws IOException {
                if (leafComparatorsInitialized) {
                    return;
                }
                leafComparatorsInitialized = true;
                for (SubQueryGroupCollector subQueryCollector : subQueryCollectors) {
                    subQueryCollector.setNextReader(ctx, compoundQueryScorer);
                }
            }

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

    private static final class CollectedGroup<T> {
        T groupValue;
        int topDoc;
        float score;
        int comparatorSlot;
    }

    /**
     * Keeps the top {@code numHits} groups for one sub-query, mirroring the bookkeeping of
     * {@link org.apache.lucene.search.grouping.FirstPassGroupingCollector}.
     */
    private final class SubQueryGroupCollector {
        private final int subQueryIndex;
        private final FieldComparator<?>[] comparators;
        private final int compIDXEnd;
        private final Map<T, CollectedGroup<T>> groupMap;
        // Null until groupMap reaches numHits distinct groups
        private TreeSet<CollectedGroup<T>> orderedGroups;
        private int spareSlot;
        private int collectedHits;

        private LeafFieldComparator[] leafComparators;
        private HybridLeafFieldComparator scoreLeafComparator;

        SubQueryGroupCollector(int subQueryIndex) {
            this.subQueryIndex = subQueryIndex;
            SortField[] sortFields = sort.getSort();
            this.comparators = new FieldComparator<?>[sortFields.length];
            for (int i = 0; i < sortFields.length; i++) {
                // numHits + 1 slots so we have a spare slot to stage comparisons
                comparators[i] = sortFields[i].getComparator(numHits + 1, Pruning.NONE);
            }
            this.compIDXEnd = comparators.length - 1;
            this.spareSlot = numHits;
            this.groupMap = new HashMap<>();
        }

        void setNextReader(LeafReaderContext ctx, HybridSubQueryScorer compoundQueryScorer) throws IOException {
            leafComparators = new LeafFieldComparator[comparators.length];
            scoreLeafComparator = null;
            SortField[] sortFields = sort.getSort();
            for (int i = 0; i < comparators.length; i++) {
                LeafFieldComparator leafComparator = comparators[i].getLeafComparator(ctx);
                if (SortField.Type.SCORE.equals(sortFields[i].getType())) {
                    // Wrap so the comparator reads this sub-query's individual score instead of the sum
                    HybridLeafFieldComparator wrappedComparator = new HybridLeafFieldComparator(leafComparator);
                    scoreLeafComparator = wrappedComparator;
                    leafComparator = wrappedComparator;
                }
                leafComparator.setScorer(compoundQueryScorer);
                leafComparators[i] = leafComparator;
            }
        }

        void collect(int doc, float score, HybridSubQueryScorer compoundQueryScorer) throws IOException {
            collectedHits++;

            if (Objects.nonNull(scoreLeafComparator)) {
                scoreLeafComparator.setCurrentSubQueryScore(score);
            }

            // A doc below the weakest group's representative can neither form a new group nor improve an existing one
            if (Objects.nonNull(orderedGroups) && isCompetitive(doc) == false) {
                return;
            }

            CollectedGroup<T> group = groupMap.get(groupSelector.currentValue());
            if (Objects.isNull(group)) {
                collectNewGroup(doc, score, compoundQueryScorer);
            } else {
                collectExistingGroup(doc, score, group);
            }
        }

        private boolean isCompetitive(int doc) throws IOException {
            for (int compIDX = 0;; compIDX++) {
                final int c = reversed[compIDX] * leafComparators[compIDX].compareBottom(doc);
                if (c < 0) {
                    return false;
                } else if (c > 0) {
                    return true;
                } else if (compIDX == compIDXEnd) {
                    // Ties lose: docs are visited in doc id order
                    return false;
                }
            }
        }

        private void collectNewGroup(int doc, float score, HybridSubQueryScorer compoundQueryScorer) throws IOException {
            if (groupMap.size() < numHits) {
                CollectedGroup<T> group = new CollectedGroup<>();
                group.groupValue = groupSelector.copyValue();
                group.comparatorSlot = groupMap.size();
                group.topDoc = docBase + doc;
                group.score = score;
                for (LeafFieldComparator leafComparator : leafComparators) {
                    leafComparator.copy(group.comparatorSlot, doc);
                }
                groupMap.put(group.groupValue, group);

                if (groupMap.size() == numHits) {
                    buildSortedSet();
                    setBottomToWeakestGroup();
                }
                return;
            }

            CollectedGroup<T> evictedGroup = orderedGroups.pollLast();
            groupMap.remove(evictedGroup.groupValue);
            float evictedScore = evictedGroup.score;

            evictedGroup.groupValue = groupSelector.copyValue();
            evictedGroup.topDoc = docBase + doc;
            evictedGroup.score = score;
            for (LeafFieldComparator leafComparator : leafComparators) {
                leafComparator.copy(evictedGroup.comparatorSlot, doc);
            }
            groupMap.put(evictedGroup.groupValue, evictedGroup);
            orderedGroups.add(evictedGroup);
            setBottomToWeakestGroup();

            if (isSortByScore) {
                minScoreThresholds[subQueryIndex] = Math.max(minScoreThresholds[subQueryIndex], evictedScore);
                compoundQueryScorer.getMinScores()[subQueryIndex] = Math.max(
                    compoundQueryScorer.getMinScores()[subQueryIndex],
                    evictedScore
                );
            }
        }

        private void collectExistingGroup(int doc, float score, CollectedGroup<T> group) throws IOException {
            for (int compIDX = 0;; compIDX++) {
                leafComparators[compIDX].copy(spareSlot, doc);
                final int c = reversed[compIDX] * comparators[compIDX].compare(group.comparatorSlot, spareSlot);
                if (c < 0) {
                    return;
                } else if (c > 0) {
                    for (int compIDX2 = compIDX + 1; compIDX2 < comparators.length; compIDX2++) {
                        leafComparators[compIDX2].copy(spareSlot, doc);
                    }
                    break;
                } else if (compIDX == compIDXEnd) {
                    // Ties lose: docs are visited in doc id order
                    return;
                }
            }

            // Remove before mutating — the sorted set locates elements by comparing slots
            if (Objects.nonNull(orderedGroups)) {
                orderedGroups.remove(group);
            }

            group.topDoc = docBase + doc;
            group.score = score;
            // The staged spare slot becomes the group's slot, the old slot becomes spare
            final int tmpSlot = spareSlot;
            spareSlot = group.comparatorSlot;
            group.comparatorSlot = tmpSlot;

            if (Objects.nonNull(orderedGroups)) {
                orderedGroups.add(group);
                setBottomToWeakestGroup();
            }
        }

        private void buildSortedSet() {
            final Comparator<CollectedGroup<T>> groupComparator = (o1, o2) -> {
                for (int compIDX = 0;; compIDX++) {
                    final int c = reversed[compIDX] * comparators[compIDX].compare(o1.comparatorSlot, o2.comparatorSlot);
                    if (c != 0) {
                        return c;
                    } else if (compIDX == compIDXEnd) {
                        return o1.topDoc - o2.topDoc;
                    }
                }
            };
            orderedGroups = new TreeSet<>(groupComparator);
            orderedGroups.addAll(groupMap.values());
        }

        private void setBottomToWeakestGroup() throws IOException {
            final int weakestSlot = orderedGroups.last().comparatorSlot;
            for (LeafFieldComparator leafComparator : leafComparators) {
                leafComparator.setBottom(weakestSlot);
            }
        }

        CollapseTopFieldDocs topGroups() {
            if (collectedHits == 0 || groupMap.isEmpty()) {
                return new CollapseTopFieldDocs(
                    collapseField,
                    new TotalHits(0, totalHitsRelation),
                    new FieldDoc[0],
                    sort.getSort(),
                    new Object[0]
                );
            }

            if (Objects.isNull(orderedGroups)) {
                buildSortedSet();
            }

            int size = orderedGroups.size();
            int numComparators = comparators.length;
            FieldDoc[] fieldDocs = new FieldDoc[size];
            Object[] collapseValues = new Object[size];

            int index = 0;
            for (CollectedGroup<T> group : orderedGroups) {
                Object[] fields = new Object[numComparators];
                for (int k = 0; k < numComparators; k++) {
                    fields[k] = comparators[k].value(group.comparatorSlot);
                }
                fieldDocs[index] = new FieldDoc(group.topDoc, group.score, fields);
                // Group values were deep-copied by GroupSelector#copyValue when stored
                collapseValues[index] = group.groupValue;
                index++;
            }

            return new CollapseTopFieldDocs(
                collapseField,
                new TotalHits(collectedHits, totalHitsRelation),
                fieldDocs,
                sort.getSort(),
                collapseValues
            );
        }
    }
}
