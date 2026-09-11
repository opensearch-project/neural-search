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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

/**
 * Collects the CollapseTopFieldDocs based on a collapse field passed in a search request containing a hybrid query.
 * Keeps the top {@code numHits} collapse groups, each represented by its most competitive document under the group
 * sort, mirroring the bookkeeping of {@link org.apache.lucene.search.grouping.FirstPassGroupingCollector}.
 * The cross-shard collapse deduplication happens downstream in the normalization pipeline.
 *
 * <p>This collector is the opt-in alternative to {@link HybridCollapsingTopDocsCollector}, selected when
 * {@code index.neural_search.hybrid_collapse_distinct_groups_enabled} is set on the index — see
 * https://github.com/opensearch-project/neural-search/issues/1947 for why the two behaviors are mutually exclusive.
 *
 * <p>Representative election is leg-independent: sub-queries rank documents differently, so a per-sub-query
 * election would elect different documents for one group and split the group's score in the downstream
 * per-document fusion. A single election is run instead — when sorting by score the comparators read
 * {@link HybridSubQueryScorer#score()}, the sum over sub-queries — and every sub-query reports its own score
 * for the one elected representative. A sub-query that did not match the representative leaves it out of its
 * list (a zero-score entry would distort its normalization statistics), and no minimum-competitive-score
 * feedback is sent to {@link HybridSubQueryScorer#getMinScores()}, since a low score in one sub-query does not
 * disqualify a document whose other sub-query scores make it the representative.
 */

@Log4j2
public class HybridCollapsingTopGroupsCollector<T> implements HybridSearchCollector {
    protected final String collapseField;
    private int totalHitCount;
    private float maxScore = 0.0f;
    private final Sort sort;
    private final GroupSelector<T> groupSelector;
    private int docBase;
    private final int numHits;
    private final int[] reversed;
    @Setter
    TotalHits.Relation totalHitsRelation = TotalHits.Relation.EQUAL_TO;
    private final HitsThresholdChecker hitsThresholdChecker;

    // Single, leg-independent election state (see class javadoc)
    private final FieldComparator<?>[] comparators;
    private final int compIDXEnd;
    private final Map<T, CollectedGroup<T>> groupMap;
    // Null until groupMap reaches numHits distinct groups
    private TreeSet<CollectedGroup<T>> orderedGroups;
    private int spareSlot;
    private LeafFieldComparator[] leafComparators;

    // Per-sub-query bookkeeping, sized on the first collected document
    private int numSubQueries = -1;
    private int[] collectedHitsPerSubQuery;

    HybridCollapsingTopGroupsCollector(
        GroupSelector<T> groupSelector,
        String collapseField,
        @NonNull Sort groupSort,
        int topNGroups,
        HitsThresholdChecker hitsThresholdChecker
    ) {
        this.groupSelector = groupSelector;
        this.collapseField = collapseField;
        this.sort = groupSort;

        SortField[] sortFields = groupSort.getSort();
        this.reversed = new int[sortFields.length];
        this.comparators = new FieldComparator<?>[sortFields.length];
        for (int i = 0; i < sortFields.length; i++) {
            // numHits + 1 slots so we have a spare slot to stage comparisons
            comparators[i] = sortFields[i].getComparator(topNGroups + 1, Pruning.NONE);
            reversed[i] = sortFields[i].getReverse() ? -1 : 1;
        }
        this.compIDXEnd = comparators.length - 1;
        this.spareSlot = topNGroups;
        this.groupMap = new HashMap<>();
        this.numHits = topNGroups;
        this.hitsThresholdChecker = hitsThresholdChecker;
    }

    /**
     * Creates a HybridCollapsingTopGroupsCollector for keyword fields.
     */
    public static HybridCollapsingTopGroupsCollector<?> createKeyword(
        String collapseField,
        MappedFieldType fieldType,
        Sort sort,
        int topNGroups,
        HitsThresholdChecker hitsThresholdChecker
    ) {
        return new HybridCollapsingTopGroupsCollector<>(
            new CollapseDocSourceGroupSelector.Keyword(fieldType),
            collapseField,
            sort,
            topNGroups,
            hitsThresholdChecker
        );
    }

    /**
     * Creates a HybridCollapsingTopGroupsCollector for numeric fields.
     */
    public static HybridCollapsingTopGroupsCollector<?> createNumeric(
        String collapseField,
        MappedFieldType fieldType,
        Sort sort,
        int topNGroups,
        HitsThresholdChecker hitsThresholdChecker
    ) {
        return new HybridCollapsingTopGroupsCollector<>(
            new CollapseDocSourceGroupSelector.Numeric(fieldType),
            collapseField,
            sort,
            topNGroups,
            hitsThresholdChecker
        );
    }

    /**
     * Returns the collected top groups, including collapse values and sort fields, grouped by sub-query.
     * Every sub-query emits the same elected representative per group, best group first, each carrying that
     * sub-query's own score; representatives a sub-query did not match are left out of its list.
     */
    @Override
    public List<CollapseTopFieldDocs> topDocs() throws IOException {
        List<CollapseTopFieldDocs> topDocsList = new ArrayList<>();
        if (numSubQueries < 0) {
            return topDocsList;
        }

        if (Objects.isNull(orderedGroups)) {
            buildSortedSet();
        }

        int numComparators = comparators.length;
        for (int subQuery = 0; subQuery < numSubQueries; subQuery++) {
            List<FieldDoc> fieldDocs = new ArrayList<>();
            List<Object> collapseValues = new ArrayList<>();
            for (CollectedGroup<T> group : orderedGroups) {
                float subQueryScore = group.scoresPerSubQuery[subQuery];
                if (subQueryScore <= 0) {
                    continue;
                }
                Object[] fields = new Object[numComparators];
                for (int k = 0; k < numComparators; k++) {
                    fields[k] = comparators[k].value(group.comparatorSlot);
                }
                fieldDocs.add(new FieldDoc(group.topDoc, subQueryScore, fields));
                if (group.groupValue instanceof BytesRef) {
                    collapseValues.add(BytesRef.deepCopyOf((BytesRef) group.groupValue));
                } else {
                    collapseValues.add(group.groupValue);
                }
            }
            topDocsList.add(
                new CollapseTopFieldDocs(
                    collapseField,
                    new TotalHits(collectedHitsPerSubQuery[subQuery], totalHitsRelation),
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
        return ScoreMode.COMPLETE;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        docBase = context.docBase;
        groupSelector.setNextReader(context);

        return new HybridLeafCollector() {
            private boolean leafComparatorsInitialized = false;

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
                ensurePerSubQueryStateInitialized(subScoresByQuery.length);
                ensureLeafComparatorsInitialized(context, compoundQueryScorer);

                updateHitCount();

                for (int subQuery = 0; subQuery < subScoresByQuery.length; subQuery++) {
                    float score = subScoresByQuery[subQuery];
                    if (score > 0) {
                        collectedHitsPerSubQuery[subQuery]++;
                        maxScore = Math.max(score, maxScore);
                    }
                }

                // Leg-independent election: when sorting by score the leaf comparators read the compound
                // scorer's score(), the sum over sub-queries, so every sub-query agrees on the outcome.
                collectGroup(doc, subScoresByQuery);
            }

            private void ensurePerSubQueryStateInitialized(int subQueryCount) {
                if (numSubQueries >= 0) {
                    return;
                }
                numSubQueries = subQueryCount;
                collectedHitsPerSubQuery = new int[subQueryCount];
            }

            private void ensureLeafComparatorsInitialized(LeafReaderContext ctx, HybridSubQueryScorer compoundQueryScorer)
                throws IOException {
                if (leafComparatorsInitialized) {
                    return;
                }
                leafComparatorsInitialized = true;
                leafComparators = new LeafFieldComparator[comparators.length];
                for (int i = 0; i < comparators.length; i++) {
                    LeafFieldComparator leafComparator = comparators[i].getLeafComparator(ctx);
                    leafComparator.setScorer(compoundQueryScorer);
                    leafComparators[i] = leafComparator;
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

    private void collectGroup(int doc, float[] subScoresByQuery) throws IOException {
        // A doc below the weakest group's representative can neither form a new group nor improve an existing one
        if (Objects.nonNull(orderedGroups) && isCompetitive(doc) == false) {
            return;
        }

        CollectedGroup<T> group = groupMap.get(groupSelector.currentValue());
        if (Objects.isNull(group)) {
            collectNewGroup(doc, subScoresByQuery);
        } else {
            collectExistingGroup(doc, subScoresByQuery, group);
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

    private void collectNewGroup(int doc, float[] subScoresByQuery) throws IOException {
        if (groupMap.size() < numHits) {
            CollectedGroup<T> group = new CollectedGroup<>();
            group.groupValue = groupSelector.copyValue();
            group.comparatorSlot = groupMap.size();
            group.topDoc = docBase + doc;
            group.scoresPerSubQuery = subScoresByQuery.clone();
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

        evictedGroup.groupValue = groupSelector.copyValue();
        evictedGroup.topDoc = docBase + doc;
        evictedGroup.scoresPerSubQuery = subScoresByQuery.clone();
        for (LeafFieldComparator leafComparator : leafComparators) {
            leafComparator.copy(evictedGroup.comparatorSlot, doc);
        }
        groupMap.put(evictedGroup.groupValue, evictedGroup);
        orderedGroups.add(evictedGroup);
        setBottomToWeakestGroup();
    }

    private void collectExistingGroup(int doc, float[] subScoresByQuery, CollectedGroup<T> group) throws IOException {
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
        group.scoresPerSubQuery = subScoresByQuery.clone();
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

    private static final class CollectedGroup<T> {
        T groupValue;
        int topDoc;
        int comparatorSlot;
        // The elected representative's score in each sub-query; a sub-query that did not match it holds 0
        float[] scoresPerSubQuery;
    }
}
