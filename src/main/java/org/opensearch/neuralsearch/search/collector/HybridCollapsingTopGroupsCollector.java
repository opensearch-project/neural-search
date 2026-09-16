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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

/**
 * Collects the CollapseTopFieldDocs based on a collapse field passed in a search request containing a hybrid query.
 * Keeps, per sub-query, the top {@code numHits} collapse groups ranked by that sub-query's own score for the
 * group's representative, mirroring the bookkeeping of {@link org.apache.lucene.search.grouping.FirstPassGroupingCollector}.
 * The cross-shard collapse deduplication happens downstream in the normalization pipeline.
 *
 * <p>This collector is the opt-in alternative to {@link HybridCollapsingTopDocsCollector}, selected when
 * {@code index.neural_search.hybrid_collapse_distinct_groups_enabled} is set on the index — see
 * https://github.com/opensearch-project/neural-search/issues/1947 for why the two behaviors are mutually exclusive.
 *
 * <p>Representative election is leg-independent: sub-queries rank documents differently, so a per-sub-query
 * election would elect different documents for one group and split the group's score in the downstream
 * per-document fusion. A single election is run instead — when sorting by score the comparators read
 * {@link HybridSubQueryScorer#score()}, the sum over sub-queries. Candidate selection stays per sub-query:
 * each sub-query keeps its own top-{@code numHits} groups, ranked and emitted by its best score over the
 * group's documents in that sub-query (a running max, or a running min under a reversed score sort), so a
 * sub-query keeps every group it matched regardless of which document was elected. No minimum-competitive-score feedback is sent to
 * {@link HybridSubQueryScorer#getMinScores()}, since a low score in one sub-query does not disqualify a
 * document whose other sub-query scores make it the representative.
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
    private final boolean[] isScoreSortField;
    private final boolean isScoreSortReversed;
    private final int compIDXEnd;
    @Setter
    TotalHits.Relation totalHitsRelation = TotalHits.Relation.EQUAL_TO;
    private final HitsThresholdChecker hitsThresholdChecker;

    // Election state shared by all sub-queries (see class javadoc)
    private final Map<T, CollectedGroup<T>> groupMap;
    private FieldComparator<?>[] comparators;
    private int spareSlot;
    private Deque<Integer> freeSlots;
    private LeafFieldComparator[] leafComparators;

    // Per-sub-query bookkeeping, sized on the first collected document
    private int numSubQueries = -1;
    private int[] collectedHitsPerSubQuery;
    private List<TreeSet<CollectedGroup<T>>> orderedGroupsPerSubQuery;

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
        this.isScoreSortField = new boolean[sortFields.length];
        boolean reversedScoreSort = false;
        for (int i = 0; i < sortFields.length; i++) {
            reversed[i] = sortFields[i].getReverse() ? -1 : 1;
            isScoreSortField[i] = SortField.Type.SCORE.equals(sortFields[i].getType());
            if (isScoreSortField[i] && reversed[i] == -1) {
                reversedScoreSort = true;
            }
        }
        this.isScoreSortReversed = reversedScoreSort;
        this.compIDXEnd = sortFields.length - 1;
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
     * Every sub-query emits the same elected representative per group, best group by that sub-query's
     * best matched score first, each entry carrying that score; a sub-query that matched none of a group's
     * documents leaves the group out of its list.
     */
    @Override
    public List<CollapseTopFieldDocs> topDocs() throws IOException {
        List<CollapseTopFieldDocs> topDocsList = new ArrayList<>();
        if (numSubQueries < 0) {
            return topDocsList;
        }

        int numComparators = comparators.length;
        for (int subQuery = 0; subQuery < numSubQueries; subQuery++) {
            List<FieldDoc> fieldDocs = new ArrayList<>();
            List<Object> collapseValues = new ArrayList<>();
            for (CollectedGroup<T> group : orderedGroupsPerSubQuery.get(subQuery)) {
                float subQueryScore = group.scoresPerSubQuery[subQuery];
                Object[] fields = new Object[numComparators];
                for (int k = 0; k < numComparators; k++) {
                    // Score components carry this sub-query's own score; other components are attributes of the representative
                    fields[k] = isScoreSortField[k] ? subQueryScore : comparators[k].value(group.comparatorSlot);
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
                orderedGroupsPerSubQuery = new ArrayList<>(subQueryCount);
                for (int subQuery = 0; subQuery < subQueryCount; subQuery++) {
                    orderedGroupsPerSubQuery.add(new TreeSet<>(subQueryGroupComparator(subQuery)));
                }
                // One slot per live group (at most numSubQueries * numHits memberships), one transient slot
                // for insert-then-trim, and the spare slot used to stage election comparisons
                int slotCapacity = subQueryCount * numHits + 2;
                SortField[] sortFields = sort.getSort();
                comparators = new FieldComparator<?>[sortFields.length];
                for (int i = 0; i < sortFields.length; i++) {
                    comparators[i] = sortFields[i].getComparator(slotCapacity, Pruning.NONE);
                }
                spareSlot = slotCapacity - 1;
                freeSlots = new ArrayDeque<>();
                for (int slot = 0; slot < slotCapacity - 1; slot++) {
                    freeSlots.push(slot);
                }
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
        CollectedGroup<T> group = groupMap.get(groupSelector.currentValue());
        if (Objects.isNull(group)) {
            collectNewGroup(doc, subScoresByQuery);
        } else {
            collectExistingGroup(doc, subScoresByQuery, group);
        }
    }

    private void collectNewGroup(int doc, float[] subScoresByQuery) throws IOException {
        CollectedGroup<T> group = new CollectedGroup<>();
        group.groupValue = groupSelector.copyValue();
        group.comparatorSlot = freeSlots.pop();
        group.topDoc = docBase + doc;
        group.scoresPerSubQuery = new float[numSubQueries];
        for (int subQuery = 0; subQuery < numSubQueries; subQuery++) {
            group.scoresPerSubQuery[subQuery] = foldScore(noMatchSentinel(), subScoresByQuery[subQuery]);
        }
        group.memberOfSubQuery = new boolean[numSubQueries];
        for (LeafFieldComparator leafComparator : leafComparators) {
            leafComparator.copy(group.comparatorSlot, doc);
        }
        groupMap.put(group.groupValue, group);
        insertIntoSubQueries(group);
        dropIfMemberless(group);
    }

    private void collectExistingGroup(int doc, float[] subScoresByQuery, CollectedGroup<T> group) throws IOException {
        boolean electionWon = docWinsElection(doc, group);
        boolean anyAggregateImproved = false;
        for (int subQuery = 0; subQuery < numSubQueries; subQuery++) {
            if (improvesAggregate(group.scoresPerSubQuery[subQuery], subScoresByQuery[subQuery])) {
                anyAggregateImproved = true;
                break;
            }
        }
        if (electionWon == false && anyAggregateImproved == false) {
            return;
        }

        // Remove before mutating — the sorted sets locate elements by comparing scores, slots, and the
        // topDoc tiebreak, so an election win invalidates the group's position in every member set
        for (int subQuery = 0; subQuery < numSubQueries; subQuery++) {
            boolean affected = electionWon || improvesAggregate(group.scoresPerSubQuery[subQuery], subScoresByQuery[subQuery]);
            if (affected && group.memberOfSubQuery[subQuery]) {
                orderedGroupsPerSubQuery.get(subQuery).remove(group);
                group.memberOfSubQuery[subQuery] = false;
            }
        }

        for (int subQuery = 0; subQuery < numSubQueries; subQuery++) {
            group.scoresPerSubQuery[subQuery] = foldScore(group.scoresPerSubQuery[subQuery], subScoresByQuery[subQuery]);
        }
        if (electionWon) {
            group.topDoc = docBase + doc;
            // The staged spare slot becomes the group's slot, the old slot becomes spare
            final int tmpSlot = spareSlot;
            spareSlot = group.comparatorSlot;
            group.comparatorSlot = tmpSlot;
        }

        insertIntoSubQueries(group);
        dropIfMemberless(group);
    }

    private boolean docWinsElection(int doc, CollectedGroup<T> group) throws IOException {
        for (int compIDX = 0;; compIDX++) {
            leafComparators[compIDX].copy(spareSlot, doc);
            final int c = reversed[compIDX] * comparators[compIDX].compare(group.comparatorSlot, spareSlot);
            if (c < 0) {
                return false;
            } else if (c > 0) {
                for (int compIDX2 = compIDX + 1; compIDX2 < comparators.length; compIDX2++) {
                    leafComparators[compIDX2].copy(spareSlot, doc);
                }
                return true;
            } else if (compIDX == compIDXEnd) {
                // Ties lose: docs are visited in doc id order
                return false;
            }
        }
    }

    private void insertIntoSubQueries(CollectedGroup<T> group) {
        for (int subQuery = 0; subQuery < numSubQueries; subQuery++) {
            if (hasMatch(group.scoresPerSubQuery[subQuery]) == false || group.memberOfSubQuery[subQuery]) {
                continue;
            }
            TreeSet<CollectedGroup<T>> orderedGroups = orderedGroupsPerSubQuery.get(subQuery);
            orderedGroups.add(group);
            group.memberOfSubQuery[subQuery] = true;
            if (orderedGroups.size() > numHits) {
                CollectedGroup<T> evicted = orderedGroups.pollLast();
                evicted.memberOfSubQuery[subQuery] = false;
                if (evicted != group) {
                    dropIfMemberless(evicted);
                }
            }
        }
    }

    // The per-sub-query aggregate follows the score sort direction: a running max, or a running min
    // seeded with +Infinity under a reversed score sort, folding matched (positive) scores only
    private float foldScore(float aggregate, float score) {
        if (score <= 0) {
            return aggregate;
        }
        return isScoreSortReversed ? Math.min(aggregate, score) : Math.max(aggregate, score);
    }

    private float noMatchSentinel() {
        return isScoreSortReversed ? Float.POSITIVE_INFINITY : 0f;
    }

    private boolean hasMatch(float aggregate) {
        return isScoreSortReversed ? aggregate != Float.POSITIVE_INFINITY : aggregate > 0;
    }

    private boolean improvesAggregate(float aggregate, float score) {
        if (score <= 0) {
            return false;
        }
        return isScoreSortReversed ? score < aggregate : score > aggregate;
    }

    private void dropIfMemberless(CollectedGroup<T> group) {
        for (boolean member : group.memberOfSubQuery) {
            if (member) {
                return;
            }
        }
        groupMap.remove(group.groupValue);
        freeSlots.push(group.comparatorSlot);
    }

    private Comparator<CollectedGroup<T>> subQueryGroupComparator(int subQuery) {
        return (o1, o2) -> {
            for (int compIDX = 0;; compIDX++) {
                final int c = reversed[compIDX] * compareBySortField(compIDX, subQuery, o1, o2);
                if (c != 0) {
                    return c;
                } else if (compIDX == compIDXEnd) {
                    return o1.topDoc - o2.topDoc;
                }
            }
        };
    }

    private int compareBySortField(int compIDX, int subQuery, CollectedGroup<T> o1, CollectedGroup<T> o2) {
        if (isScoreSortField[compIDX]) {
            // Mirrors RelevanceComparator: natural order puts the higher score first
            return Float.compare(o2.scoresPerSubQuery[subQuery], o1.scoresPerSubQuery[subQuery]);
        }
        return comparators[compIDX].compare(o1.comparatorSlot, o2.comparatorSlot);
    }

    private static final class CollectedGroup<T> {
        T groupValue;
        int topDoc;
        int comparatorSlot;
        // Best matched score per sub-query in the score sort's direction (see foldScore); the sentinel
        // marks a sub-query that matched none of the group's documents
        float[] scoresPerSubQuery;
        // Membership in each sub-query's top-numHits set; a group that is member of none is dropped
        boolean[] memberOfSubQuery;
    }
}
