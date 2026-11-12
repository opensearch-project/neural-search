/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query.util;

import lombok.RequiredArgsConstructor;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.opensearch.common.Nullable;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.neuralsearch.search.collector.HybridCollapsingTopDocsCollector;
import org.opensearch.neuralsearch.search.collector.HybridSearchCollector;
import org.opensearch.neuralsearch.search.query.HybridCollectorResultsUtilParams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Reduces the hybrid query results of a shard's collectors, one per concurrent segment search slice, into one shard result
 * (https://github.com/opensearch-project/neural-search/issues/2091). Every sub-query's slice lists are merged, by score, by the
 * request's sort or by the collapse sort, and cut to numHits before the result is encoded, and rescore sees one window per
 * sub-query. When every slice collector's lists are the top hits of its own segments, each sub-query keeps the hits, order,
 * scores, sort values and collapse values that a single collector over all the shard's segments keeps. The shard total stays the
 * sum of the collectors' counts, as before; it can differ from a single collector's count once the hybrid bulk scorer skips docs
 * that are not competitive.
 */
@RequiredArgsConstructor
public class HybridSliceResultsReducer {

    /**
     * Slice collectors keep the lower doc id on a score or sort value tie (Lucene's {@code HitQueue} and
     * {@code FieldValueHitQueue}), slices visit their segments in ascending doc order, and doc ids are global and unique within a
     * shard, so breaking merge ties by ascending doc id reproduces one collector's order. The exception is the reversed leaf order
     * that core applies to ascending sorts on time series indices: there one collector keeps the doc of the segment it visits first
     * on an exact tie at the cut, while the merge keeps the lower doc id, as Lucene's own merge of sorted slice results does.
     */
    private static final Comparator<ScoreDoc> DOC_ID_ASC = Comparator.comparingInt(scoreDoc -> scoreDoc.doc);

    private final HybridCollectorResultsUtilParams params;
    private final int numHits;

    /**
     * Merges the collectors' per sub-query top docs, cut to numHits, rescores each merged list once unless the request sorts by a
     * field or collapses, and encodes the shard result. Supports score ordered, field sorted and default-mode collapse results;
     * distinct-groups collapse results cannot be merged across slices.
     * @param collectors the shard's collectors, at least one
     * @return the encoded shard result and its max score
     */
    public TopDocsAndMaxScore reduce(final List<HybridSearchCollector> collectors) throws IOException {
        if (params.isCollapseEnabled()
            && collectors.stream().anyMatch(collector -> !(collector instanceof HybridCollapsingTopDocsCollector))) {
            throw new IllegalStateException("only default-mode collapse results of a hybrid query can be merged across slices");
        }
        final List<List<? extends TopDocs>> perSliceSubQueryTopDocs = new ArrayList<>(collectors.size());
        long totalHits = 0;
        // collectors report a max score of 0 when they see no document
        float maxScore = 0.0f;
        for (HybridSearchCollector collector : collectors) {
            final List<? extends TopDocs> subQueryTopDocs = collector.topDocs();
            maxScore = Math.max(maxScore, collector.getMaxScore());
            // a collector that saw no document returns no lists and adds no hits, as in the per-collector fold
            if (subQueryTopDocs.isEmpty()) {
                continue;
            }
            totalHits += collector.getTotalHits();
            perSliceSubQueryTopDocs.add(subQueryTopDocs);
        }
        final TotalHits shardTotalHits = new TotalHits(totalHits, HybridSearchCollectorResultUtil.totalHitsRelation(params));
        if (params.isCollapseEnabled()) {
            final String collapseField = params.getSearchContext().collapse().getFieldName();
            final List<CollapseTopFieldDocs> merged = mergeCollapseSubQueryTopDocs(
                perSliceSubQueryTopDocs,
                numHits,
                new Sort(params.getSortFields()),
                collapseField
            );
            // collapse results are not rescored: core rejects collapse with rescore
            return new TopDocsAndMaxScore(
                HybridSearchCollectorResultUtil.getCollapseTopFieldDocs(collapseField, shardTotalHits, merged, params.getSortFields()),
                maxScore
            );
        }
        if (params.isSortEnabled()) {
            final List<TopDocs> merged = mergeSubQueryTopDocs(perSliceSubQueryTopDocs, numHits, params.getSortAndFormats().sort);
            // sorted results are not rescored, as on the per-collector path
            return new TopDocsAndMaxScore(
                HybridSearchCollectorResultUtil.getNewTopFieldDocs(
                    shardTotalHits,
                    merged.stream().map(TopFieldDocs.class::cast).toList(),
                    params.getSortFields()
                ),
                maxScore
            );
        }
        List<TopDocs> merged = mergeSubQueryTopDocs(perSliceSubQueryTopDocs, numHits, null);
        // one rescore window per sub-query over the merged shard list, not one per slice
        if (HybridSearchCollectorResultUtil.shouldRescore(params)) {
            merged = HybridSearchCollectorResultUtil.rescore(merged, params);
        }
        maxScore = HybridSearchCollectorResultUtil.calculateMaxScore(merged, maxScore, params);
        return new TopDocsAndMaxScore(HybridSearchCollectorResultUtil.getNewTopDocs(shardTotalHits, merged), maxScore);
    }

    /**
     * Merges every sub-query's lists from all slices into its top numHits: score descending, or by the sort, and doc id ascending
     * on a tie. The merged lists' total hits are not used: the shard total comes from the collectors.
     * @param perSliceSubQueryTopDocs per slice, the sub-query lists in sub-query order
     * @param numHits hits to keep per sub-query
     * @param sort the request's sort, null to merge by score; with a sort every list must be a {@link TopFieldDocs}
     * @return merged list per sub-query, in sub-query order, a {@link TopFieldDocs} for each when sorting
     */
    static List<TopDocs> mergeSubQueryTopDocs(
        final List<List<? extends TopDocs>> perSliceSubQueryTopDocs,
        final int numHits,
        @Nullable final Sort sort
    ) {
        if (perSliceSubQueryTopDocs.isEmpty()) {
            return List.of();
        }
        final int numSubQueries = perSliceSubQueryTopDocs.getFirst().size();
        for (List<? extends TopDocs> subQueryTopDocs : perSliceSubQueryTopDocs) {
            if (subQueryTopDocs.size() != numSubQueries) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "slice collectors of one shard returned results for [%d] and [%d] sub-queries",
                        numSubQueries,
                        subQueryTopDocs.size()
                    )
                );
            }
        }
        final List<TopDocs> merged = new ArrayList<>(numSubQueries);
        for (int subQuery = 0; subQuery < numSubQueries; subQuery++) {
            final List<TopDocs> sliceTopDocs = new ArrayList<>(perSliceSubQueryTopDocs.size());
            for (List<? extends TopDocs> subQueryTopDocs : perSliceSubQueryTopDocs) {
                final TopDocs topDocs = subQueryTopDocs.get(subQuery);
                if (Objects.nonNull(topDocs) && Objects.nonNull(topDocs.scoreDocs)) {
                    sliceTopDocs.add(topDocs);
                }
            }
            // with no slice list left the merge returns empty top docs
            if (Objects.isNull(sort)) {
                merged.add(TopDocs.merge(0, numHits, sliceTopDocs.toArray(new TopDocs[0]), DOC_ID_ASC));
            } else {
                merged.add(TopDocs.merge(sort, 0, numHits, toTopFieldDocs(sliceTopDocs), DOC_ID_ASC));
            }
        }
        return merged;
    }

    /**
     * Merges every sub-query's default-mode collapse lists from all slices into its top numHits by the collapse sort, like
     * {@link #mergeSubQueryTopDocs} with a sort, and carries every kept hit's collapse value along. Default-mode collapse keeps
     * documents, not groups: grouping happens on the coordinator, so the merge must not dedupe by collapse value.
     * @param perSliceSubQueryTopDocs per slice, the sub-query collapse lists in sub-query order
     * @param numHits hits to keep per sub-query
     * @param collapseSort the sort the collapse collectors use
     * @param collapseField the collapse field
     * @return merged collapse list per sub-query, in sub-query order
     */
    static List<CollapseTopFieldDocs> mergeCollapseSubQueryTopDocs(
        final List<List<? extends TopDocs>> perSliceSubQueryTopDocs,
        final int numHits,
        final Sort collapseSort,
        final String collapseField
    ) {
        final List<TopDocs> merged = mergeSubQueryTopDocs(perSliceSubQueryTopDocs, numHits, collapseSort);
        final List<CollapseTopFieldDocs> collapseMerged = new ArrayList<>(merged.size());
        for (int subQuery = 0; subQuery < merged.size(); subQuery++) {
            // the merge keeps the slices' own hit objects, so each hit finds its collapse value by identity; null is a valid value
            final Map<ScoreDoc, Object> collapseValueByHit = new IdentityHashMap<>();
            for (List<? extends TopDocs> subQueryTopDocs : perSliceSubQueryTopDocs) {
                putCollapseValues(subQueryTopDocs.get(subQuery), collapseValueByHit);
            }
            final ScoreDoc[] hits = merged.get(subQuery).scoreDocs;
            final FieldDoc[] fieldDocs = new FieldDoc[hits.length];
            final Object[] collapseValues = new Object[hits.length];
            for (int hit = 0; hit < hits.length; hit++) {
                assert collapseValueByHit.containsKey(hits[hit]);
                fieldDocs[hit] = (FieldDoc) hits[hit];
                collapseValues[hit] = collapseValueByHit.get(hits[hit]);
            }
            collapseMerged.add(
                new CollapseTopFieldDocs(collapseField, merged.get(subQuery).totalHits, fieldDocs, collapseSort.getSort(), collapseValues)
            );
        }
        return collapseMerged;
    }

    private static void putCollapseValues(final TopDocs topDocs, final Map<ScoreDoc, Object> collapseValueByHit) {
        if (Objects.isNull(topDocs) || Objects.isNull(topDocs.scoreDocs)) {
            return;
        }
        if (topDocs instanceof CollapseTopFieldDocs collapseTopFieldDocs) {
            if (collapseTopFieldDocs.collapseValues.length != collapseTopFieldDocs.scoreDocs.length) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "collapse results of a sub-query have [%d] hits and [%d] collapse values",
                        collapseTopFieldDocs.scoreDocs.length,
                        collapseTopFieldDocs.collapseValues.length
                    )
                );
            }
            for (int hit = 0; hit < collapseTopFieldDocs.scoreDocs.length; hit++) {
                collapseValueByHit.put(collapseTopFieldDocs.scoreDocs[hit], collapseTopFieldDocs.collapseValues[hit]);
            }
        } else {
            throw new IllegalStateException("slice collectors of a collapsed hybrid query must return collapse results per sub-query");
        }
    }

    private static TopFieldDocs[] toTopFieldDocs(final List<TopDocs> sliceTopDocs) {
        final TopFieldDocs[] topFieldDocs = new TopFieldDocs[sliceTopDocs.size()];
        for (int slice = 0; slice < topFieldDocs.length; slice++) {
            if (sliceTopDocs.get(slice) instanceof TopFieldDocs sliceTopFieldDocs) {
                topFieldDocs[slice] = sliceTopFieldDocs;
            } else {
                throw new IllegalStateException("slice collectors of a sorted hybrid query must return sorted results per sub-query");
            }
        }
        return topFieldDocs;
    }
}
