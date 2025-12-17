/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import com.google.common.annotations.VisibleForTesting;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;

import java.util.Comparator;
import java.util.Objects;

import org.opensearch.search.collapse.CollapseContext;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.neuralsearch.search.query.HybridQueryScoreDocsMerger.MergeResult;

/**
 * Utility class for merging TopDocs and MaxScore across multiple search queries
 */
@RequiredArgsConstructor
public class TopDocsMerger {
    private HybridQueryScoreDocsMerger docsMerger;
    private SortAndFormats sortAndFormats;
    private CollapseContext collapseContext;
    @VisibleForTesting
    protected static Comparator<ScoreDoc> SCORE_DOC_BY_SCORE_COMPARATOR;
    @VisibleForTesting
    protected static HybridQueryFieldDocComparator FIELD_DOC_BY_SORT_CRITERIA_COMPARATOR;
    private final Comparator<ScoreDoc> MERGING_TIE_BREAKER = (o1, o2) -> {
        int docIdComparison = Integer.compare(o1.doc, o2.doc);
        return docIdComparison;
    };

    /**
     * Uses hybrid query score docs merger to merge internal score docs
     */
    TopDocsMerger(final SortAndFormats sortAndFormats, final CollapseContext collapseContext) {
        this.sortAndFormats = sortAndFormats;
        this.collapseContext = collapseContext;
        if (isSortingEnabled()) {
            docsMerger = new HybridQueryScoreDocsMerger<FieldDoc>();
            FIELD_DOC_BY_SORT_CRITERIA_COMPARATOR = new HybridQueryFieldDocComparator(sortAndFormats.sort.getSort(), MERGING_TIE_BREAKER);
        } else {
            docsMerger = new HybridQueryScoreDocsMerger<>();
            SCORE_DOC_BY_SCORE_COMPARATOR = Comparator.comparing((scoreDoc) -> scoreDoc.score);
        }
    }

    /**
     * Merge TopDocs and MaxScore from multiple search queries into a single TopDocsAndMaxScore object.
     * @param source TopDocsAndMaxScore for the original query
     * @param newTopDocs TopDocsAndMaxScore for the new query
     * @return merged TopDocsAndMaxScore object
     */
    public TopDocsAndMaxScore merge(final TopDocsAndMaxScore source, final TopDocsAndMaxScore newTopDocs) {
        // we need to check if any of source and destination top docs are empty. This is needed for case when concurrent segment search
        // is enabled. In such case search is done by multiple workers, and results are saved in multiple doc collectors. Any on those
        // results can be empty, in such case we can skip actual merge logic and just return result object.
        if (isEmpty(newTopDocs)) {
            return source;
        }
        if (isEmpty(source)) {
            return newTopDocs;
        }
        TotalHits mergedTotalHits = getMergedTotalHits(source, newTopDocs);

        MergeResult mergeResult;
        ScoreDoc[] sourceScoreDocs = source.topDocs.scoreDocs;
        ScoreDoc[] newScoreDocs = newTopDocs.topDocs.scoreDocs;

        if (isCollapseEnabled()) {
            if (!(source.topDocs instanceof CollapseTopFieldDocs sourceCollapseTopFieldDocs)) {
                throw new IllegalStateException("Collapse enabled but source TopDocs is not an instance of CollapseTopFieldDocs");
            }
            if (!(newTopDocs.topDocs instanceof CollapseTopFieldDocs newCollapseTopFieldDocs)) {
                throw new IllegalStateException("Collapse enabled but new TopDocs is not an instance of CollapseTopFieldDocs");
            }
            mergeResult = getMergedScoreDocs(
                sourceScoreDocs,
                newScoreDocs,
                sourceCollapseTopFieldDocs.collapseValues,
                newCollapseTopFieldDocs.collapseValues
            );
        } else {
            mergeResult = getMergedScoreDocs(sourceScoreDocs, newScoreDocs, null, null);
        }
        return new TopDocsAndMaxScore(getTopDocs(mergeResult, mergedTotalHits), Math.max(source.maxScore, newTopDocs.maxScore));
    }

    /**
     * Checks if TopDocsAndMaxScore is null, has no top docs or zero total hits
     * @param topDocsAndMaxScore
     * @return
     */
    private static boolean isEmpty(final TopDocsAndMaxScore topDocsAndMaxScore) {
        if (Objects.isNull(topDocsAndMaxScore)
            || Objects.isNull(topDocsAndMaxScore.topDocs)
            || topDocsAndMaxScore.topDocs.totalHits.value() == 0) {
            return true;
        }
        return false;
    }

    private TotalHits getMergedTotalHits(final TopDocsAndMaxScore source, final TopDocsAndMaxScore newTopDocs) {
        // merged value is a lower bound - if both are equal_to than merged will also be equal_to,
        // otherwise assign greater_than_or_equal
        TotalHits.Relation mergedHitsRelation = source.topDocs.totalHits.relation() == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
            || newTopDocs.topDocs.totalHits.relation() == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
                ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
                : TotalHits.Relation.EQUAL_TO;
        return new TotalHits(source.topDocs.totalHits.value() + newTopDocs.topDocs.totalHits.value(), mergedHitsRelation);
    }

    private TopDocs getTopDocs(MergeResult mergeResult, TotalHits mergedTotalHits) {
        if (isCollapseEnabled()) {
            return new CollapseTopFieldDocs(
                collapseContext.getFieldName(),
                mergedTotalHits,
                mergeResult.scoreDocs(),
                getSortFieldsWhenCollapseEnabled(),
                mergeResult.collapseValues()
            );
        }

        if (isSortingEnabled()) {
            return new TopFieldDocs(mergedTotalHits, mergeResult.scoreDocs(), sortAndFormats.sort.getSort());
        }
        return new TopDocs(mergedTotalHits, mergeResult.scoreDocs());
    }

    private MergeResult getMergedScoreDocs(
        ScoreDoc[] source,
        ScoreDoc[] newScoreDocs,
        Object[] sourceCollapseValues,
        Object[] newCollapseValues
    ) {
        // Case 1 when sorting is enabled then below will be the TopDocs format
        // we need to merge hits per individual sub-query
        // format of results in both new and source TopDocs is following
        // doc_id | magic_number_1 | [1]
        // doc_id | magic_number_2 | [1]
        // ...
        // doc_id | magic_number_2 | [1]
        // ...
        // doc_id | magic_number_2 | [1]
        // ...
        // doc_id | magic_number_1 | [1]

        // Case 2 when sorting is disabled then below will be the TopDocs format
        // we need to merge hits per individual sub-query
        // format of results in both new and source TopDocs is following
        // doc_id | magic_number_1
        // doc_id | magic_number_2
        // ...
        // doc_id | magic_number_2
        // ...
        // doc_id | magic_number_2
        // ...
        // doc_id | magic_number_1
        return docsMerger.mergeScoreDocsAndCollapseValues(
            source,
            newScoreDocs,
            comparator(),
            sourceCollapseValues,
            newCollapseValues,
            isSortingEnabled(),
            isCollapseEnabled()
        );
    }

    private Comparator<? extends ScoreDoc> comparator() {
        return sortAndFormats != null ? FIELD_DOC_BY_SORT_CRITERIA_COMPARATOR : SCORE_DOC_BY_SCORE_COMPARATOR;
    }

    private boolean isSortingEnabled() {
        return sortAndFormats != null;
    }

    private boolean isCollapseEnabled() {
        return collapseContext != null;
    }

    private SortField[] getSortFieldsWhenCollapseEnabled() {
        return sortAndFormats != null ? sortAndFormats.sort.getSort() : new SortField[] { new SortField(null, SortField.Type.SCORE) };
    }
}
