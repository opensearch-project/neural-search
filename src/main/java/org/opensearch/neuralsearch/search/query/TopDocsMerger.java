/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import com.google.common.annotations.VisibleForTesting;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;

import java.util.Comparator;
import java.util.Objects;
import org.opensearch.search.sort.SortAndFormats;

/**
 * Utility class for merging TopDocs and MaxScore across multiple search queries
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class TopDocsMerger {

    private HybridQueryScoreDocsMerger<ScoreDoc> scoreDocsMerger;
    private HybridQueryScoreDocsMerger<FieldDoc> fieldDocsMerger;
    private SortAndFormats sortAndFormats;
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
    TopDocsMerger(final SortAndFormats sortAndFormats) {
        this.sortAndFormats = sortAndFormats;
        if (this.sortAndFormats != null) {
            fieldDocsMerger = new HybridQueryScoreDocsMerger<>();
            FIELD_DOC_BY_SORT_CRITERIA_COMPARATOR = new HybridQueryFieldDocComparator(sortAndFormats.sort.getSort(), MERGING_TIE_BREAKER);
        } else {
            scoreDocsMerger = new HybridQueryScoreDocsMerger<>();
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
        if (Objects.isNull(newTopDocs) || Objects.isNull(newTopDocs.topDocs) || newTopDocs.topDocs.totalHits.value == 0) {
            return source;
        }
        TotalHits mergedTotalHits = getMergedTotalHits(source, newTopDocs);
        TopDocsAndMaxScore result = new TopDocsAndMaxScore(
            getTopDocs(getMergedScoreDocs(source.topDocs.scoreDocs, newTopDocs.topDocs.scoreDocs), mergedTotalHits),
            Math.max(source.maxScore, newTopDocs.maxScore)
        );
        return result;
    }

    private TotalHits getMergedTotalHits(final TopDocsAndMaxScore source, final TopDocsAndMaxScore newTopDocs) {
        // merged value is a lower bound - if both are equal_to than merged will also be equal_to,
        // otherwise assign greater_than_or_equal
        TotalHits.Relation mergedHitsRelation = source.topDocs.totalHits.relation == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
            || newTopDocs.topDocs.totalHits.relation == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
                ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
                : TotalHits.Relation.EQUAL_TO;
        return new TotalHits(source.topDocs.totalHits.value + newTopDocs.topDocs.totalHits.value, mergedHitsRelation);
    }

    private TopDocs getTopDocs(ScoreDoc[] mergedScoreDocs, TotalHits mergedTotalHits) {
        if (sortAndFormats != null) {
            return new TopFieldDocs(mergedTotalHits, mergedScoreDocs, sortAndFormats.sort.getSort());
        }
        return new TopDocs(mergedTotalHits, mergedScoreDocs);
    }

    private ScoreDoc[] getMergedScoreDocs(ScoreDoc[] source, ScoreDoc[] newScoreDocs) {
        if (sortAndFormats != null) {
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
            return fieldDocsMerger.merge((FieldDoc[]) source, (FieldDoc[]) newScoreDocs, FIELD_DOC_BY_SORT_CRITERIA_COMPARATOR, true);
        }
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
        return scoreDocsMerger.merge(source, newScoreDocs, SCORE_DOC_BY_SCORE_COMPARATOR, false);
    }
}
