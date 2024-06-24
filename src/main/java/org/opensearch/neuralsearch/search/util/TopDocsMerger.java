/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.util;

import com.google.common.annotations.VisibleForTesting;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;

import java.util.Comparator;
import java.util.Objects;

/**
 * Utility class for merging TopDocs and MaxScore across multiple search queries
 */
@RequiredArgsConstructor
public class TopDocsMerger {

    private final HybridQueryScoreDocsMerger<ScoreDoc> scoreDocsMerger;
    @VisibleForTesting
    protected static final Comparator<ScoreDoc> SCORE_DOC_BY_SCORE_COMPARATOR = Comparator.comparing((scoreDoc) -> scoreDoc.score);

    /**
     * Merge TopDocs and MaxScore from multiple search queries into a single TopDocsAndMaxScore object.
     * @param source TopDocsAndMaxScore for the original query
     * @param newTopDocs TopDocsAndMaxScore for the new query
     * @return merged TopDocsAndMaxScore object
     */
    public TopDocsAndMaxScore merge(TopDocsAndMaxScore source, TopDocsAndMaxScore newTopDocs) {
        if (Objects.isNull(newTopDocs) || Objects.isNull(newTopDocs.topDocs) || newTopDocs.topDocs.totalHits.value == 0) {
            return source;
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
        ScoreDoc[] mergedScoreDocs = scoreDocsMerger.merge(
            source.topDocs.scoreDocs,
            newTopDocs.topDocs.scoreDocs,
            SCORE_DOC_BY_SCORE_COMPARATOR
        );
        TotalHits mergedTotalHits = getMergedTotalHits(source, newTopDocs);
        TopDocsAndMaxScore result = new TopDocsAndMaxScore(
            new TopDocs(mergedTotalHits, mergedScoreDocs),
            Math.max(source.maxScore, newTopDocs.maxScore)
        );
        return result;
    }

    private TotalHits getMergedTotalHits(TopDocsAndMaxScore source, TopDocsAndMaxScore newTopDocs) {
        // merged value is a lower bound - if both are equal_to than merged will also be equal_to,
        // otherwise assign greater_than_or_equal
        TotalHits.Relation mergedHitsRelation = source.topDocs.totalHits.relation == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
            || newTopDocs.topDocs.totalHits.relation == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
                ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
                : TotalHits.Relation.EQUAL_TO;
        return new TotalHits(source.topDocs.totalHits.value + newTopDocs.topDocs.totalHits.value, mergedHitsRelation);
    }
}
