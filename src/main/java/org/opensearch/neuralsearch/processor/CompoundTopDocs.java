/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import static org.opensearch.neuralsearch.processor.NormalizationProcessor.isHybridQueryDelimiterElement;
import static org.opensearch.neuralsearch.processor.NormalizationProcessor.isHybridQueryStartStopElement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

/**
 * Class stores collection of TodDocs for each sub query from hybrid query
 */
@ToString(includeFieldNames = true)
@AllArgsConstructor
@Log4j2
public class CompoundTopDocs {

    @Getter
    @Setter
    private TotalHits totalHits;
    @Getter
    private final List<TopDocs> compoundTopDocs;
    @Getter
    @Setter
    private ScoreDoc[] scoreDocs;

    public CompoundTopDocs(final TotalHits totalHits, final List<TopDocs> compoundTopDocs) {
        this.totalHits = totalHits;
        this.compoundTopDocs = compoundTopDocs;
        scoreDocs = cloneLargestScoreDocs(compoundTopDocs);
    }

    private static ScoreDoc[] cloneLargestScoreDocs(List<TopDocs> docs) {
        if (docs == null) {
            return null;
        }
        ScoreDoc[] maxScoreDocs = new ScoreDoc[0];
        int maxLength = -1;
        for (TopDocs topDoc : docs) {
            if (topDoc == null || topDoc.scoreDocs == null) {
                continue;
            }
            if (topDoc.scoreDocs.length > maxLength) {
                maxLength = topDoc.scoreDocs.length;
                maxScoreDocs = topDoc.scoreDocs;
            }
        }
        // do deep copy
        return Arrays.stream(maxScoreDocs).map(doc -> new ScoreDoc(doc.doc, doc.score, doc.shardIndex)).toArray(ScoreDoc[]::new);
    }

    /**
     * Create new instance from TopDocs by parsing scores of sub-queries. Final format looks like:
     *  doc_id | magic_number_1
     *  doc_id | magic_number_2
     *  ...
     *  doc_id | magic_number_2
     *  ...
     *  doc_id | magic_number_2
     *  ...
     *  doc_id | magic_number_1
     *
     * where doc_id is one of valid ids from result
     *
     * @param topDocs object with scores from multiple sub-queries
     * @return compound TopDocs object that has results from all sub-queries
     */
    public static CompoundTopDocs create(final TopDocs topDocs) {
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        if (Objects.isNull(scoreDocs) || scoreDocs.length < 2) {
            return new CompoundTopDocs(topDocs.totalHits, new ArrayList<>());
        }
        // skipping first two elements, it's a start-stop element and delimiter for first series
        List<TopDocs> topDocsList = new ArrayList<>();
        List<ScoreDoc> scoreDocList = new ArrayList<>();
        for (int index = 2; index < scoreDocs.length; index++) {
            // getting first element of score's series
            ScoreDoc scoreDoc = scoreDocs[index];
            if (isHybridQueryDelimiterElement(scoreDoc) || isHybridQueryStartStopElement(scoreDoc)) {
                ScoreDoc[] subQueryScores = scoreDocList.toArray(new ScoreDoc[0]);
                TotalHits totalHits = new TotalHits(subQueryScores.length, TotalHits.Relation.EQUAL_TO);
                TopDocs subQueryTopDocs = new TopDocs(totalHits, subQueryScores);
                topDocsList.add(subQueryTopDocs);
                scoreDocList.clear();
            } else {
                scoreDocList.add(scoreDoc);
            }
        }
        return new CompoundTopDocs(topDocs.totalHits, topDocsList);
    }
}
