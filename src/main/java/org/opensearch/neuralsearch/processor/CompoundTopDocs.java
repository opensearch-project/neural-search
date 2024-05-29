/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.opensearch.core.common.util.CollectionUtils;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQueryDelimiterElement;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQueryStartStopElement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;

/**
 * Class stores collection of TopDocs for each sub query from hybrid query. Collection of results is at shard level. We do store
 * list of TopDocs and list of ScoreDoc as well as total hits for the shard.
 */
@AllArgsConstructor
@Getter
@ToString(includeFieldNames = true)
@Log4j2
public class CompoundTopDocs {

    @Setter
    private TotalHits totalHits;
    private List<TopDocs> topDocs;
    @Setter
    private List<ScoreDoc> scoreDocs;

    public CompoundTopDocs(final TotalHits totalHits, final List<TopDocs> topDocs) {
        initialize(totalHits, topDocs);
    }

    private void initialize(TotalHits totalHits, List<TopDocs> topDocs) {
        this.totalHits = totalHits;
        if (!CollectionUtils.isEmpty(topDocs) && topDocs.get(0) instanceof TopFieldDocs) {
            this.topDocs = topDocs.stream().map(topDoc -> (TopFieldDocs) topDoc).collect(Collectors.toList());
        } else {
            this.topDocs = topDocs;
        }
        scoreDocs = cloneLargestScoreDocs(topDocs);
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
     * where doc_id is one of valid ids from result. For example, this is list with results for there sub-queries
     *
     *  0, 9549511920.4881596047
     *  0, 4422440593.9791198149
     *  0, 0.8
     *  2, 0.5
     *  0, 4422440593.9791198149
     *  0, 4422440593.9791198149
     *  2, 0.7
     *  5, 0.65
     *  6, 0.15
     *  0, 9549511920.4881596047
     */
    public CompoundTopDocs(final TopDocs topDocs) {
        boolean isSortApplied = false;
        if (topDocs instanceof TopFieldDocs) {
            isSortApplied = true;
        }
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        if (Objects.isNull(scoreDocs) || scoreDocs.length < 2) {
            initialize(topDocs.totalHits, new ArrayList<>());
            return;
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
                TopDocs subQueryTopDocs;
                if (isSortApplied) {
                    subQueryTopDocs = new TopFieldDocs(totalHits, subQueryScores, ((TopFieldDocs) topDocs).fields);
                } else {
                    subQueryTopDocs = new TopDocs(totalHits, subQueryScores);
                }
                topDocsList.add(subQueryTopDocs);
                scoreDocList.clear();
            } else {
                scoreDocList.add(scoreDoc);
            }
        }
        initialize(topDocs.totalHits, topDocsList);
    }

    private List<ScoreDoc> cloneLargestScoreDocs(final List<TopDocs> docs) {
        if (docs == null) {
            return null;
        }

        TopDocs doc1 = docs.stream().filter(Objects::nonNull).findFirst().orElse(null);
        if (doc1 instanceof TopFieldDocs) {
            FieldDoc[] maxScoreDocs = new FieldDoc[0];
            int maxLength = -1;
            for (TopDocs topDoc : docs) {
                if (topDoc == null || topDoc.scoreDocs == null) {
                    continue;
                }
                if (topDoc.scoreDocs.length > maxLength) {
                    maxLength = topDoc.scoreDocs.length;
                    maxScoreDocs = Arrays.copyOf(topDoc.scoreDocs, topDoc.scoreDocs.length, FieldDoc[].class);
                }
            }
            // do deep copy
            return Arrays.stream(maxScoreDocs)
                .map(doc -> new FieldDoc(doc.doc, doc.score, doc.fields, doc.shardIndex))
                .collect(Collectors.toList());
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
        return Arrays.stream(maxScoreDocs).map(doc -> new ScoreDoc(doc.doc, doc.score, doc.shardIndex)).collect(Collectors.toList());
    }
}
