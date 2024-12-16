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
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQueryDelimiterElement;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQueryStartStopElement;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.query.QuerySearchResult;

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
    @Getter
    private SearchShard searchShard;

    public CompoundTopDocs(
        final TotalHits totalHits,
        final List<TopDocs> topDocs,
        final boolean isSortEnabled,
        final SearchShard searchShard
    ) {
        initialize(totalHits, topDocs, isSortEnabled, searchShard);
    }

    private void initialize(TotalHits totalHits, List<TopDocs> topDocs, boolean isSortEnabled, SearchShard searchShard) {
        this.totalHits = totalHits;
        this.topDocs = topDocs;
        scoreDocs = cloneLargestScoreDocs(topDocs, isSortEnabled);
        this.searchShard = searchShard;
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
    public CompoundTopDocs(final QuerySearchResult querySearchResult) {
        final TopDocs topDocs = querySearchResult.topDocs().topDocs;
        final SearchShardTarget searchShardTarget = querySearchResult.getSearchShardTarget();
        SearchShard searchShard = SearchShard.createSearchShard(searchShardTarget);
        boolean isSortEnabled = false;
        if (topDocs instanceof TopFieldDocs) {
            isSortEnabled = true;
        }
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        if (Objects.isNull(scoreDocs) || scoreDocs.length < 2) {
            initialize(topDocs.totalHits, new ArrayList<>(), isSortEnabled, searchShard);
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
                if (isSortEnabled) {
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
        initialize(topDocs.totalHits, topDocsList, isSortEnabled, searchShard);
    }

    private List<ScoreDoc> cloneLargestScoreDocs(final List<TopDocs> docs, boolean isSortEnabled) {
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
        List<ScoreDoc> scoreDocs = new ArrayList<>();
        for (ScoreDoc scoreDoc : maxScoreDocs) {
            scoreDocs.add(deepCopyScoreDoc(scoreDoc, isSortEnabled));
        }
        return scoreDocs;
    }

    private ScoreDoc deepCopyScoreDoc(final ScoreDoc scoreDoc, final boolean isSortEnabled) {
        if (!isSortEnabled) {
            return new ScoreDoc(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex);
        }
        FieldDoc fieldDoc = (FieldDoc) scoreDoc;
        return new FieldDoc(fieldDoc.doc, fieldDoc.score, fieldDoc.fields, fieldDoc.shardIndex);
    }
}
