/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query.util;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.search.collector.HybridSearchCollector;
import org.opensearch.neuralsearch.search.collector.HybridTopScoreDocCollector;
import org.opensearch.neuralsearch.search.query.HybridCollectorResultsUtilParams;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.MAGIC_NUMBER_DELIMITER;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.MAGIC_NUMBER_START_STOP;

public class HybridSearchCollectorResultUtilTests extends OpenSearchQueryTestCase {
    protected static final float DELTA_FOR_ASSERTION = 0.001f;

    public void testReduceCollectorResults_WhenTopDocsAreMerged_thenSuccessful() {
        ScoreDoc[] scoreDocs = new ScoreDoc[4];
        scoreDocs[0] = new ScoreDoc(0, 1.0f);
        scoreDocs[1] = new ScoreDoc(1, 0.7f);
        scoreDocs[2] = new ScoreDoc(2, 0.7f);
        scoreDocs[3] = new ScoreDoc(3, 0.3f);

        TotalHits totalHits = new TotalHits(4, TotalHits.Relation.EQUAL_TO);

        TopDocs topDocs = new TopDocs(totalHits, scoreDocs);
        TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(topDocs, 1.0f);

        HybridSearchCollector hybridSearchCollector = mock(HybridSearchCollector.class);
        SearchContext searchContext = mock(SearchContext.class);
        HybridSearchCollectorResultUtil hybridSearchCollectorResultUtil = new HybridSearchCollectorResultUtil(
            new HybridCollectorResultsUtilParams.Builder().searchContext(searchContext).build(),
            hybridSearchCollector
        );

        QuerySearchResult querySearchResult = new QuerySearchResult();
        hybridSearchCollectorResultUtil.reduceCollectorResults(querySearchResult, topDocsAndMaxScore);

        assertSame(querySearchResult.topDocs(), topDocsAndMaxScore);

        TopDocsAndMaxScore topDocsAndMaxScore1 = new TopDocsAndMaxScore(topDocs, 1.0f);

        hybridSearchCollectorResultUtil.reduceCollectorResults(querySearchResult, topDocsAndMaxScore1);

        assertEquals(8, querySearchResult.getTotalHits().value());
    }

    public void testGetTopDocsAndAndMaxScore() throws IOException {
        ScoreDoc[] scoreDocs = new ScoreDoc[4];

        scoreDocs[0] = new ScoreDoc(0, 0.7f);
        scoreDocs[1] = new ScoreDoc(1, 0.3f);
        scoreDocs[2] = new ScoreDoc(2, 0.2f);
        scoreDocs[3] = new ScoreDoc(3, 0.1f);

        TotalHits totalHits = new TotalHits(4, TotalHits.Relation.EQUAL_TO);

        TopDocs topDocs = new TopDocs(totalHits, scoreDocs);

        ScoreDoc[] scoreDocs1 = new ScoreDoc[4];

        scoreDocs1[0] = new ScoreDoc(0, 0.7f);
        scoreDocs1[1] = new ScoreDoc(1, 0.3f);
        scoreDocs1[2] = new ScoreDoc(2, 0.2f);
        scoreDocs1[3] = new ScoreDoc(3, 0.1f);

        TotalHits totalHits1 = new TotalHits(4, TotalHits.Relation.EQUAL_TO);

        TopDocs topDocs1 = new TopDocs(totalHits1, scoreDocs1);

        List<TopDocs> topDocsList = new ArrayList<>();
        topDocsList.add(topDocs);
        topDocsList.add(topDocs1);

        SearchContext searchContext = mock(SearchContext.class);

        HybridTopScoreDocCollector hybridTopScoreDocCollector = mock(HybridTopScoreDocCollector.class);
        HybridSearchCollectorResultUtil hybridSearchCollectorResultUtil = new HybridSearchCollectorResultUtil(
            new HybridCollectorResultsUtilParams.Builder().searchContext(searchContext).build(),
            hybridTopScoreDocCollector
        );

        when(hybridTopScoreDocCollector.topDocs()).thenReturn(topDocsList);
        when(hybridTopScoreDocCollector.getTotalHits()).thenReturn(4);
        when(hybridTopScoreDocCollector.getMaxScore()).thenReturn(0.7f);

        TopDocsAndMaxScore topDocsAndMaxScore = hybridSearchCollectorResultUtil.getTopDocsAndAndMaxScore();
        assertEquals(4, topDocsAndMaxScore.topDocs.totalHits.value());
        assertEquals(0.7f, topDocsAndMaxScore.maxScore, DELTA_FOR_ASSERTION);
        ScoreDoc[] updatedScoreDocs = topDocsAndMaxScore.topDocs.scoreDocs;
        assertEquals(MAGIC_NUMBER_START_STOP, updatedScoreDocs[0].score, DELTA_FOR_ASSERTION);
        assertEquals(MAGIC_NUMBER_DELIMITER, updatedScoreDocs[1].score, DELTA_FOR_ASSERTION);
        assertEquals(MAGIC_NUMBER_DELIMITER, updatedScoreDocs[6].score, DELTA_FOR_ASSERTION);
        assertEquals(MAGIC_NUMBER_START_STOP, updatedScoreDocs[11].score, DELTA_FOR_ASSERTION);
    }
}
