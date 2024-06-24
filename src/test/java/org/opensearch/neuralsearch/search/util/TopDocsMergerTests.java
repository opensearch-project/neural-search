/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.util;

import lombok.SneakyThrows;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createStartStopElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createDelimiterElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.MAGIC_NUMBER_START_STOP;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.MAGIC_NUMBER_DELIMITER;

public class TopDocsMergerTests extends OpenSearchQueryTestCase {

    private static final float DELTA_FOR_ASSERTION = 0.001f;

    @SneakyThrows
    public void testMergeScoreDocs_whenBothTopDocsHasHits_thenSuccessful() {
        ScoreDocsMerger<ScoreDoc> scoreDocsMerger = new ScoreDocsMerger<>();
        TopDocsMerger topDocsMerger = new TopDocsMerger(scoreDocsMerger);

        TopDocs topDocsOriginal = new TopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),

            new ScoreDoc[] {
                createStartStopElementForHybridSearchResults(0),
                createDelimiterElementForHybridSearchResults(0),
                new ScoreDoc(0, 0.5f),
                new ScoreDoc(2, 0.3f),
                createDelimiterElementForHybridSearchResults(0),
                createStartStopElementForHybridSearchResults(0) }

        );
        TopDocsAndMaxScore topDocsAndMaxScoreOriginal = new TopDocsAndMaxScore(topDocsOriginal, 0.5f);
        TopDocs topDocsNew = new TopDocs(
            new TotalHits(4, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),

            new ScoreDoc[] {
                createStartStopElementForHybridSearchResults(2),
                createDelimiterElementForHybridSearchResults(2),
                new ScoreDoc(1, 0.7f),
                new ScoreDoc(4, 0.3f),
                new ScoreDoc(5, 0.05f),
                createDelimiterElementForHybridSearchResults(2),
                new ScoreDoc(4, 0.6f),
                createStartStopElementForHybridSearchResults(2) }
        );
        TopDocsAndMaxScore topDocsAndMaxScoreNew = new TopDocsAndMaxScore(topDocsNew, 0.7f);
        TopDocsAndMaxScore mergedTopDocsAndMaxScore = topDocsMerger.merge(topDocsAndMaxScoreOriginal, topDocsAndMaxScoreNew);

        assertNotNull(mergedTopDocsAndMaxScore);

        assertEquals(0.7f, mergedTopDocsAndMaxScore.maxScore, DELTA_FOR_ASSERTION);
        assertEquals(6, mergedTopDocsAndMaxScore.topDocs.totalHits.value);
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, mergedTopDocsAndMaxScore.topDocs.totalHits.relation);
        // expected number of rows is 5 from sub-query1 and 1 from sub-query2, plus 2 start-stop elements + 2 delimiters
        // 5 + 1 + 2 + 2 = 10
        assertEquals(10, mergedTopDocsAndMaxScore.topDocs.scoreDocs.length);
        // check format, all elements one by one
        ScoreDoc[] scoreDocs = mergedTopDocsAndMaxScore.topDocs.scoreDocs;
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[0].score, 0);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[1].score, 0);
        assertScoreDoc(scoreDocs[2], 1, 0.7f);
        assertScoreDoc(scoreDocs[3], 0, 0.5f);
        assertScoreDoc(scoreDocs[4], 2, 0.3f);
        assertScoreDoc(scoreDocs[5], 4, 0.3f);
        assertScoreDoc(scoreDocs[6], 5, 0.05f);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[7].score, 0);
        assertScoreDoc(scoreDocs[8], 4, 0.6f);
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[9].score, 0);
    }

    @SneakyThrows
    public void testMergeScoreDocs_whenOneTopDocsHasHitsAndOtherIsEmpty_thenSuccessful() {
        ScoreDocsMerger<ScoreDoc> scoreDocsMerger = new ScoreDocsMerger<>();
        TopDocsMerger topDocsMerger = new TopDocsMerger(scoreDocsMerger);

        TopDocs topDocsOriginal = new TopDocs(
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),

            new ScoreDoc[] {
                createStartStopElementForHybridSearchResults(0),
                createDelimiterElementForHybridSearchResults(0),
                createDelimiterElementForHybridSearchResults(0),
                createStartStopElementForHybridSearchResults(0) }

        );
        TopDocsAndMaxScore topDocsAndMaxScoreOriginal = new TopDocsAndMaxScore(topDocsOriginal, 0.5f);
        TopDocs topDocsNew = new TopDocs(
            new TotalHits(4, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),

            new ScoreDoc[] {
                createStartStopElementForHybridSearchResults(2),
                createDelimiterElementForHybridSearchResults(2),
                new ScoreDoc(1, 0.7f),
                new ScoreDoc(4, 0.3f),
                new ScoreDoc(5, 0.05f),
                createDelimiterElementForHybridSearchResults(2),
                new ScoreDoc(4, 0.6f),
                createStartStopElementForHybridSearchResults(2) }
        );
        TopDocsAndMaxScore topDocsAndMaxScoreNew = new TopDocsAndMaxScore(topDocsNew, 0.7f);
        TopDocsAndMaxScore mergedTopDocsAndMaxScore = topDocsMerger.merge(topDocsAndMaxScoreOriginal, topDocsAndMaxScoreNew);

        assertNotNull(mergedTopDocsAndMaxScore);

        assertEquals(0.7f, mergedTopDocsAndMaxScore.maxScore, DELTA_FOR_ASSERTION);
        assertEquals(4, mergedTopDocsAndMaxScore.topDocs.totalHits.value);
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, mergedTopDocsAndMaxScore.topDocs.totalHits.relation);
        // expected number of rows is 3 from sub-query1 and 1 from sub-query2, plus 2 start-stop elements + 2 delimiters
        // 3 + 1 + 2 + 2 = 8
        assertEquals(8, mergedTopDocsAndMaxScore.topDocs.scoreDocs.length);
        // check format, all elements one by one
        ScoreDoc[] scoreDocs = mergedTopDocsAndMaxScore.topDocs.scoreDocs;
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[0].score, 0);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[1].score, 0);
        assertScoreDoc(scoreDocs[2], 1, 0.7f);
        assertScoreDoc(scoreDocs[3], 4, 0.3f);
        assertScoreDoc(scoreDocs[4], 5, 0.05f);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[5].score, 0);
        assertScoreDoc(scoreDocs[6], 4, 0.6f);
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[7].score, 0);
    }

    @SneakyThrows
    public void testMergeScoreDocs_whenBothTopDocsHasNoHits_thenSuccessful() {
        ScoreDocsMerger<ScoreDoc> scoreDocsMerger = new ScoreDocsMerger<>();
        TopDocsMerger topDocsMerger = new TopDocsMerger(scoreDocsMerger);

        TopDocs topDocsOriginal = new TopDocs(
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),

            new ScoreDoc[] {
                createStartStopElementForHybridSearchResults(0),
                createDelimiterElementForHybridSearchResults(0),
                createDelimiterElementForHybridSearchResults(0),
                createStartStopElementForHybridSearchResults(0) }

        );
        TopDocsAndMaxScore topDocsAndMaxScoreOriginal = new TopDocsAndMaxScore(topDocsOriginal, 0);
        TopDocs topDocsNew = new TopDocs(
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),

            new ScoreDoc[] {
                createStartStopElementForHybridSearchResults(2),
                createDelimiterElementForHybridSearchResults(2),
                createDelimiterElementForHybridSearchResults(2),
                createStartStopElementForHybridSearchResults(2) }
        );
        TopDocsAndMaxScore topDocsAndMaxScoreNew = new TopDocsAndMaxScore(topDocsNew, 0);
        TopDocsAndMaxScore mergedTopDocsAndMaxScore = topDocsMerger.merge(topDocsAndMaxScoreOriginal, topDocsAndMaxScoreNew);

        assertNotNull(mergedTopDocsAndMaxScore);

        assertEquals(0f, mergedTopDocsAndMaxScore.maxScore, DELTA_FOR_ASSERTION);
        assertEquals(0, mergedTopDocsAndMaxScore.topDocs.totalHits.value);
        assertEquals(TotalHits.Relation.EQUAL_TO, mergedTopDocsAndMaxScore.topDocs.totalHits.relation);
        assertEquals(4, mergedTopDocsAndMaxScore.topDocs.scoreDocs.length);
        // check format, all elements one by one
        ScoreDoc[] scoreDocs = mergedTopDocsAndMaxScore.topDocs.scoreDocs;
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[0].score, 0);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[1].score, 0);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[2].score, 0);
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[3].score, 0);
    }

    @SneakyThrows
    public void testThreeSequentialMerges_whenAllTopDocsHasHits_thenSuccessful() {
        ScoreDocsMerger<ScoreDoc> scoreDocsMerger = new ScoreDocsMerger<>();
        TopDocsMerger topDocsMerger = new TopDocsMerger(scoreDocsMerger);

        TopDocs topDocsOriginal = new TopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),

            new ScoreDoc[] {
                createStartStopElementForHybridSearchResults(0),
                createDelimiterElementForHybridSearchResults(0),
                new ScoreDoc(0, 0.5f),
                new ScoreDoc(2, 0.3f),
                createDelimiterElementForHybridSearchResults(0),
                createStartStopElementForHybridSearchResults(0) }

        );
        TopDocsAndMaxScore topDocsAndMaxScoreOriginal = new TopDocsAndMaxScore(topDocsOriginal, 0.5f);
        TopDocs topDocsNew = new TopDocs(
            new TotalHits(4, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),

            new ScoreDoc[] {
                createStartStopElementForHybridSearchResults(2),
                createDelimiterElementForHybridSearchResults(2),
                new ScoreDoc(1, 0.7f),
                new ScoreDoc(4, 0.3f),
                new ScoreDoc(5, 0.05f),
                createDelimiterElementForHybridSearchResults(2),
                new ScoreDoc(4, 0.6f),
                createStartStopElementForHybridSearchResults(2) }
        );
        TopDocsAndMaxScore topDocsAndMaxScoreNew = new TopDocsAndMaxScore(topDocsNew, 0.7f);
        TopDocsAndMaxScore firstMergedTopDocsAndMaxScore = topDocsMerger.merge(topDocsAndMaxScoreOriginal, topDocsAndMaxScoreNew);

        assertNotNull(firstMergedTopDocsAndMaxScore);

        // merge results from collector 3
        TopDocs topDocsThirdCollector = new TopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),

            new ScoreDoc[] {
                createStartStopElementForHybridSearchResults(3),
                createDelimiterElementForHybridSearchResults(3),
                new ScoreDoc(3, 0.4f),
                createDelimiterElementForHybridSearchResults(3),
                new ScoreDoc(7, 0.85f),
                new ScoreDoc(9, 0.2f),
                createStartStopElementForHybridSearchResults(3) }
        );
        TopDocsAndMaxScore topDocsAndMaxScoreThirdCollector = new TopDocsAndMaxScore(topDocsThirdCollector, 0.85f);
        TopDocsAndMaxScore finalMergedTopDocsAndMaxScore = topDocsMerger.merge(
            firstMergedTopDocsAndMaxScore,
            topDocsAndMaxScoreThirdCollector
        );

        assertEquals(0.85f, finalMergedTopDocsAndMaxScore.maxScore, DELTA_FOR_ASSERTION);
        assertEquals(9, finalMergedTopDocsAndMaxScore.topDocs.totalHits.value);
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, finalMergedTopDocsAndMaxScore.topDocs.totalHits.relation);
        // expected number of rows is 6 from sub-query1 and 3 from sub-query2, plus 2 start-stop elements + 2 delimiters
        // 6 + 3 + 2 + 2 = 13
        assertEquals(13, finalMergedTopDocsAndMaxScore.topDocs.scoreDocs.length);
        // check format, all elements one by one
        ScoreDoc[] scoreDocs = finalMergedTopDocsAndMaxScore.topDocs.scoreDocs;
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[0].score, 0);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[1].score, 0);
        assertScoreDoc(scoreDocs[2], 1, 0.7f);
        assertScoreDoc(scoreDocs[3], 0, 0.5f);
        assertScoreDoc(scoreDocs[4], 3, 0.4f);
        assertScoreDoc(scoreDocs[5], 2, 0.3f);
        assertScoreDoc(scoreDocs[6], 4, 0.3f);
        assertScoreDoc(scoreDocs[7], 5, 0.05f);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[8].score, 0);
        assertScoreDoc(scoreDocs[9], 7, 0.85f);
        assertScoreDoc(scoreDocs[10], 4, 0.6f);
        assertScoreDoc(scoreDocs[11], 9, 0.2f);
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[12].score, 0);
    }

    private void assertScoreDoc(ScoreDoc scoreDoc, int expectedDocId, float expectedScore) {
        assertEquals(expectedDocId, scoreDoc.doc);
        assertEquals(expectedScore, scoreDoc.score, DELTA_FOR_ASSERTION);
    }
}
