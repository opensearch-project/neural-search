/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import lombok.SneakyThrows;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createStartStopElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createDelimiterElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.MAGIC_NUMBER_START_STOP;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.MAGIC_NUMBER_DELIMITER;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createFieldDocStartStopElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createFieldDocDelimiterElementForHybridSearchResults;

import org.opensearch.search.DocValueFormat;
import org.opensearch.search.collapse.CollapseContext;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.search.sort.SortBuilders;

import java.util.ArrayList;
import java.util.List;

public class TopDocsMergerTests extends OpenSearchQueryTestCase {

    @SneakyThrows
    public void testMergeScoreDocs_whenBothTopDocsHasHits_thenSuccessful() {
        TopDocsMerger topDocsMerger = new TopDocsMerger(null, null);

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
        assertEquals(6, mergedTopDocsAndMaxScore.topDocs.totalHits.value());
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, mergedTopDocsAndMaxScore.topDocs.totalHits.relation());
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
        TopDocsMerger topDocsMerger = new TopDocsMerger(null, null);

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
        assertEquals(4, mergedTopDocsAndMaxScore.topDocs.totalHits.value());
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, mergedTopDocsAndMaxScore.topDocs.totalHits.relation());
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
        TopDocsMerger topDocsMerger = new TopDocsMerger(null, null);

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
        assertEquals(0, mergedTopDocsAndMaxScore.topDocs.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, mergedTopDocsAndMaxScore.topDocs.totalHits.relation());
        assertEquals(4, mergedTopDocsAndMaxScore.topDocs.scoreDocs.length);
        // check format, all elements one by one
        ScoreDoc[] scoreDocs = mergedTopDocsAndMaxScore.topDocs.scoreDocs;
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[0].score, 0);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[1].score, 0);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[2].score, 0);
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[3].score, 0);
    }

    @SneakyThrows
    public void testMergeScoreDocs_whenSomeSegmentsHasNoHits_thenSuccessful() {
        // Given
        TopDocsMerger topDocsMerger = new TopDocsMerger(null, null);

        // When
        // first segment has no results, and we merge with non-empty segment
        TopDocs topDocsOriginal = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] {});
        TopDocsAndMaxScore topDocsAndMaxScoreOriginal = new TopDocsAndMaxScore(topDocsOriginal, 0);
        TopDocs topDocsNew = new TopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
                createStartStopElementForHybridSearchResults(0),
                createDelimiterElementForHybridSearchResults(0),
                new ScoreDoc(0, 0.5f),
                new ScoreDoc(2, 0.3f),
                createStartStopElementForHybridSearchResults(0) }
        );
        TopDocsAndMaxScore topDocsAndMaxScoreNew = new TopDocsAndMaxScore(topDocsNew, 0.5f);
        TopDocsAndMaxScore mergedTopDocsAndMaxScore = topDocsMerger.merge(topDocsAndMaxScoreOriginal, topDocsAndMaxScoreNew);

        // Then
        assertNotNull(mergedTopDocsAndMaxScore);

        assertEquals(0.5f, mergedTopDocsAndMaxScore.maxScore, DELTA_FOR_ASSERTION);
        assertEquals(2, mergedTopDocsAndMaxScore.topDocs.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, mergedTopDocsAndMaxScore.topDocs.totalHits.relation());
        assertEquals(5, mergedTopDocsAndMaxScore.topDocs.scoreDocs.length);
        // check format, all elements one by one
        ScoreDoc[] scoreDocs = mergedTopDocsAndMaxScore.topDocs.scoreDocs;
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[0].score, 0);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[1].score, 0);
        assertScoreDoc(scoreDocs[2], 0, 0.5f);
        assertScoreDoc(scoreDocs[3], 2, 0.3f);
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[4].score, 0);

        // When
        // source object has results, and we merge with empty segment
        TopDocs topDocsNewEmpty = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] {});
        TopDocsAndMaxScore topDocsAndMaxScoreNewEmpty = new TopDocsAndMaxScore(topDocsNewEmpty, 0);
        TopDocsAndMaxScore finalMergedTopDocsAndMaxScore = topDocsMerger.merge(mergedTopDocsAndMaxScore, topDocsAndMaxScoreNewEmpty);

        // Then
        // merged object remains unchanged
        assertNotNull(finalMergedTopDocsAndMaxScore);

        assertEquals(0.5f, finalMergedTopDocsAndMaxScore.maxScore, DELTA_FOR_ASSERTION);
        assertEquals(2, finalMergedTopDocsAndMaxScore.topDocs.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, finalMergedTopDocsAndMaxScore.topDocs.totalHits.relation());
        assertEquals(5, finalMergedTopDocsAndMaxScore.topDocs.scoreDocs.length);
        // check format, all elements one by one
        ScoreDoc[] finalScoreDocs = finalMergedTopDocsAndMaxScore.topDocs.scoreDocs;
        assertEquals(MAGIC_NUMBER_START_STOP, finalScoreDocs[0].score, 0);
        assertEquals(MAGIC_NUMBER_DELIMITER, finalScoreDocs[1].score, 0);
        assertScoreDoc(finalScoreDocs[2], 0, 0.5f);
        assertScoreDoc(finalScoreDocs[3], 2, 0.3f);
        assertEquals(MAGIC_NUMBER_START_STOP, finalScoreDocs[4].score, 0);
    }

    @SneakyThrows
    public void testThreeSequentialMerges_whenAllTopDocsHasHits_thenSuccessful() {
        TopDocsMerger topDocsMerger = new TopDocsMerger(null, null);

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
        assertEquals(9, finalMergedTopDocsAndMaxScore.topDocs.totalHits.value());
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, finalMergedTopDocsAndMaxScore.topDocs.totalHits.relation());
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

    @SneakyThrows
    public void testMergeFieldDocs_whenBothTopDocsHasHits_thenSuccessful() {
        DocValueFormat docValueFormat[] = new DocValueFormat[] { DocValueFormat.RAW };
        SortField sortField = new SortField("stock", SortField.Type.INT, true);
        Sort sort = new Sort(sortField);
        SortAndFormats sortAndFormats = new SortAndFormats(sort, docValueFormat);
        TopDocsMerger topDocsMerger = new TopDocsMerger(sortAndFormats, null);

        TopDocs topDocsOriginal = new TopFieldDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),

            new FieldDoc[] {
                createFieldDocStartStopElementForHybridSearchResults(0, new Object[] { 1 }),
                createFieldDocDelimiterElementForHybridSearchResults(0, new Object[] { 1 }),
                new FieldDoc(0, 0.5f, new Object[] { 100 }),
                new FieldDoc(2, 0.3f, new Object[] { 80 }),
                createFieldDocDelimiterElementForHybridSearchResults(0, new Object[] { 1 }),
                createFieldDocStartStopElementForHybridSearchResults(0, new Object[] { 1 }) },
            sortAndFormats.sort.getSort()
        );
        TopDocsAndMaxScore topDocsAndMaxScoreOriginal = new TopDocsAndMaxScore(topDocsOriginal, 0.5f);
        TopDocs topDocsNew = new TopFieldDocs(
            new TotalHits(4, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),

            new FieldDoc[] {
                createFieldDocStartStopElementForHybridSearchResults(1, new Object[] { 1 }),
                createFieldDocDelimiterElementForHybridSearchResults(1, new Object[] { 1 }),
                new FieldDoc(1, 0.7f, new Object[] { 70 }),
                new FieldDoc(4, 0.3f, new Object[] { 60 }),
                new FieldDoc(5, 0.05f, new Object[] { 30 }),
                createFieldDocDelimiterElementForHybridSearchResults(1, new Object[] { 1 }),
                new FieldDoc(4, 0.6f, new Object[] { 40 }),
                createFieldDocStartStopElementForHybridSearchResults(1, new Object[] { 1 }) },
            sortAndFormats.sort.getSort()
        );
        TopDocsAndMaxScore topDocsAndMaxScoreNew = new TopDocsAndMaxScore(topDocsNew, 0.7f);
        TopDocsAndMaxScore mergedTopDocsAndMaxScore = topDocsMerger.merge(topDocsAndMaxScoreOriginal, topDocsAndMaxScoreNew);

        assertNotNull(mergedTopDocsAndMaxScore);

        assertEquals(0.7f, mergedTopDocsAndMaxScore.maxScore, DELTA_FOR_ASSERTION);
        assertEquals(6, mergedTopDocsAndMaxScore.topDocs.totalHits.value());
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, mergedTopDocsAndMaxScore.topDocs.totalHits.relation());
        // expected number of rows is 5 from sub-query1 and 1 from sub-query2, plus 2 start-stop elements + 2 delimiters
        // 5 + 1 + 2 + 2 = 10
        assertEquals(10, mergedTopDocsAndMaxScore.topDocs.scoreDocs.length);
        // check format, all elements one by one
        FieldDoc[] fieldDocs = (FieldDoc[]) mergedTopDocsAndMaxScore.topDocs.scoreDocs;
        assertEquals(1, fieldDocs[0].fields[0]);
        assertEquals(1, fieldDocs[1].fields[0]);
        assertFieldDoc(fieldDocs[2], 0, 100);
        assertFieldDoc(fieldDocs[3], 2, 80);
        assertFieldDoc(fieldDocs[4], 1, 70);
        assertFieldDoc(fieldDocs[5], 4, 60);
        assertFieldDoc(fieldDocs[6], 5, 30);
        assertEquals(1, fieldDocs[7].fields[0]);
        assertFieldDoc(fieldDocs[8], 4, 40);
        assertEquals(1, fieldDocs[9].fields[0]);
    }

    @SneakyThrows
    public void testMergeFieldDocs_whenOneTopDocsHasHitsAndOtherIsEmpty_thenSuccessful() {
        DocValueFormat docValueFormat[] = new DocValueFormat[] { DocValueFormat.RAW };
        SortField sortField = new SortField("stock", SortField.Type.INT, true);
        Sort sort = new Sort(sortField);
        SortAndFormats sortAndFormats = new SortAndFormats(sort, docValueFormat);
        TopDocsMerger topDocsMerger = new TopDocsMerger(sortAndFormats, null);

        TopDocs topDocsOriginal = new TopFieldDocs(
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),

            new FieldDoc[] {
                createFieldDocStartStopElementForHybridSearchResults(0, new Object[] { 1 }),
                createFieldDocDelimiterElementForHybridSearchResults(0, new Object[] { 1 }),
                createFieldDocDelimiterElementForHybridSearchResults(0, new Object[] { 1 }),
                createFieldDocStartStopElementForHybridSearchResults(0, new Object[] { 1 }) },
            sortAndFormats.sort.getSort()

        );
        TopDocsAndMaxScore topDocsAndMaxScoreOriginal = new TopDocsAndMaxScore(topDocsOriginal, 0.5f);
        TopDocs topDocsNew = new TopFieldDocs(
            new TotalHits(4, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),

            new FieldDoc[] {
                createFieldDocStartStopElementForHybridSearchResults(2, new Object[] { 1 }),
                createFieldDocDelimiterElementForHybridSearchResults(2, new Object[] { 1 }),
                new FieldDoc(1, 0.7f, new Object[] { 100 }),
                new FieldDoc(4, 0.3f, new Object[] { 60 }),
                new FieldDoc(5, 0.05f, new Object[] { 30 }),
                createFieldDocDelimiterElementForHybridSearchResults(2, new Object[] { 1 }),
                new FieldDoc(4, 0.6f, new Object[] { 80 }),
                createFieldDocStartStopElementForHybridSearchResults(2, new Object[] { 1 }) },
            sortAndFormats.sort.getSort()
        );
        TopDocsAndMaxScore topDocsAndMaxScoreNew = new TopDocsAndMaxScore(topDocsNew, 0.7f);
        TopDocsAndMaxScore mergedTopDocsAndMaxScore = topDocsMerger.merge(topDocsAndMaxScoreOriginal, topDocsAndMaxScoreNew);

        assertNotNull(mergedTopDocsAndMaxScore);

        assertEquals(0.7f, mergedTopDocsAndMaxScore.maxScore, DELTA_FOR_ASSERTION);
        assertEquals(4, mergedTopDocsAndMaxScore.topDocs.totalHits.value());
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, mergedTopDocsAndMaxScore.topDocs.totalHits.relation());
        // expected number of rows is 3 from sub-query1 and 1 from sub-query2, plus 2 start-stop elements + 2 delimiters
        // 3 + 1 + 2 + 2 = 8
        assertEquals(8, mergedTopDocsAndMaxScore.topDocs.scoreDocs.length);
        // check format, all elements one by one
        FieldDoc[] fieldDocs = (FieldDoc[]) mergedTopDocsAndMaxScore.topDocs.scoreDocs;
        assertEquals(1, fieldDocs[0].fields[0]);
        assertEquals(1, fieldDocs[1].fields[0]);
        assertFieldDoc(fieldDocs[2], 1, 100);
        assertFieldDoc(fieldDocs[3], 4, 60);
        assertFieldDoc(fieldDocs[4], 5, 30);
        assertEquals(1, fieldDocs[5].fields[0]);
        assertFieldDoc(fieldDocs[6], 4, 80);
        assertEquals(1, fieldDocs[7].fields[0]);
    }

    @SneakyThrows
    public void testMergeFieldDocs_whenBothTopDocsHasNoHits_thenSuccessful() {
        DocValueFormat docValueFormat[] = new DocValueFormat[] { DocValueFormat.RAW };
        SortField sortField = new SortField("stock", SortField.Type.INT, true);
        Sort sort = new Sort(sortField);
        SortAndFormats sortAndFormats = new SortAndFormats(sort, docValueFormat);
        TopDocsMerger topDocsMerger = new TopDocsMerger(sortAndFormats, null);

        TopDocs topDocsOriginal = new TopFieldDocs(
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),

            new FieldDoc[] {
                createFieldDocStartStopElementForHybridSearchResults(0, new Object[] { 1 }),
                createFieldDocDelimiterElementForHybridSearchResults(0, new Object[] { 1 }),
                createFieldDocDelimiterElementForHybridSearchResults(0, new Object[] { 1 }),
                createFieldDocStartStopElementForHybridSearchResults(0, new Object[] { 1 }) },
            sortAndFormats.sort.getSort()

        );
        TopDocsAndMaxScore topDocsAndMaxScoreOriginal = new TopDocsAndMaxScore(topDocsOriginal, 0);
        TopDocs topDocsNew = new TopFieldDocs(
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),

            new FieldDoc[] {
                createFieldDocStartStopElementForHybridSearchResults(2, new Object[] { 1 }),
                createFieldDocDelimiterElementForHybridSearchResults(2, new Object[] { 1 }),
                createFieldDocDelimiterElementForHybridSearchResults(2, new Object[] { 1 }),
                createFieldDocStartStopElementForHybridSearchResults(2, new Object[] { 1 }) },
            sortAndFormats.sort.getSort()
        );
        TopDocsAndMaxScore topDocsAndMaxScoreNew = new TopDocsAndMaxScore(topDocsNew, 0);
        TopDocsAndMaxScore mergedTopDocsAndMaxScore = topDocsMerger.merge(topDocsAndMaxScoreOriginal, topDocsAndMaxScoreNew);

        assertNotNull(mergedTopDocsAndMaxScore);

        assertEquals(0f, mergedTopDocsAndMaxScore.maxScore, DELTA_FOR_ASSERTION);
        assertEquals(0, mergedTopDocsAndMaxScore.topDocs.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, mergedTopDocsAndMaxScore.topDocs.totalHits.relation());
        assertEquals(4, mergedTopDocsAndMaxScore.topDocs.scoreDocs.length);
        // check format, all elements one by one
        FieldDoc[] fieldDocs = (FieldDoc[]) mergedTopDocsAndMaxScore.topDocs.scoreDocs;
        assertEquals(1, fieldDocs[0].fields[0]);
        assertEquals(1, fieldDocs[1].fields[0]);
        assertEquals(1, fieldDocs[2].fields[0]);
        assertEquals(1, fieldDocs[3].fields[0]);
    }

    @SneakyThrows
    public void testThreeSequentialMergesWithFieldDocs_whenAllTopDocsHasHits_thenSuccessful() {
        DocValueFormat docValueFormat[] = new DocValueFormat[] { DocValueFormat.RAW };
        SortField sortField = new SortField("stock", SortField.Type.INT, true);
        Sort sort = new Sort(sortField);
        SortAndFormats sortAndFormats = new SortAndFormats(sort, docValueFormat);
        TopDocsMerger topDocsMerger = new TopDocsMerger(sortAndFormats, null);

        TopDocs topDocsOriginal = new TopFieldDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),

            new FieldDoc[] {
                createFieldDocStartStopElementForHybridSearchResults(0, new Object[] { 1 }),
                createFieldDocDelimiterElementForHybridSearchResults(0, new Object[] { 1 }),
                new FieldDoc(0, 0.5f, new Object[] { 100 }),
                new FieldDoc(2, 0.3f, new Object[] { 20 }),
                createFieldDocDelimiterElementForHybridSearchResults(0, new Object[] { 1 }),
                createFieldDocStartStopElementForHybridSearchResults(0, new Object[] { 1 }) },
            sortAndFormats.sort.getSort()
        );
        TopDocsAndMaxScore topDocsAndMaxScoreOriginal = new TopDocsAndMaxScore(topDocsOriginal, 0.5f);
        TopDocs topDocsNew = new TopFieldDocs(
            new TotalHits(4, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),

            new FieldDoc[] {
                createFieldDocStartStopElementForHybridSearchResults(2, new Object[] { 1 }),
                createFieldDocDelimiterElementForHybridSearchResults(2, new Object[] { 1 }),
                new FieldDoc(1, 0.7f, new Object[] { 80 }),
                new FieldDoc(4, 0.3f, new Object[] { 30 }),
                new FieldDoc(5, 0.05f, new Object[] { 10 }),
                createFieldDocDelimiterElementForHybridSearchResults(2, new Object[] { 1 }),
                new FieldDoc(4, 0.6f, new Object[] { 30 }),
                createFieldDocStartStopElementForHybridSearchResults(2, new Object[] { 1 }) },
            sortAndFormats.sort.getSort()
        );
        TopDocsAndMaxScore topDocsAndMaxScoreNew = new TopDocsAndMaxScore(topDocsNew, 0.7f);
        TopDocsAndMaxScore firstMergedTopDocsAndMaxScore = topDocsMerger.merge(topDocsAndMaxScoreOriginal, topDocsAndMaxScoreNew);

        assertNotNull(firstMergedTopDocsAndMaxScore);

        // merge results from collector 3
        TopDocs topDocsThirdCollector = new TopFieldDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),

            new FieldDoc[] {
                createFieldDocStartStopElementForHybridSearchResults(3, new Object[] { 1 }),
                createFieldDocDelimiterElementForHybridSearchResults(3, new Object[] { 1 }),
                new FieldDoc(3, 0.4f, new Object[] { 90 }),
                createFieldDocDelimiterElementForHybridSearchResults(3, new Object[] { 1 }),
                new FieldDoc(7, 0.85f, new Object[] { 60 }),
                new FieldDoc(9, 0.2f, new Object[] { 50 }),
                createFieldDocStartStopElementForHybridSearchResults(3, new Object[] { 1 }) },
            sortAndFormats.sort.getSort()
        );
        TopDocsAndMaxScore topDocsAndMaxScoreThirdCollector = new TopDocsAndMaxScore(topDocsThirdCollector, 0.85f);
        TopDocsAndMaxScore finalMergedTopDocsAndMaxScore = topDocsMerger.merge(
            firstMergedTopDocsAndMaxScore,
            topDocsAndMaxScoreThirdCollector
        );

        assertEquals(0.85f, finalMergedTopDocsAndMaxScore.maxScore, DELTA_FOR_ASSERTION);
        assertEquals(9, finalMergedTopDocsAndMaxScore.topDocs.totalHits.value());
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, finalMergedTopDocsAndMaxScore.topDocs.totalHits.relation());
        // expected number of rows is 6 from sub-query1 and 3 from sub-query2, plus 2 start-stop elements + 2 delimiters
        // 6 + 3 + 2 + 2 = 13
        assertEquals(13, finalMergedTopDocsAndMaxScore.topDocs.scoreDocs.length);
        // check format, all elements one by one
        FieldDoc[] fieldDocs = (FieldDoc[]) finalMergedTopDocsAndMaxScore.topDocs.scoreDocs;
        assertEquals(1, fieldDocs[0].fields[0]);
        assertEquals(1, fieldDocs[1].fields[0]);
        assertFieldDoc(fieldDocs[2], 0, 100);
        assertFieldDoc(fieldDocs[3], 3, 90);
        assertFieldDoc(fieldDocs[4], 1, 80);
        assertFieldDoc(fieldDocs[5], 4, 30);
        assertFieldDoc(fieldDocs[6], 2, 20);
        assertFieldDoc(fieldDocs[7], 5, 10);
        assertEquals(1, fieldDocs[8].fields[0]);
        assertFieldDoc(fieldDocs[9], 7, 60);
        assertFieldDoc(fieldDocs[10], 9, 50);
        assertFieldDoc(fieldDocs[11], 4, 30);
        assertEquals(1, fieldDocs[12].fields[0]);
    }

    @SneakyThrows
    public void testMergeFieldDocsAndCollapseValues_whenBothTopDocsHasHits_thenSuccessful() {
        DocValueFormat docValueFormat[] = new DocValueFormat[] { DocValueFormat.RAW };
        SortField sortField = new SortField("stock", SortField.Type.INT, true);
        Sort sort = new Sort(sortField);
        SortAndFormats sortAndFormats = new SortAndFormats(sort, docValueFormat);
        InnerHitBuilder collapsedAgesInnerHitBuilder = new InnerHitBuilder("authors");
        collapsedAgesInnerHitBuilder.setSize(10);
        collapsedAgesInnerHitBuilder.setSorts(List.of(SortBuilders.scoreSort()));

        List<InnerHitBuilder> innerHitBuilders = new ArrayList<>();
        innerHitBuilders.add(collapsedAgesInnerHitBuilder);
        CollapseContext collapseContext = new CollapseContext("author", null, innerHitBuilders);
        TopDocsMerger topDocsMerger = new TopDocsMerger(sortAndFormats, collapseContext);

        TopDocs topDocsOriginal = new CollapseTopFieldDocs(
            "author",
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),

            new FieldDoc[] {
                createFieldDocStartStopElementForHybridSearchResults(0, new Object[] { 1 }),
                createFieldDocDelimiterElementForHybridSearchResults(0, new Object[] { 1 }),
                new FieldDoc(0, 0.5f, new Object[] { 100 }),
                new FieldDoc(2, 0.3f, new Object[] { 80 }),
                createFieldDocDelimiterElementForHybridSearchResults(0, new Object[] { 1 }),
                createFieldDocStartStopElementForHybridSearchResults(0, new Object[] { 1 }) },
            sortAndFormats.sort.getSort(),
            new Object[] { 0, 0, new BytesRef(), new BytesRef(), 0, 0 }
        );
        TopDocsAndMaxScore topDocsAndMaxScoreOriginal = new TopDocsAndMaxScore(topDocsOriginal, 0.5f);
        TopDocs topDocsNew = new CollapseTopFieldDocs(
            "author",
            new TotalHits(4, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),

            new FieldDoc[] {
                createFieldDocStartStopElementForHybridSearchResults(1, new Object[] { 1 }),
                createFieldDocDelimiterElementForHybridSearchResults(1, new Object[] { 1 }),
                new FieldDoc(1, 0.7f, new Object[] { 70 }),
                new FieldDoc(4, 0.3f, new Object[] { 60 }),
                new FieldDoc(5, 0.05f, new Object[] { 30 }),
                createFieldDocDelimiterElementForHybridSearchResults(1, new Object[] { 1 }),
                new FieldDoc(4, 0.6f, new Object[] { 40 }),
                createFieldDocStartStopElementForHybridSearchResults(1, new Object[] { 1 }) },
            sortAndFormats.sort.getSort(),
            new Object[] { 0, 0, new BytesRef(), new BytesRef(), new BytesRef(), 0, new BytesRef(), 0 }
        );
        TopDocsAndMaxScore topDocsAndMaxScoreNew = new TopDocsAndMaxScore(topDocsNew, 0.7f);
        TopDocsAndMaxScore mergedTopDocsAndMaxScore = topDocsMerger.merge(topDocsAndMaxScoreOriginal, topDocsAndMaxScoreNew);

        assertNotNull(mergedTopDocsAndMaxScore);

        CollapseTopFieldDocs collapseTopFieldDocs = (CollapseTopFieldDocs) mergedTopDocsAndMaxScore.topDocs;
        Object[] mergedCollapseValues = collapseTopFieldDocs.collapseValues;
        assertEquals(10, mergedCollapseValues.length);
        assertEquals(0, mergedCollapseValues[0]);
        assertEquals(0, mergedCollapseValues[1]);
        assertEquals(new BytesRef(), mergedCollapseValues[2]);
        assertEquals(new BytesRef(), mergedCollapseValues[3]);
        assertEquals(new BytesRef(), mergedCollapseValues[4]);
        assertEquals(new BytesRef(), mergedCollapseValues[5]);
        assertEquals(new BytesRef(), mergedCollapseValues[6]);
        assertEquals(0, mergedCollapseValues[7]);
        assertEquals(new BytesRef(), mergedCollapseValues[8]);
        assertEquals(0, mergedCollapseValues[9]);
    }

    @SneakyThrows
    public void testMergeFieldDocsAndCollapseValues_whenBothTopDocsHasNoHits_thenSuccessful() {
        DocValueFormat docValueFormat[] = new DocValueFormat[] { DocValueFormat.RAW };
        SortField sortField = new SortField("stock", SortField.Type.INT, true);
        Sort sort = new Sort(sortField);
        SortAndFormats sortAndFormats = new SortAndFormats(sort, docValueFormat);
        InnerHitBuilder collapsedAgesInnerHitBuilder = new InnerHitBuilder("authors");
        collapsedAgesInnerHitBuilder.setSize(10);
        collapsedAgesInnerHitBuilder.setSorts(List.of(SortBuilders.scoreSort()));

        List<InnerHitBuilder> innerHitBuilders = new ArrayList<>();
        innerHitBuilders.add(collapsedAgesInnerHitBuilder);
        CollapseContext collapseContext = new CollapseContext("author", null, innerHitBuilders);
        TopDocsMerger topDocsMerger = new TopDocsMerger(sortAndFormats, collapseContext);

        TopDocs topDocsOriginal = new CollapseTopFieldDocs(
            "author",
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),

            new FieldDoc[] {
                createFieldDocStartStopElementForHybridSearchResults(0, new Object[] { 1 }),
                createFieldDocDelimiterElementForHybridSearchResults(0, new Object[] { 1 }),
                createFieldDocDelimiterElementForHybridSearchResults(0, new Object[] { 1 }),
                createFieldDocStartStopElementForHybridSearchResults(0, new Object[] { 1 }) },
            sortAndFormats.sort.getSort(),
            new Object[] { 0, 0, 0, 0 }
        );
        TopDocsAndMaxScore topDocsAndMaxScoreOriginal = new TopDocsAndMaxScore(topDocsOriginal, 0);
        TopDocs topDocsNew = new CollapseTopFieldDocs(
            "author",
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),

            new FieldDoc[] {
                createFieldDocStartStopElementForHybridSearchResults(2, new Object[] { 1 }),
                createFieldDocDelimiterElementForHybridSearchResults(2, new Object[] { 1 }),
                createFieldDocDelimiterElementForHybridSearchResults(2, new Object[] { 1 }),
                createFieldDocStartStopElementForHybridSearchResults(2, new Object[] { 1 }) },
            sortAndFormats.sort.getSort(),
            new Object[] { 0, 0, 0, 0 }
        );
        TopDocsAndMaxScore topDocsAndMaxScoreNew = new TopDocsAndMaxScore(topDocsNew, 0);
        TopDocsAndMaxScore mergedTopDocsAndMaxScore = topDocsMerger.merge(topDocsAndMaxScoreOriginal, topDocsAndMaxScoreNew);

        assertNotNull(mergedTopDocsAndMaxScore);

        CollapseTopFieldDocs collapseTopFieldDocs = (CollapseTopFieldDocs) mergedTopDocsAndMaxScore.topDocs;
        Object[] mergedCollapseValues = collapseTopFieldDocs.collapseValues;
        assertEquals(4, mergedCollapseValues.length);
        assertEquals(0, mergedCollapseValues[0]);
        assertEquals(0, mergedCollapseValues[1]);
        assertEquals(0, mergedCollapseValues[2]);
        assertEquals(0, mergedCollapseValues[3]);
    }

    private void assertScoreDoc(ScoreDoc scoreDoc, int expectedDocId, float expectedScore) {
        assertEquals(expectedDocId, scoreDoc.doc);
        assertEquals(expectedScore, scoreDoc.score, DELTA_FOR_ASSERTION);
    }

    private void assertFieldDoc(FieldDoc fieldDoc, int expectedDocId, int expectedSortValue) {
        assertEquals(expectedDocId, fieldDoc.doc);
        assertEquals(expectedSortValue, fieldDoc.fields[0]);
    }
}
