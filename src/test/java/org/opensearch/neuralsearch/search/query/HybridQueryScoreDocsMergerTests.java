/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createFieldDocDelimiterElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createFieldDocStartStopElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createStartStopElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createDelimiterElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.MAGIC_NUMBER_START_STOP;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.MAGIC_NUMBER_DELIMITER;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.sort.SortAndFormats;

public class HybridQueryScoreDocsMergerTests extends OpenSearchQueryTestCase {

    public void testIncorrectInput_whenScoreDocsAreNullOrNotEnoughElements_thenFail() {
        HybridQueryScoreDocsMerger<ScoreDoc> scoreDocsMerger = new HybridQueryScoreDocsMerger<>();
        TopDocsMerger topDocsMerger = new TopDocsMerger(null);
        ScoreDoc[] scores = new ScoreDoc[] {
            createStartStopElementForHybridSearchResults(2),
            createDelimiterElementForHybridSearchResults(2),
            new ScoreDoc(1, 0.7f),
            createStartStopElementForHybridSearchResults(2) };

        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> scoreDocsMerger.merge(scores, null, topDocsMerger.SCORE_DOC_BY_SCORE_COMPARATOR, false)
        );
        assertEquals("score docs cannot be null", exception.getMessage());

        exception = assertThrows(
            NullPointerException.class,
            () -> scoreDocsMerger.merge(scores, null, topDocsMerger.SCORE_DOC_BY_SCORE_COMPARATOR, false)
        );
        assertEquals("score docs cannot be null", exception.getMessage());

        ScoreDoc[] lessElementsScoreDocs = new ScoreDoc[] { createStartStopElementForHybridSearchResults(2), new ScoreDoc(1, 0.7f) };

        IllegalArgumentException notEnoughException = assertThrows(
            IllegalArgumentException.class,
            () -> scoreDocsMerger.merge(lessElementsScoreDocs, scores, topDocsMerger.SCORE_DOC_BY_SCORE_COMPARATOR, false)
        );
        assertEquals("cannot merge top docs because it does not have enough elements", notEnoughException.getMessage());

        notEnoughException = assertThrows(
            IllegalArgumentException.class,
            () -> scoreDocsMerger.merge(scores, lessElementsScoreDocs, topDocsMerger.SCORE_DOC_BY_SCORE_COMPARATOR, false)
        );
        assertEquals("cannot merge top docs because it does not have enough elements", notEnoughException.getMessage());
    }

    public void testMergeScoreDocs_whenBothTopDocsHasHits_thenSuccessful() {
        HybridQueryScoreDocsMerger<ScoreDoc> scoreDocsMerger = new HybridQueryScoreDocsMerger<>();
        ScoreDoc[] scoreDocsOriginal = new ScoreDoc[] {
            createStartStopElementForHybridSearchResults(0),
            createDelimiterElementForHybridSearchResults(0),
            new ScoreDoc(0, 0.5f),
            new ScoreDoc(2, 0.3f),
            createDelimiterElementForHybridSearchResults(0),
            createStartStopElementForHybridSearchResults(0) };

        ScoreDoc[] scoreDocsNew = new ScoreDoc[] {
            createStartStopElementForHybridSearchResults(2),
            createDelimiterElementForHybridSearchResults(2),
            new ScoreDoc(1, 0.7f),
            new ScoreDoc(4, 0.3f),
            new ScoreDoc(5, 0.05f),
            createDelimiterElementForHybridSearchResults(2),
            new ScoreDoc(4, 0.6f),
            createStartStopElementForHybridSearchResults(2) };

        TopDocsMerger topDocsMerger = new TopDocsMerger(null);
        ScoreDoc[] mergedScoreDocs = scoreDocsMerger.merge(
            scoreDocsOriginal,
            scoreDocsNew,
            topDocsMerger.SCORE_DOC_BY_SCORE_COMPARATOR,
            false
        );

        assertNotNull(mergedScoreDocs);
        assertEquals(10, mergedScoreDocs.length);

        // check format, all elements one by one
        assertEquals(MAGIC_NUMBER_START_STOP, mergedScoreDocs[0].score, 0);
        assertEquals(MAGIC_NUMBER_DELIMITER, mergedScoreDocs[1].score, 0);
        assertScoreDoc(mergedScoreDocs[2], 1, 0.7f);
        assertScoreDoc(mergedScoreDocs[3], 0, 0.5f);
        assertScoreDoc(mergedScoreDocs[4], 2, 0.3f);
        assertScoreDoc(mergedScoreDocs[5], 4, 0.3f);
        assertScoreDoc(mergedScoreDocs[6], 5, 0.05f);
        assertEquals(MAGIC_NUMBER_DELIMITER, mergedScoreDocs[7].score, 0);
        assertScoreDoc(mergedScoreDocs[8], 4, 0.6f);
        assertEquals(MAGIC_NUMBER_START_STOP, mergedScoreDocs[9].score, 0);
    }

    public void testMergeScoreDocs_whenOneTopDocsHasHitsAndOtherIsEmpty_thenSuccessful() {
        HybridQueryScoreDocsMerger<ScoreDoc> scoreDocsMerger = new HybridQueryScoreDocsMerger<>();
        TopDocsMerger topDocsMerger = new TopDocsMerger(null);
        ScoreDoc[] scoreDocsOriginal = new ScoreDoc[] {
            createStartStopElementForHybridSearchResults(0),
            createDelimiterElementForHybridSearchResults(0),
            createDelimiterElementForHybridSearchResults(0),
            createStartStopElementForHybridSearchResults(0) };
        ScoreDoc[] scoreDocsNew = new ScoreDoc[] {
            createStartStopElementForHybridSearchResults(2),
            createDelimiterElementForHybridSearchResults(2),
            new ScoreDoc(1, 0.7f),
            new ScoreDoc(4, 0.3f),
            new ScoreDoc(5, 0.05f),
            createDelimiterElementForHybridSearchResults(2),
            new ScoreDoc(4, 0.6f),
            createStartStopElementForHybridSearchResults(2) };

        ScoreDoc[] mergedScoreDocs = scoreDocsMerger.merge(
            scoreDocsOriginal,
            scoreDocsNew,
            topDocsMerger.SCORE_DOC_BY_SCORE_COMPARATOR,
            false
        );

        assertNotNull(mergedScoreDocs);
        assertEquals(8, mergedScoreDocs.length);

        assertEquals(MAGIC_NUMBER_START_STOP, mergedScoreDocs[0].score, 0);
        assertEquals(MAGIC_NUMBER_DELIMITER, mergedScoreDocs[1].score, 0);
        assertScoreDoc(mergedScoreDocs[2], 1, 0.7f);
        assertScoreDoc(mergedScoreDocs[3], 4, 0.3f);
        assertScoreDoc(mergedScoreDocs[4], 5, 0.05f);
        assertEquals(MAGIC_NUMBER_DELIMITER, mergedScoreDocs[5].score, 0);
        assertScoreDoc(mergedScoreDocs[6], 4, 0.6f);
        assertEquals(MAGIC_NUMBER_START_STOP, mergedScoreDocs[7].score, 0);
    }

    public void testMergeScoreDocs_whenBothTopDocsHasNoHits_thenSuccessful() {
        HybridQueryScoreDocsMerger<ScoreDoc> scoreDocsMerger = new HybridQueryScoreDocsMerger<>();
        TopDocsMerger topDocsMerger = new TopDocsMerger(null);
        ScoreDoc[] scoreDocsOriginal = new ScoreDoc[] {
            createStartStopElementForHybridSearchResults(0),
            createDelimiterElementForHybridSearchResults(0),
            createDelimiterElementForHybridSearchResults(0),
            createStartStopElementForHybridSearchResults(0) };
        ScoreDoc[] scoreDocsNew = new ScoreDoc[] {
            createStartStopElementForHybridSearchResults(2),
            createDelimiterElementForHybridSearchResults(2),
            createDelimiterElementForHybridSearchResults(2),
            createStartStopElementForHybridSearchResults(2) };

        ScoreDoc[] mergedScoreDocs = scoreDocsMerger.merge(
            scoreDocsOriginal,
            scoreDocsNew,
            topDocsMerger.SCORE_DOC_BY_SCORE_COMPARATOR,
            false
        );

        assertNotNull(mergedScoreDocs);
        assertEquals(4, mergedScoreDocs.length);
        // check format, all elements one by one
        assertEquals(MAGIC_NUMBER_START_STOP, mergedScoreDocs[0].score, 0);
        assertEquals(MAGIC_NUMBER_DELIMITER, mergedScoreDocs[1].score, 0);
        assertEquals(MAGIC_NUMBER_DELIMITER, mergedScoreDocs[2].score, 0);
        assertEquals(MAGIC_NUMBER_START_STOP, mergedScoreDocs[3].score, 0);
    }

    public void testIncorrectInput_whenFieldDocsAreNullOrNotEnoughElements_thenFail() {
        HybridQueryScoreDocsMerger<FieldDoc> fieldDocsMerger = new HybridQueryScoreDocsMerger<>();
        DocValueFormat docValueFormat[] = new DocValueFormat[] { DocValueFormat.RAW };
        SortField sortField = new SortField("stock", SortField.Type.INT, true);
        Sort sort = new Sort(sortField);
        SortAndFormats sortAndFormats = new SortAndFormats(sort, docValueFormat);
        TopDocsMerger topDocsMerger = new TopDocsMerger(sortAndFormats);

        FieldDoc[] scores = new FieldDoc[] {
            createFieldDocStartStopElementForHybridSearchResults(2, new Object[] { 1 }),
            createFieldDocDelimiterElementForHybridSearchResults(2, new Object[] { 1 }),
            new FieldDoc(1, 0.7f, new Object[] { 100 }),
            createFieldDocStartStopElementForHybridSearchResults(2, new Object[] { 1 }) };

        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> fieldDocsMerger.merge(scores, null, topDocsMerger.FIELD_DOC_BY_SORT_CRITERIA_COMPARATOR, true)
        );
        assertEquals("score docs cannot be null", exception.getMessage());

        exception = assertThrows(
            NullPointerException.class,
            () -> fieldDocsMerger.merge(scores, null, topDocsMerger.FIELD_DOC_BY_SORT_CRITERIA_COMPARATOR, true)
        );
        assertEquals("score docs cannot be null", exception.getMessage());

        FieldDoc[] lessElementsScoreDocs = new FieldDoc[] {
            createFieldDocStartStopElementForHybridSearchResults(0, new Object[] { 1 }),
            new FieldDoc(1, 0.7f, new Object[] { 100 }) };

        IllegalArgumentException notEnoughException = assertThrows(
            IllegalArgumentException.class,
            () -> fieldDocsMerger.merge(lessElementsScoreDocs, scores, topDocsMerger.FIELD_DOC_BY_SORT_CRITERIA_COMPARATOR, true)
        );
        assertEquals("cannot merge top docs because it does not have enough elements", notEnoughException.getMessage());

        notEnoughException = assertThrows(
            IllegalArgumentException.class,
            () -> fieldDocsMerger.merge(scores, lessElementsScoreDocs, topDocsMerger.FIELD_DOC_BY_SORT_CRITERIA_COMPARATOR, true)
        );
        assertEquals("cannot merge top docs because it does not have enough elements", notEnoughException.getMessage());
    }

    public void testMergeFieldDocs_whenBothTopDocsHasHits_thenSuccessful() {
        HybridQueryScoreDocsMerger<FieldDoc> fieldDocsMerger = new HybridQueryScoreDocsMerger<>();
        FieldDoc[] fieldDocsOriginal = new FieldDoc[] {
            createFieldDocStartStopElementForHybridSearchResults(0, new Object[] { 1 }),
            createFieldDocDelimiterElementForHybridSearchResults(0, new Object[] { 1 }),
            new FieldDoc(0, 0.5f, new Object[] { 100 }),
            new FieldDoc(2, 0.3f, new Object[] { 80 }),
            createFieldDocDelimiterElementForHybridSearchResults(0, new Object[] { 1 }),
            createFieldDocStartStopElementForHybridSearchResults(0, new Object[] { 1 }) };

        FieldDoc[] fieldDocsNew = new FieldDoc[] {
            createFieldDocStartStopElementForHybridSearchResults(2, new Object[] { 1 }),
            createFieldDocDelimiterElementForHybridSearchResults(2, new Object[] { 1 }),
            new FieldDoc(1, 0.7f, new Object[] { 10 }),
            new FieldDoc(4, 0.3f, new Object[] { 5 }),
            new FieldDoc(5, 0.05f, new Object[] { 2 }),
            createFieldDocDelimiterElementForHybridSearchResults(2, new Object[] { 1 }),
            new FieldDoc(4, 0.6f, new Object[] { 5 }),
            createFieldDocStartStopElementForHybridSearchResults(2, new Object[] { 1 }) };

        DocValueFormat docValueFormat[] = new DocValueFormat[] { DocValueFormat.RAW };
        SortField sortField = new SortField("stock", SortField.Type.INT, true);
        Sort sort = new Sort(sortField);
        SortAndFormats sortAndFormats = new SortAndFormats(sort, docValueFormat);
        TopDocsMerger topDocsMerger = new TopDocsMerger(sortAndFormats);

        FieldDoc[] mergedFieldDocs = fieldDocsMerger.merge(
            fieldDocsOriginal,
            fieldDocsNew,
            topDocsMerger.FIELD_DOC_BY_SORT_CRITERIA_COMPARATOR,
            true
        );

        assertNotNull(mergedFieldDocs);
        assertEquals(10, mergedFieldDocs.length);

        // check format, all elements one by one
        assertEquals(1, mergedFieldDocs[0].fields[0]);
        assertEquals(1, mergedFieldDocs[1].fields[0]);
        assertFieldDoc(mergedFieldDocs[2], 0, 100);
        assertFieldDoc(mergedFieldDocs[3], 2, 80);
        assertFieldDoc(mergedFieldDocs[4], 1, 10);
        assertFieldDoc(mergedFieldDocs[5], 4, 5);
        assertFieldDoc(mergedFieldDocs[6], 5, 2);
        assertEquals(1, mergedFieldDocs[7].fields[0]);
        assertFieldDoc(mergedFieldDocs[8], 4, 5);
        assertEquals(1, mergedFieldDocs[9].fields[0]);
    }

    public void testMergeFieldDocs_whenOneTopDocsHasHitsAndOtherIsEmpty_thenSuccessful() {
        HybridQueryScoreDocsMerger<FieldDoc> fieldDocsMerger = new HybridQueryScoreDocsMerger<>();
        DocValueFormat docValueFormat[] = new DocValueFormat[] { DocValueFormat.RAW };
        SortField sortField = new SortField("stock", SortField.Type.INT, true);
        Sort sort = new Sort(sortField);
        SortAndFormats sortAndFormats = new SortAndFormats(sort, docValueFormat);
        TopDocsMerger topDocsMerger = new TopDocsMerger(sortAndFormats);

        FieldDoc[] fieldDocsOriginal = new FieldDoc[] {
            createFieldDocStartStopElementForHybridSearchResults(0, new Object[] { 1 }),
            createFieldDocDelimiterElementForHybridSearchResults(0, new Object[] { 1 }),
            createFieldDocDelimiterElementForHybridSearchResults(0, new Object[] { 1 }),
            createFieldDocStartStopElementForHybridSearchResults(0, new Object[] { 1 }) };
        FieldDoc[] fieldDocsNew = new FieldDoc[] {
            createFieldDocStartStopElementForHybridSearchResults(2, new Object[] { 1 }),
            createFieldDocDelimiterElementForHybridSearchResults(2, new Object[] { 1 }),
            new FieldDoc(1, 0.7f, new Object[] { 100 }),
            new FieldDoc(4, 0.3f, new Object[] { 80 }),
            new FieldDoc(5, 0.05f, new Object[] { 20 }),
            createFieldDocDelimiterElementForHybridSearchResults(2, new Object[] { 1 }),
            new FieldDoc(4, 0.6f, new Object[] { 50 }),
            createFieldDocStartStopElementForHybridSearchResults(2, new Object[] { 1 }) };

        FieldDoc[] mergedFieldDocs = fieldDocsMerger.merge(
            fieldDocsOriginal,
            fieldDocsNew,
            topDocsMerger.FIELD_DOC_BY_SORT_CRITERIA_COMPARATOR,
            true
        );

        assertNotNull(mergedFieldDocs);
        assertEquals(8, mergedFieldDocs.length);

        assertEquals(1, mergedFieldDocs[0].fields[0]);
        assertEquals(1, mergedFieldDocs[1].fields[0]);
        assertFieldDoc(mergedFieldDocs[2], 1, 100);
        assertFieldDoc(mergedFieldDocs[3], 4, 80);
        assertFieldDoc(mergedFieldDocs[4], 5, 20);
        assertEquals(1, mergedFieldDocs[5].fields[0]);
        assertFieldDoc(mergedFieldDocs[6], 4, 50);
        assertEquals(1, mergedFieldDocs[7].fields[0]);
    }

    public void testMergeFieldDocs_whenBothTopDocsHasNoHits_thenSuccessful() {
        HybridQueryScoreDocsMerger<FieldDoc> fieldDocsMerger = new HybridQueryScoreDocsMerger<>();
        DocValueFormat docValueFormat[] = new DocValueFormat[] { DocValueFormat.RAW };
        SortField sortField = new SortField("stock", SortField.Type.INT, true);
        Sort sort = new Sort(sortField);
        SortAndFormats sortAndFormats = new SortAndFormats(sort, docValueFormat);
        TopDocsMerger topDocsMerger = new TopDocsMerger(sortAndFormats);

        FieldDoc[] fieldDocsOriginal = new FieldDoc[] {
            createFieldDocStartStopElementForHybridSearchResults(0, new Object[] { 1 }),
            createFieldDocDelimiterElementForHybridSearchResults(0, new Object[] { 1 }),
            createFieldDocDelimiterElementForHybridSearchResults(0, new Object[] { 1 }),
            createFieldDocStartStopElementForHybridSearchResults(0, new Object[] { 1 }) };
        FieldDoc[] fieldDocsNew = new FieldDoc[] {
            createFieldDocStartStopElementForHybridSearchResults(2, new Object[] { 1 }),
            createFieldDocDelimiterElementForHybridSearchResults(2, new Object[] { 1 }),
            createFieldDocDelimiterElementForHybridSearchResults(2, new Object[] { 1 }),
            createFieldDocStartStopElementForHybridSearchResults(2, new Object[] { 1 }) };

        FieldDoc[] mergedFieldDocs = fieldDocsMerger.merge(
            fieldDocsOriginal,
            fieldDocsNew,
            topDocsMerger.FIELD_DOC_BY_SORT_CRITERIA_COMPARATOR,
            true
        );

        assertNotNull(mergedFieldDocs);
        assertEquals(4, mergedFieldDocs.length);
        // check format, all elements one by one
        assertEquals(1, mergedFieldDocs[0].fields[0]);
        assertEquals(1, mergedFieldDocs[1].fields[0]);
        assertEquals(1, mergedFieldDocs[2].fields[0]);
        assertEquals(1, mergedFieldDocs[3].fields[0]);
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
