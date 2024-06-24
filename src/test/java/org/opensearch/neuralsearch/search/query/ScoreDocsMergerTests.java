/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import org.apache.lucene.search.ScoreDoc;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

import static org.opensearch.neuralsearch.search.query.HybridCollectorManager.SCORE_DOC_BY_SCORE_COMPARATOR;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createStartStopElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createDelimiterElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.MAGIC_NUMBER_START_STOP;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.MAGIC_NUMBER_DELIMITER;

public class ScoreDocsMergerTests extends OpenSearchQueryTestCase {

    private static final float DELTA_FOR_ASSERTION = 0.001f;

    public void testIncorrectInput_whenScoreDocsAreNullOrNotEnoughElements_thenFail() {
        ScoreDocsMerger scoreDocsMerger = new ScoreDocsMerger();

        ScoreDoc[] scores = new ScoreDoc[] {
            createStartStopElementForHybridSearchResults(2),
            createDelimiterElementForHybridSearchResults(2),
            new ScoreDoc(1, 0.7f),
            createStartStopElementForHybridSearchResults(2) };

        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> scoreDocsMerger.mergedScoreDocs(scores, null, SCORE_DOC_BY_SCORE_COMPARATOR)
        );
        assertEquals("score docs cannot be null", exception.getMessage());

        exception = assertThrows(
            NullPointerException.class,
            () -> scoreDocsMerger.mergedScoreDocs(scores, null, SCORE_DOC_BY_SCORE_COMPARATOR)
        );
        assertEquals("score docs cannot be null", exception.getMessage());

        ScoreDoc[] lessElementsScoreDocs = new ScoreDoc[] { createStartStopElementForHybridSearchResults(2), new ScoreDoc(1, 0.7f) };

        IllegalArgumentException notEnoughException = assertThrows(
            IllegalArgumentException.class,
            () -> scoreDocsMerger.mergedScoreDocs(lessElementsScoreDocs, scores, SCORE_DOC_BY_SCORE_COMPARATOR)
        );
        assertEquals("cannot merge top docs because it does not have enough elements", notEnoughException.getMessage());

        notEnoughException = assertThrows(
            IllegalArgumentException.class,
            () -> scoreDocsMerger.mergedScoreDocs(scores, lessElementsScoreDocs, SCORE_DOC_BY_SCORE_COMPARATOR)
        );
        assertEquals("cannot merge top docs because it does not have enough elements", notEnoughException.getMessage());
    }

    public void testMergeScoreDocs_whenBothTopDocsHasHits_thenSuccessful() {
        ScoreDocsMerger scoreDocsMerger = new ScoreDocsMerger();

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

        ScoreDoc[] mergedScoreDocs = scoreDocsMerger.mergedScoreDocs(scoreDocsOriginal, scoreDocsNew, SCORE_DOC_BY_SCORE_COMPARATOR);

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
        ScoreDocsMerger scoreDocsMerger = new ScoreDocsMerger();

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

        ScoreDoc[] mergedScoreDocs = scoreDocsMerger.mergedScoreDocs(scoreDocsOriginal, scoreDocsNew, SCORE_DOC_BY_SCORE_COMPARATOR);

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
        ScoreDocsMerger scoreDocsMerger = new ScoreDocsMerger();

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

        ScoreDoc[] mergedScoreDocs = scoreDocsMerger.mergedScoreDocs(scoreDocsOriginal, scoreDocsNew, SCORE_DOC_BY_SCORE_COMPARATOR);

        assertNotNull(mergedScoreDocs);
        assertEquals(4, mergedScoreDocs.length);
        // check format, all elements one by one
        assertEquals(MAGIC_NUMBER_START_STOP, mergedScoreDocs[0].score, 0);
        assertEquals(MAGIC_NUMBER_DELIMITER, mergedScoreDocs[1].score, 0);
        assertEquals(MAGIC_NUMBER_DELIMITER, mergedScoreDocs[2].score, 0);
        assertEquals(MAGIC_NUMBER_START_STOP, mergedScoreDocs[3].score, 0);
    }

    private void assertScoreDoc(ScoreDoc scoreDoc, int expectedDocId, float expectedScore) {
        assertEquals(expectedDocId, scoreDoc.doc);
        assertEquals(expectedScore, scoreDoc.score, DELTA_FOR_ASSERTION);
    }
}
