/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.util;

import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.MAGIC_NUMBER_DELIMITER;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.MAGIC_NUMBER_START_STOP;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createDelimiterElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createStartStopElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQueryDelimiterElement;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQueryStartStopElement;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQueryScoreDocElement;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQuerySpecialElement;

import org.apache.lucene.search.ScoreDoc;
import org.opensearch.common.Randomness;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

public class HybridSearchResultFormatUtilTests extends OpenSearchQueryTestCase {

    public void testScoreDocsListElements_whenTestingListElements_thenCheckResultsAreCorrect() {
        ScoreDoc validStartStopElement = new ScoreDoc(0, MAGIC_NUMBER_START_STOP);
        assertTrue(isHybridQueryStartStopElement(validStartStopElement));

        ScoreDoc validStartStopElement1 = new ScoreDoc(-1, MAGIC_NUMBER_START_STOP);
        assertFalse(isHybridQueryStartStopElement(validStartStopElement1));

        ScoreDoc validStartStopElement2 = new ScoreDoc(0, Randomness.get().nextFloat());
        assertFalse(isHybridQueryStartStopElement(validStartStopElement2));

        assertFalse(isHybridQueryStartStopElement(null));

        ScoreDoc validDelimiterElement = new ScoreDoc(0, MAGIC_NUMBER_DELIMITER);
        assertTrue(isHybridQueryDelimiterElement(validDelimiterElement));

        ScoreDoc validDelimiterElement1 = new ScoreDoc(-1, MAGIC_NUMBER_DELIMITER);
        assertFalse(isHybridQueryDelimiterElement(validDelimiterElement1));

        ScoreDoc validDelimiterElement2 = new ScoreDoc(0, Randomness.get().nextFloat());
        assertFalse(isHybridQueryDelimiterElement(validDelimiterElement2));

        assertFalse(isHybridQueryDelimiterElement(null));
    }

    public void testCreateElements_whenCreateStartStopAndDelimiterElements_thenSuccessful() {
        int docId = 1;
        ScoreDoc startStopElement = createStartStopElementForHybridSearchResults(docId);
        assertNotNull(startStopElement);
        assertEquals(docId, startStopElement.doc);
        assertEquals(MAGIC_NUMBER_START_STOP, startStopElement.score, 0.0f);

        ScoreDoc delimiterElement = createDelimiterElementForHybridSearchResults(docId);
        assertNotNull(delimiterElement);
        assertEquals(docId, delimiterElement.doc);
        assertEquals(MAGIC_NUMBER_DELIMITER, delimiterElement.score, 0.0f);
    }

    public void testSpecialElementCheck_whenElementIsSpecialAndIsNotSpecial_thenSuccessful() {
        int docId = 1;
        ScoreDoc startStopElement = new ScoreDoc(docId, MAGIC_NUMBER_START_STOP);
        assertTrue(isHybridQuerySpecialElement(startStopElement));
        assertFalse(isHybridQueryScoreDocElement(startStopElement));

        ScoreDoc delimiterElement = new ScoreDoc(docId, MAGIC_NUMBER_DELIMITER);
        assertTrue(isHybridQuerySpecialElement(delimiterElement));
        assertFalse(isHybridQueryScoreDocElement(delimiterElement));
    }

    public void testScoreElementCheck_whenElementIsSpecialAndIsNotSpecial_thenSuccessful() {
        int docId = 1;
        float score = Randomness.get().nextFloat();
        ScoreDoc startStopElement = new ScoreDoc(docId, score);
        assertFalse(isHybridQuerySpecialElement(startStopElement));
        assertTrue(isHybridQueryScoreDocElement(startStopElement));
    }
}
