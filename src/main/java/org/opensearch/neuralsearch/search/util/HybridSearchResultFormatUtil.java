/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.util;

import java.util.Objects;

import org.apache.lucene.search.ScoreDoc;

/**
 * Utility class for handling format of Hybrid Search query results
 */
public class HybridSearchResultFormatUtil {
    // both magic numbers are randomly generated, there should be no collision as whole part of score is huge
    // and OpenSearch convention is that scores are positive numbers
    public static final Float MAGIC_NUMBER_START_STOP = -9549511920.4881596047f;
    public static final Float MAGIC_NUMBER_DELIMITER = -4422440593.9791198149f;

    /**
     * Create ScoreDoc object that is a start/stop element in case of hybrid search query results
     * @param docId id of one of docs from actual result object, or -1 if there are no matches
     * @return
     */
    public static ScoreDoc createStartStopElementForHybridSearchResults(final int docId) {
        return new ScoreDoc(docId, MAGIC_NUMBER_START_STOP);
    }

    /**
     * Create ScoreDoc object that is a delimiter element between sub-query results in hybrid search query results
     * @param docId id of one of docs from actual result object, or -1 if there are no matches
     * @return
     */
    public static ScoreDoc createDelimiterElementForHybridSearchResults(final int docId) {
        return new ScoreDoc(docId, MAGIC_NUMBER_DELIMITER);
    }

    /**
     * Checking if passed scoreDocs object is a start/stop element in the list of hybrid query result scores
     * @param scoreDoc
     * @return true if it is a start/stop element
     */
    public static boolean isHybridQueryStartStopElement(final ScoreDoc scoreDoc) {
        return Objects.nonNull(scoreDoc) && scoreDoc.doc >= 0 && Float.compare(scoreDoc.score, MAGIC_NUMBER_START_STOP) == 0;
    }

    /**
     * Checking if passed scoreDocs object is a delimiter element in the list of hybrid query result scores
     * @param scoreDoc
     * @return true if it is a delimiter element
     */
    public static boolean isHybridQueryDelimiterElement(final ScoreDoc scoreDoc) {
        return Objects.nonNull(scoreDoc) && scoreDoc.doc >= 0 && Float.compare(scoreDoc.score, MAGIC_NUMBER_DELIMITER) == 0;
    }
}
