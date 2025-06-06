/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.util;

import java.util.Objects;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.util.BytesRef;

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

    public static FieldDoc createFieldDocStartStopElementForHybridSearchResults(final int docId, final Object[] fields) {
        return new FieldDoc(docId, MAGIC_NUMBER_START_STOP, fields);
    }

    /**
     * Create ScoreDoc object that is a delimiter element between sub-query results in hybrid search query results
     * @param docId id of one of docs from actual result object, or -1 if there are no matches
     * @return
     */
    public static FieldDoc createFieldDocDelimiterElementForHybridSearchResults(final int docId, final Object[] fields) {
        return new FieldDoc(docId, MAGIC_NUMBER_DELIMITER, fields);
    }

    public static byte[] createCollapseValueDelimiterElementForHybridSearchResults() {
        byte[] delimiterValueAsArray = new byte[1];
        delimiterValueAsArray[0] = MAGIC_NUMBER_DELIMITER.byteValue();
        return delimiterValueAsArray;
    }

    public static byte[] createCollapseValueStartStopElementForHybridSearchResults() {
        byte[] startStopValueAsArray = new byte[1];
        startStopValueAsArray[0] = MAGIC_NUMBER_START_STOP.byteValue();
        return startStopValueAsArray;
    }

    /**
     * Checking if passed scoreDocs object is a special element (start/stop or delimiter) in the list of hybrid query result scores
     * @param scoreDoc score doc object to check on
     * @return true if it is a special element
     */
    public static boolean isHybridQuerySpecialElement(final ScoreDoc scoreDoc) {
        if (Objects.isNull(scoreDoc)) {
            return false;
        }
        return isHybridQueryStartStopElement(scoreDoc) || isHybridQueryDelimiterElement(scoreDoc);
    }

    /**
     * Checking if passed scoreDocs object is a document score element
     * @param scoreDoc score doc object to check on
     * @return true if element has score
     */
    public static boolean isHybridQueryScoreDocElement(final ScoreDoc scoreDoc) {
        if (Objects.isNull(scoreDoc)) {
            return false;
        }
        return !isHybridQuerySpecialElement(scoreDoc);
    }

    /**
     * This method is for creating dummy sort object for the field docs having magic number scores which acts as delimiters.
     *  The sort object should be in the same type of the field on which sorting criteria is applied.
     * @param fields contains the information about the object type of the field on which sorting criteria is applied
     * @return
     */
    public static Object[] createSortFieldsForDelimiterResults(final Object[] fields) {
        final Object[] sortFields = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            SortField sortField = (SortField) fields[i];
            SortField.Type type = sortField.getType();
            if (sortField instanceof SortedNumericSortField) {
                type = ((SortedNumericSortField) sortField).getNumericType();
            }
            // Example: Lets consider there are 2 sort fields on which the sort criteria has to be applied.
            // <docId, MAGIC_NUMBER_START_START, ShardId, [1,1]>
            // <docId, MAGIC_NUMBER_DELIMITER, ShardId, [1,1]>
            // ...
            // <docId, MAGIC_NUMBER_DELIMITER, ShardId, [1,1]>
            // <docId, MAGIC_NUMBER_START_STOP, ShardId, [1,1]> `
            Object SORT_FIELDS_FOR_DELIMITER_RESULTS;
            switch (type) {
                case DOC:
                case INT:
                    SORT_FIELDS_FOR_DELIMITER_RESULTS = 1;
                    break;
                case LONG:
                    SORT_FIELDS_FOR_DELIMITER_RESULTS = 1L;
                    break;
                case SCORE:
                case FLOAT:
                    SORT_FIELDS_FOR_DELIMITER_RESULTS = 1.0f;
                    break;
                case DOUBLE:
                    SORT_FIELDS_FOR_DELIMITER_RESULTS = 1.0;
                    break;
                default:
                    SORT_FIELDS_FOR_DELIMITER_RESULTS = new BytesRef();
            }

            sortFields[i] = SORT_FIELDS_FOR_DELIMITER_RESULTS;
        }
        return sortFields;
    }
}
