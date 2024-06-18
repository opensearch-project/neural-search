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
    public static final Float MAX_SCORE_WHEN_NO_HITS_FOUND = 0.0f;

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

    /**
     * This method is for creating dummy sort object for the field docs having magic number scores which acts as delimiters.
     *  The sort object should be in the same type of the field on which sorting criteria is applied.
     * @param fields contains the information about the object type of the field on which sorting criteria is applied
     * @return
     */
    public static Object[] createSortFieldsForDelimiterResults(final Object[] fields) {
        Object SORT_FIELDS_FOR_DELIMITER_RESULTS;
        final Object[] sortFields = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            SortField sortField = (SortField) fields[i];
            SortField.Type type = sortField.getType();
            if (sortField instanceof SortedNumericSortField) {
                type = ((SortedNumericSortField) sortField).getNumericType();
            }
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
