/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.util;

import java.util.List;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.sort.SortedWiderNumericSortField;

/**
 * Utility class for evaluating and creating sort criteria
 */
public class HybridSearchSortUtil {

    public static Sort evaluateSortCriteria(final List<QuerySearchResult> querySearchResults, final List<CompoundTopDocs> queryTopDocs) {
        if (!checkIfSortEnabled(querySearchResults)) {
            return null;
        }
        return createSort(getTopFieldDocs(queryTopDocs));
    }

    // Check if sort is enabled by checking docValueFormats Object
    private static boolean checkIfSortEnabled(final List<QuerySearchResult> querySearchResults) {
        if (querySearchResults == null || querySearchResults.isEmpty() || querySearchResults.get(0) == null) {
            throw new IllegalArgumentException("shard result cannot be null in the normalization process.");
        }
        return querySearchResults.get(0).sortValueFormats() != null;
    }

    // Get the topFieldDocs array from the first shard result
    private static TopFieldDocs[] getTopFieldDocs(final List<CompoundTopDocs> queryTopDocs) {
        // loop over queryTopDocs and return the first set of topFieldDocs found
        // Considering the topDocs can be empty if no result is found on the shard therefore we need iterate over all the shards .
        for (CompoundTopDocs compoundTopDocs : queryTopDocs) {
            if (compoundTopDocs == null) {
                throw new IllegalArgumentException("CompoundTopDocs cannot be null in the normalization process");
            }
            if (checkIfTopFieldDocExists(compoundTopDocs.getTopDocs())) {
                return compoundTopDocs.getTopDocs().toArray(new TopFieldDocs[0]);
            }
        }
        return new TopFieldDocs[0];
    }

    private static boolean checkIfTopFieldDocExists(List<TopDocs> topDocs) {
        // topDocs can be empty if no results found in the shard
        if (topDocs == null || topDocs.isEmpty()) {
            return false;
        }
        for (TopDocs topDoc : topDocs) {
            if (topDoc != null && topDoc instanceof TopFieldDocs) {
                return true;
            }
        }
        return false;
    }

    /**
     * Creates a Sort object based on the provided top field documents.
     * This method takes an array of top field documents and processes each field to create a corresponding SortField.
     * The created  SortField objects are then used to create and return a new Sort object.
     * @param topFieldDocs array of top field documents need to be processed.
     * @return Sort object created based on provided top field documents.
     */
    private static Sort createSort(TopFieldDocs[] topFieldDocs) {
        if (topFieldDocs == null || topFieldDocs[0] == null) {
            throw new IllegalArgumentException("topFieldDocs cannot be null when sorting is applied");
        }
        final SortField[] firstTopDocFields = topFieldDocs[0].fields;
        final SortField[] newFields = new SortField[firstTopDocFields.length];

        for (int i = 0; i < firstTopDocFields.length; i++) {
            final SortField delegate = firstTopDocFields[i];
            final SortField.Type type = delegate instanceof SortedNumericSortField
                ? ((SortedNumericSortField) delegate).getNumericType()
                : delegate.getType();

            if (SortedWiderNumericSortField.isTypeSupported(type) && isSortWideningRequired(topFieldDocs, i)) {
                newFields[i] = new SortedWiderNumericSortField(delegate.getField(), type, delegate.getReverse());
            } else {
                newFields[i] = firstTopDocFields[i];
            }
        }
        return new Sort(newFields);
    }

    /**
     * Checks if sort widening is required for the provided top field documents.
     *
     * This method iterates through the provided topFieldDocs array and checks if any adjacent pairs of sort fields at the specified index are not equal.
     * If any such pair is found, it returns true, indicating that sort widening is required. Otherwise, it returns false.
     * @param topFieldDocs array of top field documents to be checked.
     * @param sortFieldindex index of the sort field to be checked within each top field document.
     * @return true if sort widening is required, false otherwise.
     */
    private static boolean isSortWideningRequired(TopFieldDocs[] topFieldDocs, int sortFieldindex) {
        for (int i = 0; i < topFieldDocs.length - 1; i++) {
            TopFieldDocs currentTopFieldDoc = topFieldDocs[i];
            TopFieldDocs nextTopFieldDoc = topFieldDocs[i + 1];
            if (currentTopFieldDoc == null || nextTopFieldDoc == null) {
                throw new IllegalArgumentException("topFieldDocs cannot be null when sorting is applied");
            }
            if (!currentTopFieldDoc.fields[sortFieldindex].equals(nextTopFieldDoc.fields[sortFieldindex])) {
                return true;
            }
        }
        return false;
    }
}
