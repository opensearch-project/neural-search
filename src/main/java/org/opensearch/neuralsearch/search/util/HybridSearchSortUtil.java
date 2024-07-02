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

    /**
     * @param querySearchResults list of query search results where each search result represents a result from the shard.
     * @param queryTopDocs list of top docs which have results with top scores.
     * @return sort criteria
     */
    public static Sort evaluateSortCriteria(final List<QuerySearchResult> querySearchResults, final List<CompoundTopDocs> queryTopDocs) {
        if (!checkIfSortEnabled(querySearchResults)) {
            return null;
        }
        return createSort(getTopFieldDocs(queryTopDocs));
    }

    // Check if sort is enabled by checking docValueFormats Object
    private static boolean checkIfSortEnabled(final List<QuerySearchResult> querySearchResults) {
        if (querySearchResults == null || querySearchResults.isEmpty() || querySearchResults.get(0) == null) {
            throw new IllegalArgumentException("shard results cannot be null in the normalization process.");
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
            if (containsTopFieldDocs(compoundTopDocs.getTopDocs())) {
                return compoundTopDocs.getTopDocs().toArray(new TopFieldDocs[0]);
            }
        }
        return new TopFieldDocs[0];
    }

    private static boolean containsTopFieldDocs(List<TopDocs> topDocs) {
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
     * Creates Sort object from topFieldsDocs fields.
     * It is necessary to widen the SortField.Type to maximum byte size for merging sorted docs.
     * Different indices might have different types. This will avoid user to do re-index of data
     * in case of mapping field change for newly indexed data.
     * This will support Int to Long and Float to Double.
     * Earlier widening of type was taken care in IndexNumericFieldData, but since we now want to
     * support sort optimization, we removed type widening there and taking care here during merging.
     * More details here https://github.com/opensearch-project/OpenSearch/issues/6326
     */
    private static Sort createSort(TopFieldDocs[] topFieldDocs) {
        final SortField[] firstTopDocFields = topFieldDocs[0].fields;
        final SortField[] newFields = new SortField[firstTopDocFields.length];

        for (int i = 0; i < firstTopDocFields.length; i++) {
            final SortField delegate = firstTopDocFields[i];
            final SortField.Type sortFieldType = delegate instanceof SortedNumericSortField
                ? ((SortedNumericSortField) delegate).getNumericType()
                : delegate.getType();

            if (SortedWiderNumericSortField.isTypeSupported(sortFieldType) && isSortWideningRequired(topFieldDocs, i)) {
                newFields[i] = new SortedWiderNumericSortField(delegate.getField(), sortFieldType, delegate.getReverse());
            } else {
                newFields[i] = firstTopDocFields[i];
            }
        }
        return new Sort(newFields);
    }

    /**
     * It will compare respective SortField between shards to see if any shard results have different
     * field mapping type, accordingly it will decide to widen the sort fields.
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
