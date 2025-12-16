/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQueryScoreDocElement;

/**
 * Merges two ScoreDoc arrays into one
 */
@NoArgsConstructor(access = AccessLevel.PACKAGE)
class HybridQueryScoreDocsMerger<T extends ScoreDoc> {

    private static final int MIN_NUMBER_OF_ELEMENTS_IN_SCORE_DOC = 3;

    /**
     * Merge two score docs objects and collapseValues if collapse is enabled, result MergeResult will have ScoreDocs[] object will have all the hits per sub-query from both original objects.
     * It will also have collapseValues[] object for all scoreDocs[].
     * Input and output ScoreDocs are in format that is specific to Hybrid Query. This method should not be used for ScoreDocs from
     * other query types.
     * Logic is based on assumption that hits of every sub-query are sorted by score.
     * Method returns new object and doesn't mutate original ScoreDocs arrays.
     * @param sourceScoreDocs original score docs from query result
     * @param newScoreDocs new score docs that we need to merge into existing scores
     * @param comparator comparator to compare the score docs
     * @param sourceCollapseValues source collapse values
     * @param newCollapseValues new collapse values
     * @param isSortEnabled flag that show if sort is enabled or disabled
     * @param isCollapseEnabled flag that show if collapse is enabled or disabled
     * @return merged array of ScoreDocs objects
     */
    public MergeResult<T> mergeScoreDocsAndCollapseValues(
        final T[] sourceScoreDocs,
        final T[] newScoreDocs,
        final Comparator<T> comparator,
        final Object[] sourceCollapseValues,
        final Object[] newCollapseValues,
        final boolean isSortEnabled,
        final boolean isCollapseEnabled
    ) {
        // The length of sourceScoreDocs or newScoreDocs can be 0 in the following conditions
        // 1. When concurrent segment search is enabled then there can be multiple collector instances that can have search results.
        // 2. The total hits count of every collector instance represent the actual count of search results present in the shard
        // irrespective of pagination.
        // 3. If the PagingFieldCollector removes the search results as per `search_after` criteria and totalHits added in the collector is
        // greater than 0,
        // then the newTopFieldDocs method in the HybridCollectorManager will set the fieldDocs as TopFieldDocs(totalHits, new FieldDoc[0],
        // sortFields).
        // In this case the size of fieldDocs is 0 with no delimiters.
        if (Objects.requireNonNull(sourceScoreDocs, "score docs cannot be null").length == 0) {
            return new MergeResult<>(newScoreDocs, newCollapseValues);
        }
        if (Objects.requireNonNull(newScoreDocs, "score docs cannot be null").length == 0) {
            return new MergeResult<>(sourceScoreDocs, sourceCollapseValues);
        }
        if (sourceScoreDocs.length < MIN_NUMBER_OF_ELEMENTS_IN_SCORE_DOC || newScoreDocs.length < MIN_NUMBER_OF_ELEMENTS_IN_SCORE_DOC) {
            throw new IllegalArgumentException("cannot merge top docs because it does not have enough elements");
        }

        if (isCollapseEnabled) {
            if (sourceScoreDocs.length != Objects.requireNonNull(sourceCollapseValues, "collapse values cannot be null").length) {
                throw new IllegalArgumentException(
                    "cannot merge collapse values because the number of elements does not match score docs count"
                );
            }

            if (newScoreDocs.length != Objects.requireNonNull(newCollapseValues, "collapse values cannot be null").length) {
                throw new IllegalArgumentException(
                    "cannot merge collapse values because the number of elements does not match score docs coun"
                );
            }
        }

        // we overshoot and preallocate more than we need - length of both top docs combined.
        // we will take only portion of the array at the end
        List<T> mergedScoreDocs = new ArrayList<>(sourceScoreDocs.length + newScoreDocs.length);
        List<Object> mergedCollapseValues = null;
        if (isCollapseEnabled) {
            mergedCollapseValues = new ArrayList<>(sourceCollapseValues.length + newCollapseValues.length);
        }

        int sourcePointer = 0;
        // mark beginning of hybrid query results by start element
        mergedScoreDocs.add(sourceScoreDocs[sourcePointer]);
        if (isCollapseEnabled) {
            mergedCollapseValues.add(sourceCollapseValues[sourcePointer]);
        }
        sourcePointer++;
        // new pointer is set to 1 as we don't care about it start-stop element
        int newPointer = 1;

        while (sourcePointer < sourceScoreDocs.length - 1 && newPointer < newScoreDocs.length - 1) {
            // every iteration is for results of one sub-query
            mergedScoreDocs.add(sourceScoreDocs[sourcePointer]);
            if (isCollapseEnabled) {
                mergedCollapseValues.add(sourceCollapseValues[sourcePointer]);
            }
            sourcePointer++;
            newPointer++;
            // simplest case when both arrays have results for sub-query
            while (sourcePointer < sourceScoreDocs.length
                && isHybridQueryScoreDocElement(sourceScoreDocs[sourcePointer])
                && newPointer < newScoreDocs.length
                && isHybridQueryScoreDocElement(newScoreDocs[newPointer])) {
                if (compareCondition(sourceScoreDocs[sourcePointer], newScoreDocs[newPointer], comparator, isSortEnabled)) {
                    mergedScoreDocs.add(sourceScoreDocs[sourcePointer]);
                    if (isCollapseEnabled) {
                        mergedCollapseValues.add(sourceCollapseValues[sourcePointer]);
                    }
                    sourcePointer++;
                } else {
                    mergedScoreDocs.add(newScoreDocs[newPointer]);
                    if (isCollapseEnabled) {
                        mergedCollapseValues.add(newCollapseValues[newPointer]);
                    }
                    newPointer++;
                }
            }
            // at least one object got exhausted at this point, now merge all elements from object that's left
            while (sourcePointer < sourceScoreDocs.length && isHybridQueryScoreDocElement(sourceScoreDocs[sourcePointer])) {
                mergedScoreDocs.add(sourceScoreDocs[sourcePointer]);
                if (isCollapseEnabled) {
                    mergedCollapseValues.add(sourceCollapseValues[sourcePointer]);
                }
                sourcePointer++;
            }
            while (newPointer < newScoreDocs.length && isHybridQueryScoreDocElement(newScoreDocs[newPointer])) {
                mergedScoreDocs.add(newScoreDocs[newPointer]);
                if (isCollapseEnabled) {
                    mergedCollapseValues.add(newCollapseValues[newPointer]);
                }
                newPointer++;
            }
        }
        // mark end of hybrid query results by end element
        mergedScoreDocs.add(sourceScoreDocs[sourceScoreDocs.length - 1]);
        if (isCollapseEnabled) {
            mergedCollapseValues.add(sourceScoreDocs[sourceScoreDocs.length - 1]);
        }
        if (isCollapseEnabled) {
            return new MergeResult<>(mergedScoreDocs.toArray((T[]) new FieldDoc[0]), mergedCollapseValues.toArray(new Object[0]));
        } else if (isSortEnabled) {
            return new MergeResult<>(mergedScoreDocs.toArray((T[]) new FieldDoc[0]), null);
        }
        return new MergeResult<>(mergedScoreDocs.toArray((T[]) new ScoreDoc[0]), null);
    }

    private boolean compareCondition(
        final ScoreDoc oldScoreDoc,
        final ScoreDoc secondScoreDoc,
        final Comparator<T> comparator,
        final boolean isSortEnabled
    ) {
        // If sorting is enabled then compare condition will be different then normal HybridQuery
        if (isSortEnabled) {
            return comparator.compare((T) oldScoreDoc, (T) secondScoreDoc) < 0;
        } else {
            return comparator.compare((T) oldScoreDoc, (T) secondScoreDoc) >= 0;
        }
    }

    public record MergeResult<T extends ScoreDoc>(T[] scoreDocs, Object[] collapseValues) {
    }
}
