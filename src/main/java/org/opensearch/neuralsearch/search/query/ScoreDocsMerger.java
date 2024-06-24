/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import com.google.common.annotations.VisibleForTesting;
import lombok.NoArgsConstructor;
import org.apache.lucene.search.ScoreDoc;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQueryScoreDocElement;

/**
 * Merges two ScoreDocs arrays into one
 */
@NoArgsConstructor
public class ScoreDocsMerger {

    private static final int MIN_NUMBER_OF_ELEMENTS_IN_SCORE_DOC = 3;

    /**
     * Merge two score docs objects, result ScoreDocs[] object will have all hits per sub-query from both original objects.
     * Logic is based on assumption that hits of every sub-query are sorted by score.
     * Method returns new object and doesn't mutate original ScoreDocs arrays.
     * @param sourceScoreDocs original score docs from query result
     * @param newScoreDocs new score docs that we need to merge into existing scores
     * @return merged array of ScoreDocs objects
     */
    @VisibleForTesting
    protected ScoreDoc[] mergedScoreDocs(
        final ScoreDoc[] sourceScoreDocs,
        final ScoreDoc[] newScoreDocs,
        final Comparator<ScoreDoc> scoreDocComparator
    ) {
        if (Objects.requireNonNull(sourceScoreDocs, "score docs cannot be null").length < MIN_NUMBER_OF_ELEMENTS_IN_SCORE_DOC
            || Objects.requireNonNull(newScoreDocs, "score docs cannot be null").length < MIN_NUMBER_OF_ELEMENTS_IN_SCORE_DOC) {
            throw new IllegalArgumentException("cannot merge top docs because it does not have enough elements");
        }
        // we overshoot and preallocate more than we need - length of both top docs combined.
        // we will take only portion of the array at the end
        List<ScoreDoc> mergedScoreDocs = new ArrayList<>(sourceScoreDocs.length + newScoreDocs.length);
        int sourcePointer = 0;
        // mark beginning of hybrid query results by start element
        mergedScoreDocs.add(sourceScoreDocs[sourcePointer]);
        sourcePointer++;
        // new pointer is set to 1 as we don't care about it start-stop element
        int newPointer = 1;

        while (sourcePointer < sourceScoreDocs.length - 1 && newPointer < newScoreDocs.length - 1) {
            // every iteration is for results of one sub-query
            mergedScoreDocs.add(sourceScoreDocs[sourcePointer]);
            sourcePointer++;
            newPointer++;
            // simplest case when both arrays have results for sub-query
            while (sourcePointer < sourceScoreDocs.length
                && isHybridQueryScoreDocElement(sourceScoreDocs[sourcePointer])
                && newPointer < newScoreDocs.length
                && isHybridQueryScoreDocElement(newScoreDocs[newPointer])) {
                if (scoreDocComparator.compare(sourceScoreDocs[sourcePointer], newScoreDocs[newPointer]) >= 0) {
                    mergedScoreDocs.add(sourceScoreDocs[sourcePointer]);
                    sourcePointer++;
                } else {
                    mergedScoreDocs.add(newScoreDocs[newPointer]);
                    newPointer++;
                }
            }
            // at least one object got exhausted at this point, now merge all elements from object that's left
            while (sourcePointer < sourceScoreDocs.length && isHybridQueryScoreDocElement(sourceScoreDocs[sourcePointer])) {
                mergedScoreDocs.add(sourceScoreDocs[sourcePointer]);
                sourcePointer++;
            }
            while (newPointer < newScoreDocs.length && isHybridQueryScoreDocElement(newScoreDocs[newPointer])) {
                mergedScoreDocs.add(newScoreDocs[newPointer]);
                newPointer++;
            }
        }
        // mark end of hybrid query results by end element
        mergedScoreDocs.add(sourceScoreDocs[sourceScoreDocs.length - 1]);
        return mergedScoreDocs.toArray(new ScoreDoc[0]);
    }
}
