/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.lucene;

import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;

/*
MultiLeafFieldComparator holds information when sort criteria is applied on more that one field.
This class is same as of lucene. Because lucene implementation does not have public access we have to add it in neural search plugin.
https://github.com/apache/lucene/blob/main/lucene/core/src/java/org/apache/lucene/search/MultiLeafFieldComparator.java
 */
public final class MultiLeafFieldComparator implements LeafFieldComparator {

    private final LeafFieldComparator[] comparators;
    private final int[] reverseMul;
    // we extract the first comparator to avoid array access in the common case
    // that the first comparator compares worse than the bottom entry in the queue
    private final LeafFieldComparator firstComparator;
    private final int firstReverseMul;

    public MultiLeafFieldComparator(LeafFieldComparator[] comparators, int[] reverseMul) {
        if (comparators.length != reverseMul.length) {
            throw new IllegalArgumentException(
                "Must have the same number of comparators and reverseMul, got " + comparators.length + " and " + reverseMul.length
            );
        }
        this.comparators = comparators;
        this.reverseMul = reverseMul;
        this.firstComparator = comparators[0];
        this.firstReverseMul = reverseMul[0];
    }

    @Override
    public void setBottom(int slot) throws IOException {
        for (LeafFieldComparator comparator : comparators) {
            comparator.setBottom(slot);
        }
    }

    @Override
    public int compareBottom(int doc) throws IOException {
        // Compare the first comparator's result with reverse multiplier
        int comparison = firstReverseMul * firstComparator.compareBottom(doc);
        if (comparison != 0) {
            return comparison;
        }
        // Loop through remaining comparators and compare
        for (int i = 1; i < comparators.length; ++i) {
            comparison = reverseMul[i] * comparators[i].compareBottom(doc);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    @Override
    public int compareTop(int doc) throws IOException {
        // Compare the first comparator's result with reverse multiplier
        int comparison = firstReverseMul * firstComparator.compareTop(doc);
        if (comparison != 0) {
            return comparison;
        }
        for (int i = 1; i < comparators.length; ++i) {
            comparison = reverseMul[i] * comparators[i].compareTop(doc);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
        for (LeafFieldComparator comparator : comparators) {
            comparator.copy(slot, doc);
        }
    }

    @Override
    public void setScorer(final Scorable scorer) throws IOException {
        for (LeafFieldComparator comparator : comparators) {
            comparator.setScorer(scorer);
        }
    }

    @Override
    public void setHitsThresholdReached() throws IOException {
        // this is needed for skipping functionality that is only relevant for the 1st comparator
        firstComparator.setHitsThresholdReached();
    }

    @Override
    public DocIdSetIterator competitiveIterator() throws IOException {
        // this is needed for skipping functionality that is only relevant for the 1st comparator
        return firstComparator.competitiveIterator();
    }
}
