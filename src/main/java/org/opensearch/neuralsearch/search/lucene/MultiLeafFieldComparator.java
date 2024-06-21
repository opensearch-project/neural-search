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
HybridMultiLeafFieldComparator holds information when sort criteria is applied on more that one field.
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

    // Set the bottom slot, ie the "weakest" (sorted last) entry in the queue. When compareBottom is called, you should compare against this
    // slot. This will always be called before compareBottom.
    // Params
    // slot – the currently weakest (sorted last) slot in the queue
    @Override
    public void setBottom(int slot) throws IOException {
        for (LeafFieldComparator comparator : comparators) {
            comparator.setBottom(slot);
        }
    }

    // Compare the bottom of the queue with this doc. This will only invoked after setBottom has been called. This should return the same
    // result as FieldComparator.compare(int, int)} as if bottom were slot1 and the new document were slot 2.
    // For a search that hits many results, this method will be the hotspot (invoked by far the most frequently).
    // Params:
    // doc – that was hit
    // Returns:
    // any N < 0 if the doc's value is sorted after the bottom entry (not competitive),
    // any N > 0 if the doc's value is sorted before the bottom entry and 0 if they are equal.
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

    // Compare the top value with this doc. This will only invoked after setTopValue has been called.
    // This should return the same result as FieldComparator.compare(int, int)} as if topValue were slot1 and the new document were slot 2.
    // This is only called for searches that use searchAfter (deep paging).
    // Params:
    // doc – that was hit
    // Returns:
    // any N < 0 if the doc's value is sorted after the top entry (not competitive),
    // any N > 0 if the doc's value is sorted before the top entry and 0 if they are equal.
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

    // This method is called when a new hit is competitive.
    // You should copy any state associated with this document that will be required for future comparisons, into the specified slot.
    // Params:
    // slot – which slot to copy the hit to doc – docID relative to current reader
    @Override
    public void copy(int slot, int doc) throws IOException {
        for (LeafFieldComparator comparator : comparators) {
            comparator.copy(slot, doc);
        }
    }

    // Sets the Scorer to use in case a document's score is needed.
    // Params:
    // scorer – Scorer instance that you should use to obtain the current hit's score, if necessary.
    @Override
    public void setScorer(final Scorable scorer) throws IOException {
        for (LeafFieldComparator comparator : comparators) {
            comparator.setScorer(scorer);
        }
    }

    // nforms this leaf comparator that hits threshold is reached.
    // This method is called from a collector when hits threshold is reached.
    @Override
    public void setHitsThresholdReached() throws IOException {
        // this is needed for skipping functionality that is only relevant for the 1st comparator
        firstComparator.setHitsThresholdReached();
    }

    // Returns a competitive iterator
    // Returns:
    // an iterator over competitive docs that are stronger than already collected docs
    // or null if such an iterator is not available for the current comparator or segment.
    @Override
    public DocIdSetIterator competitiveIterator() throws IOException {
        // this is needed for skipping functionality that is only relevant for the 1st comparator
        return firstComparator.competitiveIterator();
    }
}
