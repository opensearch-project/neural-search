/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.lucene.search.CheckedIntConsumer;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.Objects;

/**
 * This class is used to create a DocIdStream for HybridQuery
 */
@RequiredArgsConstructor
public class HybridQueryDocIdStream extends DocIdStream {
    private static final int BLOCK_SHIFT = 6;
    private final HybridBulkScorer hybridBulkScorer;
    private FixedBitSet localMatchingBitSet;
    @Setter
    private int base;

    @Override
    public boolean mayHaveRemaining() {
        return getLocalMatchingBitSet().cardinality() > 0;
    }

    @Override
    public int count(int upTo) {
        // Use a counter to track how many documents are processed
        final int[] count = { 0 };
        try {
            forEach(upTo, docId -> count[0]++);
        } catch (IOException e) {
            // This should not happen as we're just counting, not doing any I/O
            throw new RuntimeException(e);
        }
        return count[0];
    }

    @Override
    public void forEach(int upTo, CheckedIntConsumer<IOException> consumer) throws IOException {
        // bitset that represents matching documents, bit is set (1) if doc id is a match
        FixedBitSet matchingBitSet = getLocalMatchingBitSet();
        long[] bitArray = matchingBitSet.getBits();
        // iterate through each block of 64 documents (since each long contains 64 bits)
        for (int idx = 0; idx < bitArray.length; idx++) {
            long bits = bitArray[idx];
            while (bits != 0L) {
                // find position of the rightmost set bit (1)
                int numberOfTrailingZeros = Long.numberOfTrailingZeros(bits);
                // calculate actual document ID within the window
                // idx << 6 is equivalent to idx * 64 (block offset)
                // numberOfTrailingZeros gives position within the block
                final int docIndexInWindow = (idx << BLOCK_SHIFT) | numberOfTrailingZeros;
                final int docId = base | docIndexInWindow;

                // Only process documents up to the specified limit
                if (docId >= upTo) {
                    return;
                }

                float[][] windowScores = hybridBulkScorer.getWindowScores();
                for (int subQueryIndex = 0; subQueryIndex < windowScores.length; subQueryIndex++) {
                    if (Objects.isNull(windowScores[subQueryIndex])) {
                        continue;
                    }
                    float scoreOfDocIdForSubQuery = windowScores[subQueryIndex][docIndexInWindow];
                    hybridBulkScorer.getHybridSubQueryScorer().getSubQueryScores()[subQueryIndex] = scoreOfDocIdForSubQuery;
                }
                // process the document with its base offset
                consumer.accept(docId);
                // reset scores after processing of one doc, this is required because scorer object is re-used
                hybridBulkScorer.getHybridSubQueryScorer().resetScores();
                // clear bit from the local bitset copy to indicate that it has been consumed
                matchingBitSet.clear(docIndexInWindow);
                // reset bit for this doc id to indicate that it has been consumed
                bits ^= 1L << numberOfTrailingZeros;
            }
        }
    }

    // This lazy loading is necessary because the matching bit isn't available at the time this class is constructed.
    private FixedBitSet getLocalMatchingBitSet() {
        if (localMatchingBitSet == null) {
            localMatchingBitSet = hybridBulkScorer.getMatching().clone();
        }
        return localMatchingBitSet;
    }
}
