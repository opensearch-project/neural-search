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
    @Setter
    private int base;

    /**
     * Iterate over all doc ids and collect each doc id with leaf collector
     * @param consumer consumer that is called for each accepted doc id
     * @throws IOException in case of IO exception
     */
    @Override
    public void forEach(CheckedIntConsumer<IOException> consumer) throws IOException {
        // bitset that represents matching documents, bit is set (1) if doc id is a match
        FixedBitSet matchingBitSet = hybridBulkScorer.getMatching();
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
                float[][] windowScores = hybridBulkScorer.getWindowScores();
                for (int subQueryIndex = 0; subQueryIndex < windowScores.length; subQueryIndex++) {
                    if (Objects.isNull(windowScores[subQueryIndex])) {
                        continue;
                    }
                    float scoreOfDocIdForSubQuery = windowScores[subQueryIndex][docIndexInWindow];
                    hybridBulkScorer.getHybridSubQueryScorer().getSubQueryScores()[subQueryIndex] = scoreOfDocIdForSubQuery;
                }
                // process the document with its base offset
                consumer.accept(base | docIndexInWindow);
                // reset scores after processing of one doc, this is required because scorer object is re-used
                hybridBulkScorer.getHybridSubQueryScorer().resetScores();
                // reset bit for this doc id to indicate that it has been consumed
                bits ^= 1L << numberOfTrailingZeros;
            }
        }
    }
}
