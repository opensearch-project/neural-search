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
    public boolean mayHaveRemaining() {
        return false;
    }

    // This class does not respect the upTo value; it consumes all matching documents.
    @Override
    public void forEach(int upTo, CheckedIntConsumer<IOException> consumer) throws IOException {
        // Always get the current matching bitset from the bulk scorer
        FixedBitSet matchingBitSet = hybridBulkScorer.getMatching();
        long[] bitArray = matchingBitSet.getBits();

        for (int idx = 0; idx < bitArray.length; idx++) {
            long bits = bitArray[idx];
            while (bits != 0L) {
                int numberOfTrailingZeros = Long.numberOfTrailingZeros(bits);
                final int docIndexInWindow = (idx << BLOCK_SHIFT) | numberOfTrailingZeros;
                final int docId = base | docIndexInWindow;

                float[][] windowScores = hybridBulkScorer.getWindowScores();
                for (int subQueryIndex = 0; subQueryIndex < windowScores.length; subQueryIndex++) {
                    if (Objects.isNull(windowScores[subQueryIndex])) {
                        continue;
                    }
                    float scoreOfDocIdForSubQuery = windowScores[subQueryIndex][docIndexInWindow];
                    hybridBulkScorer.getHybridSubQueryScorer().getSubQueryScores()[subQueryIndex] = scoreOfDocIdForSubQuery;
                }
                consumer.accept(docId);
                hybridBulkScorer.getHybridSubQueryScorer().resetScores();

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
