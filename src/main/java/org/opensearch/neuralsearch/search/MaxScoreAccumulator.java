/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import java.util.Objects;
import java.util.concurrent.atomic.LongAccumulator;

final class MaxScoreAccumulator {
    // we use 2^10-1 to check the remainder with a bitwise operation
    static final int DEFAULT_INTERVAL = 0x3ff;

    // scores are always positive
    final LongAccumulator acc = new LongAccumulator(MaxScoreAccumulator::maxEncode, Long.MIN_VALUE);

    // non-final and visible for tests
    long modInterval;

    MaxScoreAccumulator() {
        this.modInterval = DEFAULT_INTERVAL;
    }

    /**
     * Return the max encoded DocAndScore in a way that is consistent with {@link
     * MaxScoreAccumulator.DocAndScore#compareTo}.
     */
    private static long maxEncode(long v1, long v2) {
        float score1 = Float.intBitsToFloat((int) (v1 >> 32));
        float score2 = Float.intBitsToFloat((int) (v2 >> 32));
        int cmp = Float.compare(score1, score2);
        if (cmp == 0) {
            // tie-break on the minimum doc base
            return (int) v1 < (int) v2 ? v1 : v2;
        } else if (cmp > 0) {
            return v1;
        }
        return v2;
    }

    void accumulate(int docBase, float score) {
        assert docBase >= 0 && score >= 0;
        long encode = (((long) Float.floatToIntBits(score)) << 32) | docBase;
        acc.accumulate(encode);
    }

    MaxScoreAccumulator.DocAndScore get() {
        long value = acc.get();
        if (value == Long.MIN_VALUE) {
            return null;
        }
        float score = Float.intBitsToFloat((int) (value >> 32));
        int docBase = (int) value;
        return new MaxScoreAccumulator.DocAndScore(docBase, score);
    }

    static class DocAndScore implements Comparable<MaxScoreAccumulator.DocAndScore> {
        final int docBase;
        final float score;

        DocAndScore(int docBase, float score) {
            this.docBase = docBase;
            this.score = score;
        }

        @Override
        public int compareTo(MaxScoreAccumulator.DocAndScore o) {
            int cmp = Float.compare(score, o.score);
            if (cmp == 0) {
                // tie-break on the minimum doc base
                // For a given minimum competitive score, we want to know the first segment
                // where this score occurred, hence the reverse order here.
                // On segments with a lower docBase, any document whose score is greater
                // than or equal to this score would be competitive, while on segments with a
                // higher docBase, documents need to have a strictly greater score to be
                // competitive since we tie break on doc ID.
                return Integer.compare(o.docBase, docBase);
            }
            return cmp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MaxScoreAccumulator.DocAndScore result = (MaxScoreAccumulator.DocAndScore) o;
            return docBase == result.docBase && Float.compare(result.score, score) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(docBase, score);
        }

        @Override
        public String toString() {
            return "DocAndScore{" + "docBase=" + docBase + ", score=" + score + '}';
        }
    }
}
