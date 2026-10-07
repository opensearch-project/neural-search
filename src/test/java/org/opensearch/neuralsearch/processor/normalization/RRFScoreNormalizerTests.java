/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Tests the shared RRF arithmetic in isolation from the {@link org.opensearch.neuralsearch.processor.CompoundTopDocs}
 * traversal that {@link RRFNormalizationTechnique} wraps around it.
 */
public class RRFScoreNormalizerTests extends OpenSearchTestCase {
    private static final float DELTA = 1e-6f;

    public void testScoreForRank_whenDefaultRankConstant_thenReciprocalOfOneBasedRank() {
        assertEquals(1.0f / 61.0f, RRFScoreNormalizer.scoreForRank(0, RRFScoreNormalizer.DEFAULT_RANK_CONSTANT), DELTA);
        assertEquals(1.0f / 62.0f, RRFScoreNormalizer.scoreForRank(1, RRFScoreNormalizer.DEFAULT_RANK_CONSTANT), DELTA);
        assertEquals(1.0f / 70.0f, RRFScoreNormalizer.scoreForRank(9, RRFScoreNormalizer.DEFAULT_RANK_CONSTANT), DELTA);
    }

    public void testScoreForRank_whenCustomRankConstant_thenUsesIt() {
        assertEquals(1.0f / 26.0f, RRFScoreNormalizer.scoreForRank(0, 25), DELTA);
        assertEquals(1.0f / 2.0f, RRFScoreNormalizer.scoreForRank(0, 1), DELTA);
    }

    public void testScoreForRank_whenScoresDecrease_thenStrictlyMonotonic() {
        float previous = Float.MAX_VALUE;
        for (int rank = 0; rank < 100; rank++) {
            float score = RRFScoreNormalizer.scoreForRank(rank, RRFScoreNormalizer.DEFAULT_RANK_CONSTANT);
            assertTrue("score must decrease as rank grows", score < previous);
            previous = score;
        }
    }

    /**
     * The score is computed in integer arithmetic rather than with {@link BigDecimal}. That is only a safe
     * substitution if it is exactly equal to the BigDecimal form it replaced, so assert on raw float bits rather
     * than within a delta - a change that rounded even one ULP differently would pass a delta comparison. The
     * denominators covered span the point where a scale-10 numerator crosses 2^24, where BigDecimal's conversion
     * to float changes behavior, and the point past which scale 10 is coarser than float itself.
     */
    public void testScoreForRank_whenComputed_thenBitIdenticalToBigDecimalReference() {
        for (int rankConstant : new int[] {
            RRFScoreNormalizer.MIN_RANK_CONSTANT,
            RRFScoreNormalizer.DEFAULT_RANK_CONSTANT,
            RRFScoreNormalizer.MAX_RANK_CONSTANT }) {
            for (int rank = 0; rank <= 5000; rank++) {
                assertEquals(
                    "rank_constant [" + rankConstant + "], rank [" + rank + "]",
                    Float.floatToIntBits(bigDecimalScoreForRank(rank, rankConstant)),
                    Float.floatToIntBits(RRFScoreNormalizer.scoreForRank(rank, rankConstant))
                );
            }
        }
    }

    /**
     * The BigDecimal implementation that {@link RRFScoreNormalizer#scoreForRank} replaced, retained as an
     * independent reference for what the rank score must be. Deliberately does not delegate to the production
     * code: an oracle that calls the implementation cannot detect a change in the implementation.
     */
    private static float bigDecimalScoreForRank(final int rank, final int rankConstant) {
        return BigDecimal.ONE.divide(BigDecimal.valueOf(rankConstant + rank + 1), 10, RoundingMode.HALF_UP).floatValue();
    }

    public void testResolveRankConstant_whenParamAbsent_thenDefault() {
        assertEquals(RRFScoreNormalizer.DEFAULT_RANK_CONSTANT, RRFScoreNormalizer.resolveRankConstant(null));
        assertEquals(RRFScoreNormalizer.DEFAULT_RANK_CONSTANT, RRFScoreNormalizer.resolveRankConstant(Map.of()));
        assertEquals(RRFScoreNormalizer.DEFAULT_RANK_CONSTANT, RRFScoreNormalizer.resolveRankConstant(Map.of("other_param", 5)));
    }

    public void testResolveRankConstant_whenParamPresent_thenParsed() {
        assertEquals(25, RRFScoreNormalizer.resolveRankConstant(Map.of(RRFScoreNormalizer.PARAM_NAME_RANK_CONSTANT, 25)));
        // values arrive from parsed JSON, so a string representation must parse too
        assertEquals(25, RRFScoreNormalizer.resolveRankConstant(Map.of(RRFScoreNormalizer.PARAM_NAME_RANK_CONSTANT, "25")));
    }

    public void testResolveRankConstant_whenParamNotNumeric_thenFail() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> RRFScoreNormalizer.resolveRankConstant(Map.of(RRFScoreNormalizer.PARAM_NAME_RANK_CONSTANT, "not_a_number"))
        );
        assertEquals("parameter [rank_constant] must be an integer", exception.getMessage());
    }

    public void testResolveRankConstant_whenParamOutOfRange_thenFail() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> RRFScoreNormalizer.resolveRankConstant(Map.of(RRFScoreNormalizer.PARAM_NAME_RANK_CONSTANT, 0))
        );
        assertEquals("rank constant must be in the interval between 1 and 10000, submitted rank constant: 0", exception.getMessage());
    }

    public void testValidateRankConstant_whenAtBounds_thenAccepted() {
        RRFScoreNormalizer.validateRankConstant(RRFScoreNormalizer.MIN_RANK_CONSTANT);
        RRFScoreNormalizer.validateRankConstant(RRFScoreNormalizer.MAX_RANK_CONSTANT);
    }

    public void testValidateRankConstant_whenOutsideBounds_thenFail() {
        expectThrows(IllegalArgumentException.class, () -> RRFScoreNormalizer.validateRankConstant(0));
        expectThrows(IllegalArgumentException.class, () -> RRFScoreNormalizer.validateRankConstant(-1));
        expectThrows(IllegalArgumentException.class, () -> RRFScoreNormalizer.validateRankConstant(10_001));
    }

    public void testDrainToRanks_whenOrderedByComparator_thenZeroBasedRanks() {
        PriorityQueue<String> queue = new PriorityQueue<>(Comparator.<String>naturalOrder());
        queue.addAll(List.of("c", "a", "b"));

        assertEquals(Map.of("a", 0, "b", 1, "c", 2), RRFScoreNormalizer.drainToRanks(queue, item -> item));
    }

    public void testDrainToRanks_whenEmpty_thenEmptyMap() {
        PriorityQueue<String> queue = new PriorityQueue<>(Comparator.<String>naturalOrder());

        assertTrue(RRFScoreNormalizer.drainToRanks(queue, item -> item).isEmpty());
    }

    public void testDrainToRanks_whenKeyFunctionDiffersFromItem_thenKeyedByFunction() {
        PriorityQueue<Integer> queue = new PriorityQueue<>(Comparator.<Integer>naturalOrder());
        queue.addAll(List.of(30, 10, 20));

        assertEquals(Map.of("doc_10", 0, "doc_20", 1, "doc_30", 2), RRFScoreNormalizer.drainToRanks(queue, item -> "doc_" + item));
    }

    public void testDrainToRanks_whenCalled_thenQueueIsDrained() {
        PriorityQueue<String> queue = new PriorityQueue<>(Comparator.<String>naturalOrder());
        queue.addAll(List.of("b", "a"));

        RRFScoreNormalizer.drainToRanks(queue, item -> item);

        assertTrue("drainToRanks consumes the queue it is given", queue.isEmpty());
    }

    public void testAssignRanksByScoreDescending_whenDistinctScores_thenHighestScoreRanksFirst() {
        Map<String, Integer> ranks = RRFScoreNormalizer.assignRanksByScoreDescending(Map.of("d1", 0.2f, "d2", 0.9f, "d3", 0.5f));

        assertEquals(Map.of("d2", 0, "d3", 1, "d1", 2), ranks);
    }

    public void testAssignRanksByScoreDescending_whenScoresTie_thenBrokenByAscendingId() {
        Map<String, Integer> ranks = RRFScoreNormalizer.assignRanksByScoreDescending(Map.of("b", 0.5f, "a", 0.5f, "c", 0.9f));

        assertEquals(Map.of("c", 0, "a", 1, "b", 2), ranks);
    }

    public void testAssignRanksByScoreDescending_whenEmpty_thenEmptyMap() {
        assertTrue(RRFScoreNormalizer.assignRanksByScoreDescending(Map.of()).isEmpty());
    }
}
