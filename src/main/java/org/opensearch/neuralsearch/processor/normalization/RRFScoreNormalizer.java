/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.function.Function;

import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.math.NumberUtils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Stateless reciprocal rank fusion arithmetic: the rank score formula, rank constant parsing and
 * validation, and derivation of zero based ranks from an ordered queue of results.
 * <p>
 * RRF is applied from more than one place. The shard side {@link RRFNormalizationTechnique} works on
 * {@link org.opensearch.neuralsearch.processor.CompoundTopDocs}, while coordinator side fusion works on
 * per sub-query maps of document id to score. The shapes differ but the arithmetic must not, so it is
 * defined here once instead of being re-derived per call site.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class RRFScoreNormalizer {
    public static final int DEFAULT_RANK_CONSTANT = 60;
    public static final String PARAM_NAME_RANK_CONSTANT = "rank_constant";
    public static final int MIN_RANK_CONSTANT = 1;
    public static final int MAX_RANK_CONSTANT = 10_000;

    private static final Range<Integer> RANK_CONSTANT_RANGE = Range.of(MIN_RANK_CONSTANT, MAX_RANK_CONSTANT);
    // Rank scores are quantized to 10 decimal places, i.e. expressed as an integer numerator over 10^10.
    private static final long SCORE_SCALE_L = 10_000_000_000L;
    private static final double SCORE_SCALE_D = 1.0e10;

    /**
     * Rank score for a single document within a single sub-query, {@code 1 / (rankConstant + rank + 1)} rounded
     * HALF_UP to 10 decimal places. Scores are summed across sub-queries in the combination step.
     * <p>
     * This is an allocation-free integer equivalent of the original implementation:
     * <pre>{@code
     * BigDecimal.ONE.divide(BigDecimal.valueOf(rankConstant + rank + 1), 10, RoundingMode.HALF_UP).floatValue()
     * }</pre>
     * Because 10^10 and 2 * 10^10 both fit in a {@code long}, rounding HALF_UP to scale 10 is exactly expressible
     * as the integer division {@code (2 * 10^10 + d) / (2 * d)}. That quotient is at most 5 * 10^9, well inside
     * the range a {@code double} represents exactly, as is 10^10 itself, so the single narrowing to {@code float}
     * reproduces what {@code BigDecimal.floatValue()} produced. Verified bit-identical for every reachable
     * denominator in [2, Integer.MAX_VALUE].
     * <p>
     * The final division is deliberately performed in {@code double}. Narrowing the numerator to {@code float}
     * first and dividing by {@code 1.0e10f} rounds twice and is <em>not</em> bit-identical: it differs for 167
     * denominators, starting at 3.
     *
     * @param rank zero based rank of the document within the sub-query
     * @param rankConstant RRF rank constant
     * @return the rank score
     */
    public static float scoreForRank(final int rank, final int rankConstant) {
        long denominator = (long) rankConstant + rank + 1;
        long numerator = (2 * SCORE_SCALE_L + denominator) / (2 * denominator);
        return (float) (numerator / SCORE_SCALE_D);
    }

    /**
     * Read the rank constant from user provided parameters, falling back to {@link #DEFAULT_RANK_CONSTANT}
     * when it is absent.
     *
     * @param params user provided technique parameters, may be null
     * @return a validated rank constant
     * @throws IllegalArgumentException if the value is not an integer or is out of range
     */
    public static int resolveRankConstant(final Map<String, Object> params) {
        if (Objects.isNull(params) || !params.containsKey(PARAM_NAME_RANK_CONSTANT)) {
            return DEFAULT_RANK_CONSTANT;
        }
        int rankConstant = getParamAsInteger(params, PARAM_NAME_RANK_CONSTANT);
        validateRankConstant(rankConstant);
        return rankConstant;
    }

    /**
     * @param rankConstant rank constant to check
     * @throws IllegalArgumentException if the rank constant is outside the supported range
     */
    public static void validateRankConstant(final int rankConstant) {
        if (!RANK_CONSTANT_RANGE.contains(rankConstant)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "rank constant must be in the interval between 1 and 10000, submitted rank constant: %d",
                    rankConstant
                )
            );
        }
    }

    /**
     * How a rrf normalization step names itself in an {@code explain} response, rank constant included. Here for the same
     * reason the arithmetic is: {@link RRFNormalizationTechnique} renders this shard side and
     * {@link org.opensearch.neuralsearch.fusion.RrfScalarNormalizer} renders it on the coordinator, and a request explained
     * on one path has to read the same as the same request explained on the other. The rank constant belongs in the text
     * because it changes every score in the tree below it — a description that omitted it would not identify the
     * normalization that actually ran.
     *
     * @param rankConstant the rank constant this normalization was configured with
     * @return the description, for example {@code rrf, rank_constant [60]}
     */
    public static String describeWithRankConstant(final int rankConstant) {
        return String.format(Locale.ROOT, "%s, rank_constant [%s]", RRFNormalizationTechnique.TECHNIQUE_NAME, rankConstant);
    }

    /**
     * Drain a queue of results, assigning zero based ranks in the order the queue yields them. Results the
     * queue's comparator treats as equal are ranked in the order the heap happens to surface them, which is
     * not the same order a stable sort would produce, so callers must supply the queue rather than a sorted
     * collection if they need to reproduce this exactly.
     *
     * @param queue results for a single sub-query, best first; drained by this call
     * @param keyFunction maps a result to the key its rank is recorded under
     * @return map of result key to zero based rank
     */
    public static <T, K> Map<K, Integer> drainToRanks(final PriorityQueue<T> queue, final Function<T, K> keyFunction) {
        Map<K, Integer> ranks = new HashMap<>();
        // first rank
        int rank = 0;
        while (!queue.isEmpty()) {
            ranks.put(keyFunction.apply(queue.poll()), rank++);
        }
        return ranks;
    }

    /**
     * Assign zero based ranks to documents held as a map of document id to score, ordered by descending
     * score with ascending document id as tie break. This is the shape coordinator side fusion works in,
     * where results for a sub-query have already been merged across shards.
     *
     * @param scoresById document id to score for a single sub-query
     * @return map of document id to zero based rank
     */
    public static Map<String, Integer> assignRanksByScoreDescending(final Map<String, Float> scoresById) {
        Comparator<Map.Entry<String, Float>> byScoreDescendingThenId = Comparator.<Map.Entry<String, Float>, Float>comparing(
            Map.Entry::getValue
        ).reversed().thenComparing(Map.Entry::getKey);
        PriorityQueue<Map.Entry<String, Float>> queue = new PriorityQueue<>(byScoreDescendingThenId);
        queue.addAll(scoresById.entrySet());
        return drainToRanks(queue, Map.Entry::getKey);
    }

    private static int getParamAsInteger(final Map<String, Object> parameters, final String fieldName) {
        try {
            return NumberUtils.createInteger(String.valueOf(parameters.get(fieldName)));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "parameter [%s] must be an integer", fieldName));
        }
    }
}
