/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion.math;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.apache.commons.lang3.Range;

import com.google.common.math.DoubleMath;

/**
 * The per-leg weights of a combination technique, validated once. Empty means unweighted: every leg weighs {@code 1.0}.
 *
 * <p>The validation rules and their messages are the ones the classic combination techniques have always enforced —
 * every weight in {@code [0.0, 1.0]}, the weights summing to {@code 1.0} within {@value #DELTA_FOR_WEIGHTS_ASSERTION},
 * and, once scores arrive, exactly one weight per leg — so a request rejected on one path is rejected with the same words
 * on every other.
 */
public final class CombinationWeights {

    /** Tolerance on the sum of the weights, as classic has always allowed. */
    public static final float DELTA_FOR_WEIGHTS_ASSERTION = 0.01f;
    /** The weight of every leg when no weights were given. */
    public static final float DEFAULT_WEIGHT = 1.0f;

    private static final Range<Float> WEIGHT_RANGE = Range.of(0.0f, 1.0f);

    /** No weights: every leg weighs {@link #DEFAULT_WEIGHT}. */
    public static final CombinationWeights UNWEIGHTED = new CombinationWeights(List.of());

    private final List<Float> weights;

    private CombinationWeights(final List<Float> weights) {
        this.weights = weights;
    }

    /**
     * @param weights one weight per leg, or empty / {@code null} for unweighted
     * @return validated weights
     * @throws IllegalArgumentException when a weight is out of range or the weights do not sum to {@code 1.0}
     */
    public static CombinationWeights of(final List<Float> weights) {
        if (Objects.isNull(weights) || weights.isEmpty()) {
            return UNWEIGHTED;
        }
        List<Float> copy = List.copyOf(weights);
        validate(copy);
        return new CombinationWeights(copy);
    }

    /** Array form of {@link #of(List)}. */
    public static CombinationWeights of(final float[] weights) {
        if (Objects.isNull(weights) || weights.length == 0) {
            return UNWEIGHTED;
        }
        List<Float> list = new ArrayList<>(weights.length);
        for (float weight : weights) {
            list.add(weight);
        }
        return of(list);
    }

    /**
     * The classic range-and-sum validation, exposed so callers that hold weights in another shape can reuse the rules.
     *
     * @throws IllegalArgumentException with the classic messages
     */
    public static void validate(final List<Float> weights) {
        boolean isOutOfRange = weights.stream().anyMatch(weight -> WEIGHT_RANGE.contains(weight) == false);
        if (isOutOfRange) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "all weights must be in range [0.0 ... 1.0], submitted weights: %s",
                    Arrays.toString(weights.toArray(new Float[0]))
                )
            );
        }
        float sumOfWeights = weights.stream().reduce(0.0f, Float::sum);
        if (DoubleMath.fuzzyEquals(1.0f, sumOfWeights, DELTA_FOR_WEIGHTS_ASSERTION) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "sum of weights for combination must be equal to 1.0, submitted weights: %s",
                    Arrays.toString(weights.toArray(new Float[0]))
                )
            );
        }
    }

    /** @return whether no weights were given */
    public boolean isEmpty() {
        return weights.isEmpty();
    }

    /** @return the number of weights, zero when unweighted */
    public int size() {
        return weights.size();
    }

    /** @return the weights as an immutable list, empty when unweighted */
    public List<Float> asList() {
        return weights;
    }

    /**
     * @param legIndex zero-based leg position
     * @return the leg's weight, or {@link #DEFAULT_WEIGHT} when unweighted or when the index is beyond the given weights
     */
    public float weightFor(final int legIndex) {
        return weightFor(weights, legIndex);
    }

    /** List form of {@link #weightFor(int)} for callers that hold unvalidated weights. */
    public static float weightFor(final List<Float> weights, final int legIndex) {
        return Objects.nonNull(weights) && legIndex < weights.size() ? weights.get(legIndex) : DEFAULT_WEIGHT;
    }

    /**
     * Require one weight per leg once the number of legs is known.
     *
     * @param legCount number of legs (scores) being combined
     * @throws IllegalArgumentException with the classic message when weights were given and their count differs
     */
    public void requireMatches(final int legCount) {
        requireCountMatches(weights, legCount);
    }

    /**
     * List form of {@link #requireMatches(int)} for callers that hold unvalidated weights: only the count is checked.
     */
    public static void requireCountMatches(final List<Float> weights, final int legCount) {
        if (Objects.isNull(weights) || weights.isEmpty()) {
            return;
        }
        if (legCount != weights.size()) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "number of weights [%d] must match number of sub-queries [%d] in hybrid query",
                    weights.size(),
                    legCount
                )
            );
        }
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof CombinationWeights that && weights.equals(that.weights);
    }

    @Override
    public int hashCode() {
        return weights.hashCode();
    }

    @Override
    public String toString() {
        return weights.toString();
    }
}
