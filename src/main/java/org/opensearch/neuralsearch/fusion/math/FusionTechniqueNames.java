/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion.math;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * The names a request uses to pick a normalization or combination technique, and the parameter keys those techniques
 * read. These strings are part of the public request contract: they appear in the normalization-processor and
 * score-ranker-processor configs, in the hybrid query's {@code fusion} clause, and in explain output.
 *
 * <p>This class is the single owner of the strings. The classic processor techniques and the in-query (fused) path both
 * reference the constants here rather than each other, so neither side initializes a class of the other to learn a
 * technique's name. That decoupling is what lets the classic execution classes be deleted later without taking the
 * fused path with them.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class FusionTechniqueNames {

    /** Normalization: scale every leg score into {@code [0, 1]} from the leg's global minimum and maximum. */
    public static final String MIN_MAX = "min_max";
    /** Normalization: divide every leg score by the leg's global L2 norm. */
    public static final String L2 = "l2";
    /** Normalization: standardize every leg score by the leg's global mean and standard deviation. */
    public static final String Z_SCORE = "z_score";
    /** Normalization: replace every leg score by the reciprocal of its global rank within the leg. */
    public static final String RRF = "rrf";

    /** Combination: weighted arithmetic mean of the normalized leg scores a document has. */
    public static final String ARITHMETIC_MEAN = "arithmetic_mean";
    /** Combination: weighted geometric mean of the positive normalized leg scores a document has. */
    public static final String GEOMETRIC_MEAN = "geometric_mean";
    /** Combination: weighted harmonic mean of the positive normalized leg scores a document has. */
    public static final String HARMONIC_MEAN = "harmonic_mean";
    /**
     * Combination: weighted sum of the reciprocal-rank contributions. Shares its name with the {@link #RRF} normalization
     * because a request spells the whole reciprocal rank fusion as one technique.
     */
    public static final String RRF_COMBINATION = RRF;

    /** Combination parameter: per-leg weights, one per sub-query, each in {@code [0, 1]} and summing to {@code 1.0}. */
    public static final String PARAM_WEIGHTS = "weights";
    /** RRF parameter: the {@code k} in {@code 1 / (k + rank)}. */
    public static final String PARAM_RANK_CONSTANT = "rank_constant";
    /** min_max parameter: per-leg lower bounds. */
    public static final String PARAM_LOWER_BOUNDS = "lower_bounds";
    /** min_max parameter: per-leg upper bounds. */
    public static final String PARAM_UPPER_BOUNDS = "upper_bounds";
}
