/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion;

import java.util.Map;

/**
 * Per-leg normalization step of coordinator-side fusion: turns one leg's raw scores into that leg's normalized scores.
 * Paired with {@link org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique}, which combines the
 * normalized values <i>across</i> legs — one interface normalizes within a leg, the other combines between legs.
 *
 * <p>The contract is a whole-leg transform rather than a per-score function on purpose: every technique needs to see the
 * leg's full value set before it can score any single doc, and each computes different summary state internally
 * ({@code min_max} → min/max, {@code z_score} → mean/stddev, {@code l2} → the L2 norm). It also lets rank-based
 * techniques such as RRF fit the same shape — they simply sort the leg and emit {@code 1/(rank_constant + rank + 1)} by
 * position, deriving rank internally instead of reading a scalar statistic.
 *
 * <p>Notes for implementors:
 * <ul>
 *   <li><b>Keys are opaque.</b> A key identifies a document to the caller (today {@code _index} plus {@code _id}); a
 *       normalizer must only carry keys through unchanged, never parse or construct them. This is what lets the caller
 *       change its document identity scheme without touching any normalizer.</li>
 *   <li><b>Return one entry per input entry.</b> {@link CoordinatorScoreFusion} treats a key missing from the returned
 *       map as "this leg did not match that doc", so dropping keys silently changes fusion input.</li>
 *   <li><b>No shard merging.</b> On the coordinator a leg's map is already the merged across-shard result set, so a
 *       technique may compute its statistics directly from the values — no need for the mergeable
 *       {@code {count, sum, sumSq}} accumulators the classic shard-side path requires.</li>
 * </ul>
 */
public interface ScalarNormalizer {

    /**
     * Normalize one leg's scores.
     *
     * @param legRawScores that leg's {@code key -> raw score} view (never null; may be empty when the leg matched nothing)
     * @return {@code key -> normalized score} with the same key set as the input
     */
    Map<String, Float> normalizeLeg(Map<String, Float> legRawScores);

    /** The technique name this normalizer implements, matching the name used in the {@code fusion} config. */
    String techniqueName();

    /**
     * How this normalization step names itself in an {@code explain} response, the coordinator-side counterpart of
     * {@link org.opensearch.neuralsearch.processor.explain.ExplainableTechnique#describe()}. The name alone is the whole
     * description for a technique that carries no parameter, which is why that is the default; a technique that does carry
     * one overrides this to name it, so the same request explained on either path reads the same.
     *
     * <p>Kept separate from {@link #techniqueName()} rather than folded into it because that name is also the key
     * {@link ScalarNormalizers} resolves a config value against, and a description is not a lookup key.
     *
     * @return this normalizer's description for {@code explain}
     */
    default String describe() {
        return techniqueName();
    }
}
