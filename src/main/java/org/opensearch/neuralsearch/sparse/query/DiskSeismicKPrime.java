/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

/**
 * Converts the user-facing {@code heap_factor} recall/latency knob into the {@code k'} (k-prime)
 * block budget that the native disk-resident index ({@code disk_seismic_sq}, i.e. a {@code per_block}
 * forward index) takes.
 *
 * <p>Plain SEISMIC prunes with a <em>relative</em> threshold ({@code summary * heap_factor <
 * kth_score}); the disk-resident index instead scores the globally best {@code k'} block summaries
 * and reads exactly those, which keeps the per-query block-read count flat (good for a disk index's
 * tail latency). So the native disk path takes a {@code k'}, not a {@code heap_factor}. Rather than
 * force users onto a second knob, {@link NativeIndexScorer} keeps exposing {@code heap_factor} and
 * maps it here before putting {@code k_prime} into the native {@code methodParameters}, leaving the
 * native library's {@code k'} interface untouched for callers that use it directly.
 *
 * <p>The mapping is a deliberately simple, predictable linear convention (not a per-corpus
 * calibration): {@code heap_factor = 1.0} reads {@value #K_PRIME_AT_UNIT_HEAP_FACTOR} blocks, and
 * every {@code 0.01} step in {@code heap_factor} moves the budget by one block. It is monotone
 * non-decreasing, so raising {@code heap_factor} never reads fewer blocks.
 *
 * <ul>
 *   <li><b>Floor:</b> {@code k'} is clamped to {@value #MIN_K_PRIME}; the native side rejects a
 *       non-positive budget.
 *   <li><b>No ceiling:</b> an oversized budget is harmless — the native {@code block_budget_query}
 *       takes {@code std::min(k', candidate_pool)}, so a budget above the per-query pool just means
 *       "score every candidate block" (an exhaustive search within the cut lists). The result is
 *       only saturated to {@link Integer#MAX_VALUE} to avoid {@code int} overflow at extreme inputs.
 * </ul>
 */
public final class DiskSeismicKPrime {

    /** Block budget when {@code heap_factor == 1.0} (also the default, since heap_factor defaults to 1.0). */
    static final int K_PRIME_AT_UNIT_HEAP_FACTOR = 20;

    /** Blocks added per unit of {@code heap_factor} (i.e. one block per 0.01 step). */
    static final double K_PRIME_PER_HEAP_FACTOR_UNIT = 100.0d;

    /** Smallest budget the native side accepts (it requires {@code k' > 0}). */
    static final int MIN_K_PRIME = 1;

    private DiskSeismicKPrime() {}

    /**
     * Maps {@code heap_factor} to the disk block budget {@code k'}.
     *
     * @param heapFactor the query's heap factor (validated positive upstream; any non-positive,
     *                   {@code NaN}, or otherwise tiny value simply clamps to {@link #MIN_K_PRIME})
     * @return {@code k'} in {@code [MIN_K_PRIME, Integer.MAX_VALUE]}
     */
    public static int fromHeapFactor(float heapFactor) {
        double kPrime = K_PRIME_AT_UNIT_HEAP_FACTOR + K_PRIME_PER_HEAP_FACTOR_UNIT * ((double) heapFactor - 1.0d);
        long rounded = Math.round(kPrime);
        if (rounded < MIN_K_PRIME) {
            return MIN_K_PRIME;
        }
        if (rounded > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) rounded;
    }
}
