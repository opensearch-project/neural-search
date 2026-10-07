/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

/**
 * Coordinator-side {@code timed_out} for fused ({@code fusion}) hybrid queries: makes a leg that a soft {@code timeout}
 * truncated visible on the response.
 *
 * <p>A fused hybrid propagates the request's {@code timeout} to each of its legs, and a leg that exceeds it returns the
 * candidates it had collected rather than failing — that being what a {@code timeout} is, a soft bound, as distinct from
 * the hard bounds a leg can also hit and which do fail it. The fusion window is then narrower than a complete run's, so
 * the ranking is computed from an incomplete candidate set — and without this, the response says nothing about it. Classic
 * hybrid has no such gap: its sub-queries run shard-side inside the search phases, so core's own counters see the
 * truncation and set the response's {@code timed_out} itself. Fused mode's legs are a separate in-process
 * {@code MultiSearch} issued during the coordinator rewrite, and the per-leg responses that know are consumed there and
 * discarded.
 *
 * <p>Request-scoped, like {@code FusedLegProfileMerger}: created by {@link HybridQuerySearchRequestFilter}, handed to every
 * fused {@code hybrid} the request fans out, published to by the leg fan-out callback, read once when the response comes
 * back. Unlike that one it is attached whether or not the request asked to be profiled — an incomplete answer has to be
 * reported as one either way, and the profile entry's per-leg {@code timed_out} under {@code debug} is a diagnostic for
 * someone already looking, not a flag a client can act on.
 *
 * <p>Across legs and across hybrids the signal is a logical OR, and only ever sets the flag: it means "some part of this
 * answer is incomplete", which is what core's own {@code timed_out} means, and a leg that finished cannot make an
 * incomplete answer complete.
 *
 * <p>What is deliberately <b>not</b> reported is the response's {@code _shards} counters, and the reason is that
 * {@code timed_out} is a boolean closed under OR while {@code _shards} is a {@code total}/{@code successful}/
 * {@code skipped} triple plus a failures array over <i>one</i> shard set. N legs search the same shards, so folding their
 * counters in would mean either redefining what {@code total} counts on every fused response — double-counting a shard
 * that failed in two legs — or emitting {@code failed > total - successful}. That is a response-shape decision rather
 * than a merge. A leg degraded by shard failures is <b>not</b> silent meanwhile: {@code HybridFusionOrchestrator}
 * names the affected legs in a response {@code Warning} header, saying the fused scores were computed over an incomplete
 * result set. So the gap that survives is the channel — a header rather than a body field — not the absence of a signal.
 *
 * <p>{@code terminated_early} is omitted for a simpler reason: no leg can report it. {@code terminate_after} is rejected
 * outright at rewrite in fused mode ({@code CandidateScope}), and core sets the flag only on its {@code terminate_after}
 * paths, so the value is unreachable rather than unmergeable.
 *
 * <p>Published on the leg-MultiSearch response thread and read on the response-listener thread; {@code volatile} for that
 * hand-off, which is ordered anyway by the rewrite completing before the search phases start.
 */
public final class FusedLegTimeoutMerger {

    /** Whether any leg of any fused hybrid in this request reported a soft timeout. Never cleared once set. */
    private volatile boolean anyLegTimedOut;

    /** A handle for one fused hybrid, handed to its {@code HybridQueryBuilder} and published to once its legs are back. */
    public interface LegTimeoutConsumer {
        void accept(boolean legTimedOut);
    }

    /**
     * The consumer to attach to a fused hybrid. Publishing {@code false} is the normal case and records nothing, so a
     * hybrid whose legs all completed cannot undo a sibling hybrid's truncation.
     */
    public LegTimeoutConsumer consumer() {
        return legTimedOut -> {
            if (legTimedOut) {
                anyLegTimedOut = true;
            }
        };
    }

    public boolean anyLegTimedOut() {
        return anyLegTimedOut;
    }
}
