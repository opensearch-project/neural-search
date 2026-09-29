/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.profile;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * What the coordinator itself spent on one fused ({@code fusion}) hybrid query, plus the shape of the work it spent it on.
 *
 * <p>Fused mode fans its legs out and fuses their scores during the coordinator rewrite, which core runs <b>before</b> the
 * first search phase starts: the request's own {@code SearchTimeProvider} is created ahead of
 * {@code Rewriteable.rewriteAndFetch}, so this work is inside the response's {@code took} but outside every
 * {@code phase_took} phase. Nothing in the profile output accounts for it either — the shard entries describe round 1's
 * legs and round 2's substitute query, neither of which is the coordinator. This class is what makes that span reportable:
 * {@code HybridQueryBuilder} times the fan-out around it, {@code HybridFusionOrchestrator} times the fusion phases inside
 * it, and {@link FusedLegProfileMerger} renders it as a profile entry.
 *
 * <p>Mutable and single-writer: one instance per fused hybrid per request, written from up to three threads in sequence —
 * the rewrite thread, the leg MultiSearch response thread, and (when a union count round runs) the count's response
 * thread, which closes {@link #unionCountWaitNanos} and then writes every fusion span. Never two at once: each span is
 * closed before the next callback is invoked, so the handoff is a sequence rather than sharing. Read once when the entry
 * is synthesized. Always constructed, even when the request is not profiled, so
 * that the orchestrator never has to null-check; an unprofiled request throws away everything here except
 * {@link #anyLegTimedOut()}, which is reported on the response itself rather than in the profile section.
 */
@Getter
@Setter
@Accessors(chain = true, fluent = true)
public final class FusedCoordinatorTimings {

    /** Building the leg MultiSearch request: one source copy per leg, plus the per-leg overrides. */
    private long fanOutBuildNanos;

    /**
     * Dispatching the leg MultiSearch and waiting for it. Elapsed, not additional: the legs' own shard work is inside it
     * and is also reported, per shard, by the {@code [fused:hybrid_N.leg_M]} entries. It is the wait, not a cost to add to
     * them.
     */
    private long fanOutWaitNanos;

    /** Reducing the leg responses into the per-leg hit arrays fusion consumes, including the per-hit {@code _index} check. */
    private long windowMergeNanos;

    /**
     * Normalizing each leg's scores and combining them — the shared {@code CoordinatorScoreFusion} core. Normalization and
     * combination are one span rather than two on purpose: splitting them means instrumenting the core that classic
     * hybrid's shard-side path also runs, and this class exists to describe fused mode's coordinator, not to change what
     * classic measures.
     */
    private long fuseScoresNanos;

    /** Sorting the fused scores and cutting to the window. */
    private long rankWindowNanos;

    /**
     * Dispatching the union count round and waiting for it, when one was issued. Elapsed, not additional: like
     * {@link #fanOutWaitNanos} it contains a distributed round the shards also report themselves. Zero when no count ran —
     * which is most requests, since the count is only issued when it can replace the Tail.
     */
    private long unionCountWaitNanos;

    /** Building the query round 2 runs: the {@code _id}-addressed Top clauses, and the Tail when one is needed. */
    private long substituteBuildNanos;

    /** The candidate window this hybrid asked each leg for. */
    private int windowSize;

    /** How many documents survived fusion into the window — the number of Top clauses round 2 carries. */
    private int rankedDocs;

    /** Whether round 2 carries a Tail (needed for totals, aggregations or {@code _name} registration). */
    private boolean tailBuilt;

    private String normalizationTechnique;

    private String combinationTechnique;

    /**
     * Whether this hybrid takes the fast path and, if not, why — see {@link FastPathDecision}. Set before the legs run and
     * completed once they have answered; rendered under {@code debug.fast_path}. {@code null} when nothing evaluated it.
     */
    private FastPathDecision fastPath;

    /** One entry per leg, in leg order, as rendered under the profile node's {@code debug}. */
    private final List<Map<String, Object>> legs = new ArrayList<>();

    /**
     * Whether any leg was truncated by a soft {@code timeout}. Kept as its own field rather than derived from
     * {@link #legs} because it is the one thing here that is reported <b>outside</b> the profile section — it sets the
     * response's own {@code timed_out}, on profiled and unprofiled requests alike (see
     * {@code org.opensearch.neuralsearch.search.FusedLegTimeoutMerger}) — so it must not be reachable only by parsing the
     * rendering.
     */
    @Getter(lombok.AccessLevel.NONE)
    @Setter(lombok.AccessLevel.NONE)
    private boolean anyLegTimedOut;

    /**
     * Record what one leg returned. {@code tookMillis} is the leg response's own {@code took}, which core reports in whole
     * milliseconds — coarse enough at small corpus sizes that it is a shape signal rather than a measurement.
     */
    public void addLeg(final int legIndex, final long tookMillis, final int hits, final boolean timedOut) {
        Map<String, Object> leg = new LinkedHashMap<>();
        leg.put("leg", legIndex);
        leg.put("took_in_millis", tookMillis);
        leg.put("hits", hits);
        leg.put("timed_out", timedOut);
        legs.add(leg);
        anyLegTimedOut = anyLegTimedOut || timedOut;
    }

    /** Whether any leg recorded so far reported a soft timeout, so the response has to be marked incomplete. */
    public boolean anyLegTimedOut() {
        return anyLegTimedOut;
    }

    /** What fusing cost once the legs were back: everything the coordinator did that was not waiting on them. */
    public long fusionNanos() {
        return windowMergeNanos + fuseScoresNanos + rankWindowNanos + substituteBuildNanos;
    }

    /** The whole coordinator span for this hybrid: building the fan-out, waiting on it, and fusing what came back. */
    public long totalNanos() {
        return fanOutBuildNanos + fanOutWaitNanos + unionCountWaitNanos + fusionNanos();
    }
}
