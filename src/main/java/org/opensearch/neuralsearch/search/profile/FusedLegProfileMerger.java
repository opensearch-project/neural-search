/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.profile;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.profile.NetworkTime;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.profile.aggregation.AggregationProfileShardResult;
import org.opensearch.search.profile.fetch.FetchProfileShardResult;
import org.opensearch.search.profile.query.CollectorResult;
import org.opensearch.search.profile.query.QueryProfileShardResult;

/**
 * Coordinator-side profiling for a fused ({@code fusion}) hybrid query: its legs, and its own fusion work.
 *
 * <p>Request-scoped. Collects the profile trees of a fused hybrid's leg sub-searches from rewrite round 1, where the legs
 * run, plus what the coordinator spent fanning them out and fusing what came back, and merges both into the profile section
 * of the response, where they are rendered. Created by {@code HybridQuerySearchRequestFilter}, handed to every fused
 * {@code hybrid} in the request, added to by the leg fan-out callback, read once when the response comes back.
 *
 * <p>Between them the entries account for a fused request end to end: {@code [fused:hybrid_N.leg_M]} for each leg as the
 * user wrote it, {@code [coordinator][fused:hybrid_N]} for the fan-out and the fusion, and {@code [fused:rewrite]} for round
 * 2 — the substituted query, together with anything else the request asked of that shard alongside it, aggregations
 * included. Without the coordinator entry the fusion span is reportable from nowhere at all — core builds
 * the request's {@code SearchTimeProvider} before {@code Rewriteable.rewriteAndFetch}, so it lands inside {@code took} but
 * outside every {@code phase_took} phase.
 *
 * <p>Shaped after core's {@code SearchResponseMerger}: entries are added as they arrive, and the merged section is built
 * once at the end. Merging is the only option available — {@link SearchProfileShardResults} takes its map whole and never
 * mutates it, so a coordinator-side component can add entries only by constructing a new one and a new response around
 * it. That is exactly what core does on its single-remote-cluster CCS path in {@code TransportSearchAction}, and how
 * {@code SearchResponseMerger} unions the profile maps of N sub-responses. The response itself is rebuilt by
 * {@link org.opensearch.neuralsearch.search.FusedResponseRebuilder}, which this class hands its section to.
 *
 * <p>Added to on the leg-MultiSearch response thread and read on the response-listener thread, hence the concurrent map.
 */
public final class FusedLegProfileMerger {

    /**
     * Everything this class contributes to the profile section, by entry key: a leg's key is a round-2 shard key with a leg
     * label inserted, and the coordinator's is {@link #COORDINATOR_KEY} with a hybrid label — so nothing collides with round
     * 2, with another leg, or with another hybrid. One map rather than two, so that {@link #isEmpty()}, the response rebuild
     * and the nested-label retagging need no special case for the coordinator entry.
     */
    private final Map<String, ProfileShardResult> legProfiles = new ConcurrentHashMap<>();

    /** Marks the label groups this class inserts, so a re-tagged key can be told apart from a plain shard key. */
    private static final String TAG_OPEN = "[fused:";

    /**
     * Label for the response's own shard entries. They describe the query the fused rewrite <b>replaced</b> the hybrid
     * with — a {@code bool} over {@code _id} terms — not the hybrid the user wrote, so leaving them untagged next to the
     * labelled legs would read as if they were the user's query.
     *
     * <p>It names round 2, not the substituted hybrid alone. A shard key owns a whole {@link ProfileShardResult} — its
     * {@code searches}, {@code aggregations} and {@code fetch} sections together — so an entry under this label is
     * everything that shard did after the rewrite: an aggregation the request also asked for, or a sibling clause of an
     * enclosing {@code bool}, is in here too. There is no sub-key to tag, so the label cannot be narrower than the entry it
     * names, and it does not need to be: every node under it ran in round 2.
     */
    private static final String REWRITE_LABEL = "rewrite";

    /**
     * Stands where a node id stands in a shard key, for the one entry that does not describe a shard. The coordinator's own
     * fusion work happens once per request, not once per shard, so there is no shard to key it by; the profile section is a
     * map of opaque id strings — core writes the key with {@code field("id", key)} and reads it back with
     * {@code parser.text()}, never parsing its groups — so a key that is not shard-shaped travels the wire and renders
     * unchanged. It is still built with the same bracket grammar, and still carries the {@code [fused:...]} label group, so
     * that a nested hybrid's entry composes through {@link #retag} exactly like a leg's.
     */
    private static final String COORDINATOR_KEY = "[coordinator]";

    /** Node type for the synthesized coordinator entry. Deliberately not {@code HybridQuery} — see {@link #synthesize}. */
    private static final String COORDINATOR_NODE_TYPE = "FusedHybridQuery";

    /** Name and reason for the collector slot core renders unconditionally — see {@link #synthesize}. */
    private static final String COORDINATOR_COLLECTOR_NAME = "HybridFusionCombiner";
    private static final String COORDINATOR_COLLECTOR_REASON = "fuse_candidates";

    /**
     * A per-hybrid handle handed to a {@code HybridQueryBuilder}: it knows its own label, so the builder only has to say
     * which leg a profile map came from.
     */
    public interface LegProfileConsumer {
        void accept(int legIndex, Map<String, ProfileShardResult> legShardProfiles);
    }

    /** A per-hybrid handle for what the coordinator itself spent, published once when the fused query has been built. */
    public interface CoordinatorTimingConsumer {
        void accept(FusedCoordinatorTimings timings);
    }

    /** A consumer for one fused hybrid in the request, labelled so nested and sibling hybrids stay apart. */
    public LegProfileConsumer forHybrid(final String hybridLabel) {
        return (legIndex, legShardProfiles) -> {
            if (Objects.isNull(legShardProfiles)) {
                return;
            }
            String legLabel = String.format(Locale.ROOT, "%s.leg_%d", hybridLabel, legIndex);
            legShardProfiles.forEach((shardKey, result) -> legProfiles.put(retag(shardKey, legLabel), withoutFetch(result)));
        };
    }

    /**
     * A timing consumer for one fused hybrid in the request. Publishes into the same map the leg trees go into, so the
     * coordinator entry composes, sorts and survives the response rebuild by exactly the same path they do — and so a
     * nested hybrid's coordinator entry, which arrives inside its leg's response already keyed
     * {@code [coordinator][fused:hybrid_N]}, is retagged into the label path like anything else the leg reported.
     */
    public CoordinatorTimingConsumer forHybridTiming(final String hybridLabel) {
        return timings -> {
            if (Objects.isNull(timings)) {
                return;
            }
            legProfiles.put(COORDINATOR_KEY + TAG_OPEN + hybridLabel + "]", synthesize(timings));
        };
    }

    public boolean isEmpty() {
        return legProfiles.isEmpty();
    }

    /**
     * The profile section the response should carry: the collected entries plus the response's own, relabelled — or
     * {@code null} when nothing was ever collected, meaning the response's own section stands as it is.
     *
     * <p>The response's own entries are relabelled {@code [fused:rewrite]} rather than dropped: they are the only record
     * of what the rewritten query cost, and dropping them would hide the {@code _id} lookup and the Tail from the very
     * output that exists to account for time. Relabelling only happens when something was collected, so a classic or
     * unprofiled response keeps its own section untouched.
     *
     * <p>Returns the section rather than a rebuilt response so that a request reporting several things rebuilds once —
     * see {@link org.opensearch.neuralsearch.search.FusedResponseRebuilder}, which owns the argument lists that have to
     * track core's constructors.
     */
    public SearchProfileShardResults mergedProfileResults(final SearchResponse response) {
        if (isEmpty()) {
            return null;
        }
        Map<String, ProfileShardResult> merged = new HashMap<>();
        Map<String, ProfileShardResult> roundTwo = response.getProfileResults();
        if (Objects.nonNull(roundTwo)) {
            roundTwo.forEach((shardKey, result) -> merged.put(retag(shardKey, REWRITE_LABEL), result));
        }
        merged.putAll(legProfiles);
        return new SearchProfileShardResults(merged);
    }

    /**
     * {@code shardKey} with this leg's label inserted as a new group, <b>outermost first</b>.
     *
     * <p>A leg sub-search is a search action of its own, so a nested fused hybrid's legs arrive already tagged by the
     * inner request. Appending would then read inside-out — {@code [inner leg][outer leg]} — so the new tag goes in front
     * of any tag already present, and after the shard key itself. That keeps every entry sorted directly under its own
     * shard's entry, since the profile section is rendered in key order, and makes the label a path from the user's query
     * down.
     */
    private static String retag(final String shardKey, final String label) {
        int existing = shardKey.indexOf(TAG_OPEN);
        String base = existing < 0 ? shardKey : shardKey.substring(0, existing);
        String tail = existing < 0 ? "" : shardKey.substring(existing);
        return base + TAG_OPEN + label + "]" + tail;
    }

    /**
     * {@code result} with its fetch section emptied, keeping the query, aggregation and network sections.
     *
     * <p>A leg's fetch phase is not the user's: a leg asks for {@code _source: false} and no stored fields, so its fetch
     * only materializes the {@code _id}s the fusion needs. Reporting it alongside the real fetch would offer a timing
     * that answers no question the user can ask — and, being cheap and repeated per leg, invite reading the fetch cost as
     * higher than it is. The section is emptied rather than removed because core renders it unconditionally.
     */
    private static ProfileShardResult withoutFetch(final ProfileShardResult result) {
        return new ProfileShardResult(
            result.getQueryProfileResults(),
            result.getAggregationProfileResults(),
            new FetchProfileShardResult(List.of()),
            result.getNetworkTime()
        );
    }

    /**
     * One profile entry for what the coordinator spent fusing, built from measured spans.
     *
     * <p>The node type is {@code FusedHybridQuery} rather than {@code HybridQuery} on purpose. Classic hybrid's
     * {@code HybridQuery} node is one shard-local span for both legs sharing one collector; this is a coordinator span that
     * contains a wait on independent leg searches. They answer different questions and must not invite subtraction, so they
     * do not share a name.
     *
     * <p>{@code breakdown} carries the phases, and they are leaves: they sum to the node's own time, with nothing counted
     * twice. {@code fan_out_wait} is the one to read carefully — it is elapsed time containing the legs' own execution,
     * which the {@code [fused:hybrid_N.leg_M]} entries report per shard, so it is a wait to attribute rather than a cost to
     * add to them. {@code debug} carries what is not a duration: the window, what survived into it, whether round 2 needed
     * a Tail, and each leg's own {@code took} and {@code timed_out} — the last of which is the only place a soft-timeout leg
     * (whose window is narrower than a complete run's) becomes visible.
     *
     * <p>Two slots exist only because core renders them unconditionally. {@code collector} cannot be empty —
     * {@code QueryProfileShardResult#toXContent} dereferences it — so it carries the fusion subtotal under a name that says
     * what it is; the name deliberately avoids {@code CollectorManager}, which is core's trigger for emitting per-slice
     * fields that would be meaningless here. {@code rewrite_time} is 0 because no Lucene rewrite happens on the
     * coordinator, and reporting the fusion time there as well would give one measurement two names.
     */
    private static ProfileShardResult synthesize(final FusedCoordinatorTimings timings) {
        Map<String, Long> breakdown = new LinkedHashMap<>();
        breakdown.put("fan_out_build", timings.fanOutBuildNanos());
        breakdown.put("fan_out_wait", timings.fanOutWaitNanos());
        breakdown.put("window_merge", timings.windowMergeNanos());
        breakdown.put("fuse_scores", timings.fuseScoresNanos());
        breakdown.put("rank_window", timings.rankWindowNanos());
        breakdown.put("substitute_build", timings.substituteBuildNanos());

        Map<String, Object> debug = new LinkedHashMap<>();
        debug.put("window_size", timings.windowSize());
        debug.put("ranked_docs", timings.rankedDocs());
        debug.put("tail_built", timings.tailBuilt());
        debug.put("legs", timings.legs());
        if (Objects.nonNull(timings.fastPath())) {
            // The fast-path verdict of this request's unprofiled twin: profiling keeps two rounds, so this is how a user
            // reads why the same request runs one round or two (see FastPathDecision).
            debug.put("fast_path", timings.fastPath().toMap());
        }

        ProfileResult node = new ProfileResult(
            COORDINATOR_NODE_TYPE,
            String.format(
                Locale.ROOT,
                "%d legs, window %d, %s / %s",
                timings.legs().size(),
                timings.windowSize(),
                timings.normalizationTechnique(),
                timings.combinationTechnique()
            ),
            breakdown,
            debug,
            timings.totalNanos(),
            List.of()
        );
        QueryProfileShardResult query = new QueryProfileShardResult(
            List.of(node),
            0L,
            new CollectorResult(COORDINATOR_COLLECTOR_NAME, COORDINATOR_COLLECTOR_REASON, timings.fusionNanos(), List.of())
        );
        return new ProfileShardResult(
            List.of(query),
            new AggregationProfileShardResult(List.of()),
            new FetchProfileShardResult(List.of()),
            new NetworkTime(0, 0)
        );
    }
}
