/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;

import lombok.SneakyThrows;

/**
 * End-to-end coverage of what {@code profile: true} reports for a fused ({@code fusion}) {@code hybrid} query.
 *
 * <p>A fused hybrid runs its legs as leg sub-searches during rewrite and replaces itself with a {@code bool} over the
 * fused window's {@code _id}s, so the query the shards profile is the substituted one — on its own it names neither leg:
 * no ANN node, no per-leg collector. {@code FusedLegProfileMerger} collects each leg's profile tree and merges it into the
 * response's profile section under the leg's own shard key with a {@code [fused:<hybrid>.leg_<n>]} group inserted, and
 * relabels the response's own entries {@code [fused:rewrite]} because they describe the substituted query and not the
 * user's hybrid. It also synthesizes one {@code [coordinator][fused:<hybrid>]} entry per fused hybrid, for the fan-out and
 * the fusion between the two — work that happens before the first search phase starts and so is reportable from nowhere
 * else.
 *
 * <p>Everything asserted here needs a live cluster: which entries exist, how they are keyed, which of them names the ANN
 * leg, which fetch sections survive, what the coordinator entry says, and that asking for a profile does not change the
 * answer. Durations, tree sizes and byte sizes vary from run to run and are asserted only as structure (phases summing to
 * their node's time), never as values.
 *
 * <p>{@code track_total_hits: true} throughout, so round 2 keeps its Tail and is at its largest. That is what makes
 * "round 2 names no ANN leg" a statement about how the Tail addresses a materialized leg — by {@code _id} — rather than
 * about a clause that happened to be pruned away.
 *
 * <p>Dataset: 6 documents, ids 1..6, each with the same text and a 2-dimensional vector, queried by two legs — a
 * {@code knn} leg (the one whose nodes only per-leg profiling can show) and a {@code term} leg that matches all six.
 *
 * <p>Run: {@code ./gradlew integTest --tests "*HybridQueryFusedModeProfileIT*"}.
 */
public class HybridQueryFusedModeProfileIT extends BaseNeuralSearchIT {

    private static final String INDEX = "test-fused-profile";
    private static final String SHARDED_INDEX = "test-fused-profile-sharded";
    private static final String NORM_PIPELINE = "fused-profile-norm-pipeline";
    private static final String TEXT_FIELD = "text";
    private static final String VECTOR_FIELD = "vec";
    private static final String RANK_FIELD = "rank";
    private static final int TOTAL_DOCS = 6;
    private static final int WINDOW_SIZE = 10;
    private static final int SHARDS = 2;

    /** Marks the label group the merge inserts, so a retagged key can be told apart from a plain shard key. */
    private static final String TAG_OPEN = "[fused:";
    /** The label of the response's own entries: the query the rewrite substituted, not the hybrid the user wrote. */
    private static final String REWRITE_TAG = "[fused:rewrite]";
    /** The label of a hybrid's coordinator entry: what fusing it cost, which happens once per request and on no shard. */
    private static final String COORDINATOR_TAG = "[fused:hybrid_0]";
    /** Where a node id stands in a shard key, for the one entry that does not describe a shard. */
    private static final String COORDINATOR_BASE = "[coordinator]";
    /** Deliberately not {@code HybridQuery}: a coordinator span must not invite subtraction from a shard-local one. */
    private static final String COORDINATOR_NODE_TYPE = "FusedHybridQuery";
    /**
     * Matched case-insensitively against a profile node's {@code type}. The k-NN plugin currently reports
     * {@code LuceneEngineKnnVectorQuery} over {@code OSKnnFloatVectorQuery}; matching the family rather than a fixed class
     * name keeps this test alive across engine-side renames while still failing if no ANN node is reported at all.
     */
    private static final String ANN_NODE_MARKER = "knn";

    /**
     * The whole contract of the label scheme on the flat case: one entry per leg plus the response's own, every entry
     * labelled, the ANN leg named in its own entry and nowhere else, and only the user's fetch kept.
     */
    @SneakyThrows
    public void testProfiledFusedHybrid_thenEveryLegIsLabelledAndRoundTwoIsRelabelled() {
        ensureDataset(INDEX, 1);

        Map<String, Object> response = search(INDEX, profiled(fusedHybrid(knnLeg(), termLeg())));
        Map<String, List<String>> nodeTypes = queryNodeTypes(response);
        Map<String, Integer> fetchNodes = fetchNodeCounts(response);

        assertEquals("one entry per leg, plus the coordinator's, plus the response's own: " + nodeTypes.keySet(), 4, nodeTypes.size());
        String knnLegKey = onlyKeyEndingWith(nodeTypes.keySet(), legTag(0));
        String termLegKey = onlyKeyEndingWith(nodeTypes.keySet(), legTag(1));
        String rewriteKey = onlyKeyEndingWith(nodeTypes.keySet(), REWRITE_TAG);
        String coordinatorKey = onlyKeyEndingWith(nodeTypes.keySet(), COORDINATOR_TAG);
        assertTrue(
            "an untagged entry would read as the user's own query: " + nodeTypes.keySet(),
            nodeTypes.keySet().stream().allMatch(key -> key.contains(TAG_OPEN))
        );
        // The label is inserted after the shard key, which is what makes a leg entry render under its own shard.
        assertEquals("a leg entry keeps its own shard key", baseShardKey(rewriteKey), baseShardKey(knnLegKey));
        assertEquals("and the coordinator's entry belongs to no shard", COORDINATOR_BASE, baseShardKey(coordinatorKey));

        // The gap this closes: each leg is named as the query the user wrote.
        assertTrue("knn leg must report an ANN node: " + nodeTypes.get(knnLegKey), hasAnnNode(nodeTypes.get(knnLegKey)));
        assertTrue("term leg must report a TermQuery: " + nodeTypes.get(termLegKey), nodeTypes.get(termLegKey).contains("TermQuery"));
        assertFalse(
            "round 2 addresses a materialized leg by _id, so it names no ANN leg: " + nodeTypes.get(rewriteKey),
            hasAnnNode(nodeTypes.get(rewriteKey))
        );
        assertEquals(
            "the coordinator's entry is one node, named apart from any shard-local hybrid: " + nodeTypes.get(coordinatorKey),
            List.of(COORDINATOR_NODE_TYPE),
            nodeTypes.get(coordinatorKey)
        );

        // A leg asks for no source and no stored fields, so its fetch answers no question the user can ask.
        assertEquals("knn leg fetch is emptied", 0, (int) fetchNodes.get(knnLegKey));
        assertEquals("term leg fetch is emptied", 0, (int) fetchNodes.get(termLegKey));
        assertEquals("and the coordinator fetches nothing at all", 0, (int) fetchNodes.get(coordinatorKey));
        assertTrue("the user's own fetch survives the merge: " + fetchNodes.get(rewriteKey), fetchNodes.get(rewriteKey) > 0);
    }

    /**
     * What the coordinator's entry actually says. This is the only account of the fan-out and the fusion anywhere in the
     * response: core creates the request's {@code SearchTimeProvider} before the rewrite runs, so the work lands inside
     * {@code took} but inside no {@code phase_took} phase and on no shard.
     *
     * <p>Durations are asserted as a structure — the phases sum to the node's time and the collector carries the
     * post-fan-out subtotal — never as values, which vary from run to run.
     */
    @SneakyThrows
    public void testProfiledFusedHybrid_thenTheCoordinatorEntryAccountsForTheFanOutAndTheFusion() {
        ensureDataset(INDEX, 1);

        Map<String, Object> response = search(INDEX, profiled(fusedHybrid(knnLeg(), termLeg())));
        Map<String, Object> entry = onlyEntryEndingWith(response, COORDINATOR_TAG);
        Map<String, Object> search = onlySearch(entry);
        Map<String, Object> node = onlyQueryNode(search);

        assertEquals(COORDINATOR_NODE_TYPE, node.get("type"));
        assertEquals(
            "the description names the shape of the work, so the entry is readable without the debug section",
            "2 legs, window " + WINDOW_SIZE + ", min_max / arithmetic_mean",
            node.get("description")
        );

        // Compared as a set, not a list: the response is parsed unordered here, so render order is pinned in
        // FusedLegProfileMergerTests instead, against the map the entry is actually built from.
        Map<String, Object> breakdown = mapAt(node, "breakdown");
        assertEquals(
            "the phases of a coordinator fusion",
            Set.of("fan_out_build", "fan_out_wait", "window_merge", "fuse_scores", "rank_window", "substitute_build"),
            breakdown.keySet()
        );
        long summed = breakdown.values().stream().mapToLong(value -> ((Number) value).longValue()).sum();
        assertEquals("the phases are leaves, so they sum to the node's own time", ((Number) node.get("time_in_nanos")).longValue(), summed);
        assertTrue("waiting on the legs is the bulk of it and cannot be zero", ((Number) breakdown.get("fan_out_wait")).longValue() > 0);
        assertNull("there is nothing below the phases", node.get("children"));

        Map<String, Object> debug = mapAt(node, "debug");
        assertEquals(WINDOW_SIZE, ((Number) debug.get("window_size")).intValue());
        assertEquals("all six documents match the term leg and survive fusion", TOTAL_DOCS, ((Number) debug.get("ranked_docs")).intValue());
        assertEquals("track_total_hits is uncapped here, so round 2 carries a Tail", Boolean.TRUE, debug.get("tail_built"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> legs = (List<Map<String, Object>>) debug.get("legs");
        assertEquals(
            "one entry per leg, in leg order",
            List.of(0, 1),
            legs.stream().map(leg -> ((Number) leg.get("leg")).intValue()).toList()
        );
        for (Map<String, Object> leg : legs) {
            assertNotNull("a leg's own took is what makes an outlier leg visible", leg.get("took_in_millis"));
            assertEquals("every leg completed", Boolean.FALSE, leg.get("timed_out"));
            assertEquals("both legs match all six documents within the window", TOTAL_DOCS, ((Number) leg.get("hits")).intValue());
        }

        Map<String, Object> collector = onlyCollector(search);
        assertEquals("HybridFusionCombiner", collector.get("name"));
        assertEquals("fuse_candidates", collector.get("reason"));
        assertEquals(
            "the collector slot carries what fusing cost once the legs were back",
            summed - ((Number) breakdown.get("fan_out_build")).longValue() - ((Number) breakdown.get("fan_out_wait")).longValue(),
            ((Number) collector.get("time_in_nanos")).longValue()
        );
        assertEquals("no Lucene rewrite happens on the coordinator", 0L, ((Number) search.get("rewrite_time")).longValue());
        assertEquals("and it runs no aggregations of its own", List.of(), entry.get("aggregations"));
    }

    /**
     * Legs times shards. Every leg sub-search profiles every shard, so the label alone cannot distinguish entries — the
     * shard key has to. Pins that the scheme scales without collapsing entries.
     */
    @SneakyThrows
    public void testProfiledFusedHybridOnMultipleShards_thenOneEntryPerLegPerShard() {
        ensureDataset(SHARDED_INDEX, SHARDS);

        Map<String, Object> response = search(SHARDED_INDEX, profiled(fusedHybrid(knnLeg(), termLeg())));
        List<String> keys = new ArrayList<>(queryNodeTypes(response).keySet());

        assertEquals("no two entries may share a key: " + keys, keys.size(), keys.stream().distinct().count());
        assertEquals("leg 0 reports once per shard: " + keys, SHARDS, countEndingWith(keys, legTag(0)));
        assertEquals("leg 1 reports once per shard: " + keys, SHARDS, countEndingWith(keys, legTag(1)));
        assertEquals("round 2 reports once per shard: " + keys, SHARDS, countEndingWith(keys, REWRITE_TAG));
        // The one entry the shard count does not multiply: the coordinator fuses once per request, however many shards the
        // legs happened to touch.
        assertEquals("the coordinator reports once per hybrid: " + keys, 1, countEndingWith(keys, COORDINATOR_TAG));
        assertEquals("legs x shards, plus round 2 per shard, plus the coordinator: " + keys, 3 * SHARDS + 1, keys.size());
        assertEquals(
            "every shard entry groups under the shard it ran on, and the coordinator under none of them: " + keys,
            SHARDS + 1,
            keys.stream().map(this::baseShardKey).distinct().count()
        );
    }

    /**
     * A fused hybrid whose own leg is a fused hybrid. A leg sub-search is a search action of its own, so the ActionFilter
     * re-enters on it and the inner hybrid is labelled by its own request-scoped merger — which is why both levels are
     * {@code hybrid_0}. The labels therefore compose, and they compose <b>outermost first</b>: appending would read
     * inside-out, naming the outer leg as if it were the inner one.
     */
    @SneakyThrows
    public void testProfiledNestedFusedHybrid_thenLabelsReadOutermostFirst() {
        ensureDataset(INDEX, 1);
        String inner = fusedHybrid(knnLeg(), termLeg());

        Map<String, Object> response = search(INDEX, profiled(fusedHybrid(inner, termLeg())));
        Map<String, List<String>> nodeTypes = queryNodeTypes(response);
        Map<String, Integer> fetchNodes = fetchNodeCounts(response);

        List<String> tagPaths = nodeTypes.keySet().stream().map(this::tagPath).sorted().toList();
        assertEquals(
            "one entry per node of the fused tree, labelled outermost first",
            List.of(
                "[fused:hybrid_0.leg_0][fused:hybrid_0.leg_0]", // the inner hybrid's knn leg
                "[fused:hybrid_0.leg_0][fused:hybrid_0.leg_1]", // the inner hybrid's term leg
                "[fused:hybrid_0.leg_0][fused:hybrid_0]",       // the inner hybrid's own coordinator fusion
                "[fused:hybrid_0.leg_0][fused:rewrite]",        // the inner hybrid's own round 2
                "[fused:hybrid_0.leg_1]",                       // the outer hybrid's own term leg
                COORDINATOR_TAG,                                // the outer hybrid's own coordinator fusion
                REWRITE_TAG                                     // the outer hybrid's own round 2
            ),
            tagPaths
        );
        // The inner fusion is charged where it happened. Its entry arrives inside its leg's response already keyed
        // [coordinator][fused:hybrid_0] and is retagged into the path, so the two fusions never read as one.
        assertEquals(
            "each nesting level's fusion is its own entry",
            2,
            nodeTypes.keySet().stream().filter(key -> key.endsWith(COORDINATOR_TAG)).count()
        );
        assertEquals("no two entries may share a key: " + nodeTypes.keySet(), nodeTypes.size(), tagPaths.stream().distinct().count());
        // The inner round 2 arrives as part of a leg response, so its fetch is emptied like any other leg's.
        List<String> withFetch = nodeTypes.keySet().stream().filter(key -> fetchNodes.get(key) > 0).map(this::tagPath).toList();
        assertEquals("only the user's own fetch is kept", List.of(REWRITE_TAG), withFetch);
        // Three levels down, the ANN leg is still named.
        String innerKnnLegKey = onlyKeyEndingWith(nodeTypes.keySet(), legTag(0) + legTag(0));
        assertTrue(
            "the inner knn leg must report an ANN node: " + nodeTypes.get(innerKnnLegKey),
            hasAnnNode(nodeTypes.get(innerKnnLegKey))
        );
    }

    /**
     * A classic (non-fused) hybrid runs its legs on the shards, so its own entry already names them and there is nothing
     * to merge. Nothing may be attached: a label here would claim a leg entry that never existed.
     */
    @SneakyThrows
    public void testProfiledClassicHybrid_thenNothingIsLabelled() {
        ensureDataset(INDEX, 1);

        Map<String, Object> response = search(INDEX, profiled(classicHybrid()));
        Map<String, List<String>> nodeTypes = queryNodeTypes(response);

        assertEquals("classic reports one entry per shard: " + nodeTypes.keySet(), 1, nodeTypes.size());
        assertTrue(
            "no entry of a classic hybrid may carry a fused label: " + nodeTypes.keySet(),
            nodeTypes.keySet().stream().noneMatch(key -> key.contains(TAG_OPEN))
        );
        // Unlabelled because nothing was attached, not because nothing ran: the ANN leg is inside the user's own entry.
        List<String> types = nodeTypes.values().iterator().next();
        assertTrue("classic reports the ANN leg in its own tree: " + types, hasAnnNode(types));
        assertTrue("classic reports the term leg in the same tree: " + types, types.contains("TermQuery"));
    }

    /**
     * Profiling a leg changes how that leg executes, and a leg decides the candidate window — so the answer is measured
     * with and without it. Also the control: an unprofiled request gets no profile section, and nothing else about the
     * response is rebuilt for it.
     */
    @SneakyThrows
    public void testFusedHybrid_whenProfiled_thenRankingAndTotalsAreUnchanged() {
        ensureDataset(INDEX, 1);
        String query = fusedHybrid(knnLeg(), termLeg());

        Map<String, Object> plain = search(INDEX, "{\"query\":" + query + ",\"track_total_hits\":true}");
        Map<String, Object> profiled = search(INDEX, profiled(query));

        assertNull("no profile was asked for", plain.get("profile"));
        assertNotNull("a profile was asked for", profiled.get("profile"));
        assertEquals("profiling the legs must not change the fused ranking or the scores", rankedHits(plain), rankedHits(profiled));
        assertEquals("profiling the legs must not change the totals", totalHits(plain), totalHits(profiled));
    }

    /**
     * What {@code [fused:rewrite]} covers: round 2 on that shard, not the substituted hybrid alone. A shard key owns one
     * whole profile entry — searches, aggregations and fetch together — so anything else the request asked of that shard
     * is under the same label, and there is no sub-key that could name it more narrowly. Pinned rather than left to be
     * discovered, because an aggregation written next to the hybrid is the case where that reads as surprising.
     */
    @SneakyThrows
    public void testProfiledFusedHybridWithAggregation_thenTheAggregationIsInsideTheRewriteEntry() {
        ensureDataset(INDEX, 1);

        Map<String, Object> response = search(INDEX, profiledWithAggregation(fusedHybrid(knnLeg(), termLeg())));
        Map<String, List<String>> aggregationTypes = aggregationNodeTypes(response);

        String rewriteKey = onlyKeyEndingWith(aggregationTypes.keySet(), REWRITE_TAG);
        assertTrue(
            "the aggregation ran in round 2, so it is inside the entry labelled as round 2: " + aggregationTypes,
            aggregationTypes.get(rewriteKey).stream().anyMatch(type -> type.contains("Aggregator"))
        );
        // Nothing else may claim it: a leg is asked for ids only, and the coordinator fuses rather than aggregates.
        assertEquals(
            "a leg reports no aggregation",
            List.of(),
            aggregationTypes.get(onlyKeyEndingWith(aggregationTypes.keySet(), legTag(0)))
        );
        assertEquals("nor does the other", List.of(), aggregationTypes.get(onlyKeyEndingWith(aggregationTypes.keySet(), legTag(1))));
        assertEquals("nor the coordinator", List.of(), aggregationTypes.get(onlyKeyEndingWith(aggregationTypes.keySet(), COORDINATOR_TAG)));
        assertNotNull("and the aggregation still answers", response.get("aggregations"));
    }

    // ------------------------------------------------ the fast-path verdict ------------------------------------------------

    /**
     * A profiled request keeps two rounds, and its coordinator entry says what the same request without {@code profile}
     * would do on this coordinator, and why. Here: the shape and legs allow it, the gate has observed this
     * {@code _source} shape (the priming requests below, one per node, as production traffic would), and with totals off
     * the count is settled — so the twin {@code would_take} the fast path, with the volume it was weighed at.
     */
    @SneakyThrows
    public void testProfiledFusedHybrid_whenTheTwinIsEligible_thenTheCoordinatorEntryReportsWouldTake() {
        ensureDataset(INDEX, 1);
        String body = "{\"query\":" + fusedHybrid(knnLeg(), termLeg()) + ",\"size\":3,\"track_total_hits\":false}";
        prime(INDEX, body);

        Map<String, Object> verdict = fastPathVerdict(
            search(INDEX, profiled(fusedHybrid(knnLeg(), termLeg()), "\"size\":3,\"track_total_hits\":false"))
        );

        assertEquals(Boolean.TRUE, verdict.get("would_take"));
        assertNull("nothing refused, so no reason", verdict.get("refused_by"));
        assertEquals(Boolean.TRUE, verdict.get("count_settled"));
        assertTrue(
            "the volume was weighed: extra documents × observed bytes",
            ((Number) verdict.get("fetch_estimate_bytes")).longValue() > 0
        );
        assertEquals("the default budget", 1L << 20, ((Number) verdict.get("fetch_budget_bytes")).longValue());
    }

    /** What the legs decide, read off their actual answers: six documents cannot prove a count of 10,000. */
    @SneakyThrows
    public void testProfiledFusedHybrid_whenNoLegProvesTheDefaultCount_thenTheVerdictIsCountNotSettled() {
        ensureDataset(INDEX, 1);
        prime(INDEX, "{\"query\":" + fusedHybrid(knnLeg(), termLeg()) + ",\"size\":3}");

        Map<String, Object> verdict = fastPathVerdict(search(INDEX, profiled(fusedHybrid(knnLeg(), termLeg()), "\"size\":3")));

        assertEquals(Boolean.FALSE, verdict.get("would_take"));
        assertEquals("count_not_settled", verdict.get("refused_by"));
        assertEquals(Boolean.FALSE, verdict.get("count_settled"));
        assertNotNull("the volume had passed before the legs decided", verdict.get("fetch_estimate_bytes"));
    }

    /** Each refusal the request itself causes is named — the feature, the leg, or the nesting — before anything is weighed. */
    @SneakyThrows
    public void testProfiledFusedHybrid_whenTheRequestRefusesTheFastPath_thenTheVerdictNamesWhat() {
        ensureDataset(INDEX, 1);

        Map<String, Object> exact = fastPathVerdict(
            search(INDEX, profiled(fusedHybrid(knnLeg(), termLeg()), "\"size\":3,\"track_total_hits\":true"))
        );
        assertEquals("exact_totals", exact.get("refused_by"));
        assertNull("refused before the volume was weighed", exact.get("fetch_budget_bytes"));

        String namedLeg = "{\"term\":{\"" + TEXT_FIELD + "\":{\"value\":\"hello\",\"_name\":\"lex\"}}}";
        Map<String, Object> named = fastPathVerdict(
            search(INDEX, profiled(fusedHybrid(knnLeg(), namedLeg), "\"size\":3,\"track_total_hits\":false"))
        );
        assertEquals("leg_name", named.get("refused_by"));
        assertTrue(String.valueOf(named.get("detail")), String.valueOf(named.get("detail")).startsWith("leg 1 "));

        Map<String, Object> aggregated = fastPathVerdict(search(INDEX, profiledWithAggregation(fusedHybrid(knnLeg(), termLeg()))));
        assertEquals("request_shape", aggregated.get("refused_by"));
        assertTrue(String.valueOf(aggregated.get("detail")), String.valueOf(aggregated.get("detail")).startsWith("aggregations"));

        String nested = "{\"bool\":{\"must\":[" + fusedHybrid(knnLeg(), termLeg()) + "]}}";
        Map<String, Object> inBool = fastPathVerdict(search(INDEX, profiled(nested, "\"size\":3,\"track_total_hits\":false")));
        assertEquals("nested_hybrid", inBool.get("refused_by"));
    }

    /** On an index no coordinator has seen a response for, the twin would fail closed — and the profile says so. */
    @SneakyThrows
    public void testProfiledFusedHybrid_whenTheSourceSizeIsUnobserved_thenTheVerdictSaysSo() {
        String cold = INDEX + "-cold";
        ensureDataset(cold, 1);

        Map<String, Object> verdict = fastPathVerdict(
            search(cold, profiled(fusedHybrid(knnLeg(), termLeg()), "\"size\":3,\"track_total_hits\":false"))
        );

        assertEquals(Boolean.FALSE, verdict.get("would_take"));
        assertEquals("source_size_unobserved", verdict.get("refused_by"));
        assertEquals("the budget it would have been weighed against", 1L << 20, ((Number) verdict.get("fetch_budget_bytes")).longValue());
        assertNull("no estimate without an observation", verdict.get("fetch_estimate_bytes"));
    }

    /** The budget is a live cluster setting: at zero, the same eligible request is refused on volume, and the verdict shows both numbers. */
    @SneakyThrows
    public void testProfiledFusedHybrid_whenTheFetchBudgetIsZero_thenTheVerdictIsFetchBudget() {
        ensureDataset(INDEX, 1);
        String body = "{\"query\":" + fusedHybrid(knnLeg(), termLeg()) + ",\"size\":3,\"track_total_hits\":false}";
        prime(INDEX, body);
        updateClusterSettings("plugins.neural_search.hybrid.fusion.fast_path_fetch_budget", "0b");
        try {
            Map<String, Object> verdict = fastPathVerdict(
                search(INDEX, profiled(fusedHybrid(knnLeg(), termLeg()), "\"size\":3,\"track_total_hits\":false"))
            );
            assertEquals("fetch_budget", verdict.get("refused_by"));
            assertEquals(0L, ((Number) verdict.get("fetch_budget_bytes")).longValue());
            assertTrue(((Number) verdict.get("fetch_estimate_bytes")).longValue() > 0);
            assertTrue(String.valueOf(verdict.get("detail")), String.valueOf(verdict.get("detail")).contains("over the 0-byte budget"));
        } finally {
            updateClusterSettings("plugins.neural_search.hybrid.fusion.fast_path_fetch_budget", null);
        }
        assertEquals(
            "back on the default budget",
            Boolean.TRUE,
            fastPathVerdict(search(INDEX, profiled(fusedHybrid(knnLeg(), termLeg()), "\"size\":3,\"track_total_hits\":false"))).get(
                "would_take"
            )
        );
    }

    /**
     * Run a request once per cluster node: the fetch gate learns an index's {@code _source} size per coordinator from
     * responses, and the REST client reaches the nodes round-robin, so two turns warm every coordinator whatever position
     * other requests left the rotation in.
     */
    @SneakyThrows
    private void prime(final String index, final String body) {
        int nodes = Integer.parseInt(System.getProperty("cluster.number_of_nodes", "1"));
        for (int i = 0; i < 2 * nodes; i++) {
            search(index, body);
        }
    }

    /** The {@code fast_path} block of the coordinator entry's {@code debug}. */
    @SuppressWarnings("unchecked")
    private Map<String, Object> fastPathVerdict(final Map<String, Object> response) {
        Map<String, Object> node = onlyQueryNode(onlySearch(onlyEntryEndingWith(response, COORDINATOR_TAG)));
        Map<String, Object> verdict = (Map<String, Object>) mapAt(node, "debug").get("fast_path");
        assertNotNull("the coordinator entry carries the fast-path verdict", verdict);
        return verdict;
    }

    // ------------------------------------------------ request bodies ------------------------------------------------

    /** A materializable ANN leg: in the Tail it is replaced by an address of the hits it returned. */
    private String knnLeg() {
        return "{\"knn\":{\"" + VECTOR_FIELD + "\":{\"vector\":[1.1,1.0],\"k\":" + WINDOW_SIZE + "}}}";
    }

    /** A leg that matches every document, kept as a real query in the Tail. */
    private String termLeg() {
        return "{\"term\":{\"" + TEXT_FIELD + "\":\"hello\"}}";
    }

    /** A fused hybrid over the given legs. The {@code fusion} block is inline, so it resolves at every nesting level. */
    private String fusedHybrid(final String... legs) {
        return "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + WINDOW_SIZE
            + ",\"normalization\":{\"technique\":\"min_max\"},\"combination\":{\"technique\":\"arithmetic_mean\"}},"
            + "\"queries\":["
            + String.join(",", legs)
            + "]}}";
    }

    /** The same two legs with no {@code fusion} block: classic hybrid, normalized by the index's default pipeline. */
    private String classicHybrid() {
        return "{\"hybrid\":{\"queries\":[" + knnLeg() + "," + termLeg() + "]}}";
    }

    private String profiled(final String query) {
        return "{\"query\":" + query + ",\"profile\":true,\"track_total_hits\":true}";
    }

    /** A profiled request with the caller's extra top-level fields, or none. */
    private String profiled(final String query, final String extra) {
        return "{\"query\":" + query + ",\"profile\":true" + (Objects.isNull(extra) ? "" : "," + extra) + "}";
    }

    /**
     * The same, plus an aggregation the hybrid knows nothing about.
     *
     * <p>A metric aggregation on purpose. A bucket aggregation that builds its own {@code Weight} — {@code filter} is the
     * short one — trips an assertion in core's {@code ConcurrentQueryProfileBreakdown} that has nothing to do with fused
     * mode: it reproduces on a plain {@code match_all} with no hybrid anywhere. Reading doc values needs no weight, so a
     * metric aggregation profiles what this test is here to pin without standing on that.
     */
    private String profiledWithAggregation(final String query) {
        return "{\"query\":"
            + query
            + ",\"profile\":true,\"track_total_hits\":true,"
            + "\"aggs\":{\"rank_total\":{\"sum\":{\"field\":\""
            + RANK_FIELD
            + "\"}}}}";
    }

    // ------------------------------------------------ profile parsing -----------------------------------------------

    /** Node {@code type}s of every query tree of an entry, depth first, keyed by the entry's shard key in render order. */
    @SuppressWarnings("unchecked")
    private Map<String, List<String>> queryNodeTypes(final Map<String, Object> response) {
        Map<String, List<String>> byShardKey = new LinkedHashMap<>();
        for (Map<String, Object> shard : shardEntries(response)) {
            List<String> types = new ArrayList<>();
            List<Map<String, Object>> searches = (List<Map<String, Object>>) shard.get("searches");
            if (Objects.nonNull(searches)) {
                for (Map<String, Object> search : searches) {
                    collectNodeTypes((List<Map<String, Object>>) search.get("query"), types);
                }
            }
            byShardKey.put(String.valueOf(shard.get("id")), types);
        }
        return byShardKey;
    }

    @SuppressWarnings("unchecked")
    private void collectNodeTypes(final List<Map<String, Object>> nodes, final List<String> types) {
        if (Objects.isNull(nodes)) {
            return;
        }
        for (Map<String, Object> node : nodes) {
            types.add(String.valueOf(node.get("type")));
            collectNodeTypes((List<Map<String, Object>>) node.get("children"), types);
        }
    }

    /** Node {@code type}s of an entry's aggregation section, depth first, keyed by the entry's shard key. */
    @SuppressWarnings("unchecked")
    private Map<String, List<String>> aggregationNodeTypes(final Map<String, Object> response) {
        Map<String, List<String>> byShardKey = new LinkedHashMap<>();
        for (Map<String, Object> shard : shardEntries(response)) {
            List<String> types = new ArrayList<>();
            collectNodeTypes((List<Map<String, Object>>) shard.get("aggregations"), types);
            byShardKey.put(String.valueOf(shard.get("id")), types);
        }
        return byShardKey;
    }

    /** How many fetch nodes an entry reports, {@code -1} when the section is missing rather than empty. */
    @SuppressWarnings("unchecked")
    private Map<String, Integer> fetchNodeCounts(final Map<String, Object> response) {
        Map<String, Integer> byShardKey = new LinkedHashMap<>();
        for (Map<String, Object> shard : shardEntries(response)) {
            List<Map<String, Object>> fetch = (List<Map<String, Object>>) shard.get("fetch");
            byShardKey.put(String.valueOf(shard.get("id")), Objects.isNull(fetch) ? -1 : fetch.size());
        }
        return byShardKey;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> shardEntries(final Map<String, Object> response) {
        Map<String, Object> profile = (Map<String, Object>) response.get("profile");
        assertNotNull("the request asked for a profile and the response carries none", profile);
        List<Map<String, Object>> shards = (List<Map<String, Object>>) profile.get("shards");
        assertNotNull("a profile section must carry shard entries", shards);
        return shards;
    }

    private String legTag(final int legIndex) {
        return String.format(Locale.ROOT, "[fused:hybrid_0.leg_%d]", legIndex);
    }

    /** The label path of an entry, its shard key stripped: the fused tree the user wrote, outermost first. */
    private String tagPath(final String shardKey) {
        int tag = shardKey.indexOf(TAG_OPEN);
        return tag < 0 ? "" : shardKey.substring(tag);
    }

    private String baseShardKey(final String shardKey) {
        int tag = shardKey.indexOf(TAG_OPEN);
        return tag < 0 ? shardKey : shardKey.substring(0, tag);
    }

    private boolean hasAnnNode(final List<String> nodeTypes) {
        return nodeTypes.stream().anyMatch(type -> type.toLowerCase(Locale.ROOT).contains(ANN_NODE_MARKER));
    }

    private String onlyKeyEndingWith(final Collection<String> shardKeys, final String suffix) {
        List<String> matches = shardKeys.stream().filter(key -> key.endsWith(suffix)).toList();
        assertEquals("exactly one entry must be labelled " + suffix + ", got " + shardKeys, 1, matches.size());
        return matches.get(0);
    }

    private long countEndingWith(final Collection<String> shardKeys, final String suffix) {
        return shardKeys.stream().filter(key -> key.endsWith(suffix)).count();
    }

    /** The one profile entry whose key ends with {@code suffix}, as rendered — not just its node types. */
    private Map<String, Object> onlyEntryEndingWith(final Map<String, Object> response, final String suffix) {
        List<Map<String, Object>> matches = shardEntries(response).stream()
            .filter(shard -> String.valueOf(shard.get("id")).endsWith(suffix))
            .toList();
        assertEquals("exactly one entry must be labelled " + suffix, 1, matches.size());
        return matches.get(0);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> onlySearch(final Map<String, Object> entry) {
        List<Map<String, Object>> searches = (List<Map<String, Object>>) entry.get("searches");
        assertEquals("the coordinator's entry describes one fusion: " + searches, 1, searches.size());
        return searches.get(0);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> onlyQueryNode(final Map<String, Object> search) {
        List<Map<String, Object>> query = (List<Map<String, Object>>) search.get("query");
        assertEquals("one node, because a coordinator fusion is one span: " + query, 1, query.size());
        return query.get(0);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> onlyCollector(final Map<String, Object> search) {
        List<Map<String, Object>> collector = (List<Map<String, Object>>) search.get("collector");
        assertEquals("core renders the collector slot unconditionally, so it carries exactly one: " + collector, 1, collector.size());
        return collector.get(0);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> mapAt(final Map<String, Object> node, final String field) {
        Map<String, Object> value = (Map<String, Object>) node.get(field);
        assertNotNull("the coordinator node must render its " + field, value);
        return value;
    }

    // -------------------------------------------------- dataset -----------------------------------------------------

    private String indexConfig(final int shards) {
        return "{\"settings\":{\"index\":{\"knn\":true,\"number_of_shards\":"
            + shards
            + ",\"number_of_replicas\":0,\"search.default_pipeline\":\""
            + NORM_PIPELINE
            + "\"}},\"mappings\":{\"properties\":{\""
            + TEXT_FIELD
            + "\":{\"type\":\"text\"},\""
            + RANK_FIELD
            + "\":{\"type\":\"integer\"},\""
            + VECTOR_FIELD
            + "\":{\"type\":\"knn_vector\",\"dimension\":2,"
            + "\"method\":{\"name\":\"hnsw\",\"space_type\":\"l2\",\"engine\":\"lucene\"}}}}}";
    }

    /** The default pipeline is what normalizes the classic hybrid; a fused hybrid resolves its inline config instead. */
    @SneakyThrows
    private void ensureDataset(final String index, final int shards) {
        createSearchPipeline(NORM_PIPELINE, "min_max", "arithmetic_mean", Map.of());
        if (indexExists(index)) {
            return;
        }
        createIndex(index, indexConfig(shards));
        for (int id = 1; id <= TOTAL_DOCS; id++) {
            Request request = new Request("PUT", "/" + index + "/_doc/" + id + "?refresh=true");
            request.setJsonEntity(
                "{\""
                    + TEXT_FIELD
                    + "\":\"hello world document "
                    + id
                    + "\",\""
                    + RANK_FIELD
                    + "\":"
                    + id
                    + ",\""
                    + VECTOR_FIELD
                    + "\":[1."
                    + id
                    + ",1.0]}"
            );
            Response response = client().performRequest(request);
            int code = response.getStatusLine().getStatusCode();
            assertTrue(
                "indexing " + index + "/" + id + " failed: " + code,
                code == RestStatus.OK.getStatus() || code == RestStatus.CREATED.getStatus()
            );
        }
    }

    @SneakyThrows
    private Map<String, Object> search(final String index, final String jsonBody) {
        Request request = new Request("POST", "/" + index + "/_search");
        request.setJsonEntity(jsonBody);
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    /** The hit list as {@code _id@_score}, which is both the ranking and the scores in one comparable value. */
    @SuppressWarnings("unchecked")
    private List<String> rankedHits(final Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        List<String> ranked = new ArrayList<>();
        if (Objects.nonNull(hitList)) {
            for (Map<String, Object> hit : hitList) {
                ranked.add(hit.get("_id") + "@" + hit.get("_score"));
            }
        }
        return ranked;
    }

    @SuppressWarnings("unchecked")
    private long totalHits(final Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        Map<String, Object> total = (Map<String, Object>) hits.get("total");
        assertNotNull("track_total_hits was asked for", total);
        return ((Number) total.get("value")).longValue();
    }
}
