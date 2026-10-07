/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;

/**
 * Rolling-upgrade coverage for the hybrid query's fused (resolver) mode, enabled by a {@code fusion} block on the query
 * body.
 *
 * <p>Fused mode runs in two rounds: the coordinator fans the legs out, fuses them, and then self-erases the {@code hybrid}
 * query into an internal {@code hybrid_fusion} query that every shard has to deserialize. A node predating fused mode
 * cannot resolve that {@code NamedWriteable} name, and the resulting shard failure is <b>silent</b> under the default
 * {@code allow_partial_search_results} — an HTTP 200 short of documents, or, when the shard is retried onto an upgraded
 * copy, an HTTP 200 with no recorded failure at all. So the coordinator refuses fused mode outright while any node in the
 * cluster is below {@link #FUSED_MODE_MIN_VERSION}.
 *
 * <p>The invariant asserted at every stage is therefore not "fused mode works" but the stronger, upgrade-safe one:
 * <b>a fused query is either served completely or refused with a 400 — never partially served</b>. Which of the two applies
 * is decided by the cluster's own observed minimum node version, which is a sound reading only once the base cluster is
 * known to predate fused mode — see {@link #skipUnlessBaseClusterPredatesFusedMode()}.
 *
 * <p>On a mixed cluster either coordinator is acceptable and they say different things — a pre-3.8 node fails while parsing
 * the unknown {@code fusion} field, an upgraded one fails on the version guardrail, or on the opt-in if the
 * cluster-manager was too old to accept it — so the assertions are deliberately coordinator-agnostic: a 400, and no trace of
 * the internal round-2 query name, which would mean the fused query was dispatched and a shard rejected it.
 *
 * <p>Deliberately not excluded for any {@code bwc_version} row in {@code qa/rolling-upgrade/build.gradle}: the convention
 * there is to exclude a new feature's test class for base versions predating the feature, which for a 3.8 feature would
 * exclude every row CI runs today and leave the mixed-cluster refusal — the point of this test — unexercised.
 */
public class HybridSearchFusedModeIT extends AbstractRollingUpgradeTestCase {

    private static final String TEXT_FIELD = "passage_text";
    /**
     * First version that can run fused mode. Mirrors {@code MinClusterVersionUtil}'s
     * {@code MINIMAL_SUPPORTED_VERSION_FUSED_MODE_IN_HYBRID_QUERY}, spelled out rather than imported because that class
     * initializes against k-NN classes which this module has as {@code compileOnly} — they are absent at test runtime.
     */
    private static final Version FUSED_MODE_MIN_VERSION = Version.V_3_10_0;
    /** Documents 0..2 match at least one leg; document 3 matches neither. */
    private static final String[] DOCS = { "hello world hello", "hello there place", "welcome to the place", "nothing relevant at all" };
    private static final int MATCHING_DOCS = 3;

    /**
     * A rolling upgrade says something about a version-gated feature only when the base cluster predates it. Every
     * {@code bwc_version} row CI runs is a real release below {@link #FUSED_MODE_MIN_VERSION}, but the local default is
     * {@code systemProp.bwc.version=3.8.0-SNAPSHOT} ({@code gradle.properties}), and the base cluster is then a nightly
     * distribution of that same version number carrying the plugin built from {@code main} — nodes reporting 3.8 whose
     * plugin has neither the opt-in nor fused mode. No node version can tell those apart from the build under test, and
     * neither branch of the invariant then holds: the coordinator's own guardrail is version-based too, so a mixed stage
     * would hand a fused query to a shard that cannot read it. Pass a real released base to exercise this class locally,
     * e.g. {@code -Dtests.bwc.version=3.7.0}.
     */
    @Before
    public void skipUnlessBaseClusterPredatesFusedMode() {
        Optional<String> baseVersion = getBWCVersion();
        assumeTrue("no configured base version to compare against " + FUSED_MODE_MIN_VERSION, baseVersion.isPresent());
        assumeTrue(
            String.format(LOCALE, "needs a base version below %s, got [%s]", FUSED_MODE_MIN_VERSION, baseVersion.get()),
            // Version.fromString rejects a snapshot qualifier outright, so strip it the way qa/build.gradle does.
            Version.fromString(baseVersion.get().replace("-SNAPSHOT", "")).before(FUSED_MODE_MIN_VERSION)
        );
    }

    public void testFusedModeHybridQuery_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        switch (getClusterType()) {
            case OLD:
                createIndex(getIndexNameForTest(), indexConfig());
                for (int docId = 0; docId < DOCS.length; docId++) {
                    addDocument(getIndexNameForTest(), String.valueOf(docId), TEXT_FIELD, DOCS[docId], null, null);
                }
                assertFusedModeIsServedOrRefused(MATCHING_DOCS);
                break;
            case MIXED:
                // Runs twice: one upgraded node in the first round, two in the second. Neither round may serve a partial
                // answer, and the refusal must not depend on which third of the cluster the request happened to reach.
                assertFusedModeIsServedOrRefused(MATCHING_DOCS);
                break;
            case UPGRADED:
                try {
                    // Every node can now read the self-erased query, so this branch is the positive case on every CI row:
                    // fused mode has to actually work, over a document written before the upgrade and one written after.
                    addDocument(getIndexNameForTest(), String.valueOf(DOCS.length), TEXT_FIELD, "hello again", null, null);
                    assertFusedModeIsServedOrRefused(MATCHING_DOCS + 1);
                } finally {
                    wipeOfTestResources(getIndexNameForTest(), null, null, null);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }
    }

    /**
     * Assert the upgrade-safe invariant against whatever cluster is actually running: a cluster whose every node can read
     * the self-erased query must serve the fused query completely, and any other cluster must refuse it.
     *
     * @param expectedMatchedDocs documents matching at least one leg, used only when the cluster is expected to serve
     */
    private void assertFusedModeIsServedOrRefused(final int expectedMatchedDocs) throws Exception {
        // Fused mode is an opt-in, off unless the cluster is switched on. Attempted even when this cluster is
        // expected to refuse: on a mixed cluster whose elected cluster-manager is already upgraded the key is accepted, and
        // that is what keeps the refusal below on the version guardrail this test exists for rather than on the setting.
        // Swallowed there because a cluster-manager still running the old plugin rejects a key it has never heard of, and
        // demanded once every node can run fused mode, so the positive branch fails on the switch rather than on a query.
        if (minimumNodeVersion().onOrAfter(FUSED_MODE_MIN_VERSION)) {
            enableFusedMode();
        } else {
            tryEnableFusedMode();
        }
        // Once per node: the REST client rotates over the cluster's hosts, so this covers a coordinator of each version a
        // mixed cluster has. Which one answers decides *why* the query is refused, never whether — a pre-3.8 coordinator
        // fails parsing `fusion`, an upgraded one on the version guardrail or the opt-in — and only the upgraded coordinator
        // exercises those at all, so a single request could miss them entirely.
        for (int node = 0; node < getClusterHosts().size(); node++) {
            assertFusedModeIsServedOrRefusedOnce(expectedMatchedDocs);
        }
    }

    /** One request's worth of the invariant. Which branch applies is read off the running cluster, not off a stage name. */
    private void assertFusedModeIsServedOrRefusedOnce(final int expectedMatchedDocs) throws Exception {
        if (minimumNodeVersion().onOrAfter(FUSED_MODE_MIN_VERSION)) {
            Map<String, Object> response = search(getIndexNameForTest(), fusedTwoLegQuery(), 10);
            assertEquals("fused mode must return every matched document", expectedMatchedDocs, getHitCount(response));
            // The failure this guardrail exists to prevent is silent, so hit count alone does not prove it absent: a shard
            // that cannot read the self-erased query is booked as a shard failure and the request still returns 200.
            assertEquals("fused mode must not lose a shard", 0, failedShards(response));
            return;
        }
        ResponseException exception = expectThrows(ResponseException.class, this::searchFused);
        Response response = exception.getResponse();
        String body = EntityUtils.toString(response.getEntity());
        assertEquals(
            "a cluster that cannot run fused mode on every shard must refuse the query rather than answer it: " + body,
            RestStatus.BAD_REQUEST.getStatus(),
            response.getStatusLine().getStatusCode()
        );
        assertFalse("the self-erased query must never reach a shard: " + body, body.contains("hybrid_fusion"));
        assertFalse("no shard-level NamedWriteable failure is acceptable: " + body, body.contains("Unknown NamedWriteable"));
    }

    /**
     * A fused two-leg hybrid query with the fusion config inline, so it needs no search pipeline and no index default —
     * the shortest body that enables the resolver. Raw JSON rather than {@code HybridQueryBuilder} because this request
     * must be built identically no matter which version of the plugin is being talked to.
     */
    private String fusedTwoLegQuery() {
        return "{\"query\":{\"hybrid\":{\"fusion\":{\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\"}},"
            + "\"queries\":[{\"match\":{\""
            + TEXT_FIELD
            + "\":\"hello\"}},{\"term\":{\""
            + TEXT_FIELD
            + "\":{\"value\":\"place\"}}}]}}}";
    }

    /** The fused search without the status assertion {@code search(...)} makes, so the refusal can be inspected. */
    private Response searchFused() throws IOException {
        Request request = new Request("POST", "/" + getIndexNameForTest() + "/_search");
        request.setJsonEntity(fusedTwoLegQuery());
        return client().performRequest(request);
    }

    private int failedShards(final Map<String, Object> searchResponseAsMap) {
        Map<String, Object> shards = (Map<String, Object>) searchResponseAsMap.get("_shards");
        return ((Number) shards.get("failed")).intValue();
    }

    private String indexConfig() {
        return "{\"settings\":{\"number_of_shards\":3,\"number_of_replicas\":1},\"mappings\":{\"properties\":{\""
            + TEXT_FIELD
            + "\":{\"type\":\"text\"}}}}";
    }
}
