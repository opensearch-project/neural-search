/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.DEFAULT_MAX_FUSION_LEG_SEARCHES;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.MAX_FUSION_LEG_SEARCHES;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;

import lombok.SneakyThrows;

/**
 * Where in a search body a fused ({@code fusion}) {@code hybrid} query may appear: in the request's own {@code query},
 * and nowhere else.
 *
 * <p>Core rewrites six positions of a body against the same coordinator context — {@code query}, {@code post_filter},
 * aggregations, sorts, {@code rescore} and {@code highlight} — so fused mode is entered from any of them, while every
 * guard that bounds fan-out reads the request's {@code query}. Two things went wrong there, and both are pinned here
 * against a live cluster because neither is visible to a unit test that only rewrites a query:
 *
 * <ol>
 *   <li>{@code plugins.neural_search.hybrid.fusion.max_leg_searches} counted zero for a body that declared
 *       {@code 2 x levels} leg sub-searches from any position other than {@code query}.</li>
 *   <li>Worse, a leg sub-search inherits the request's {@code post_filter} (deliberately, so that round 2's post-filter
 *       does not decimate an unfiltered leg window). A fused {@code hybrid} in {@code post_filter} was therefore copied
 *       onto every leg it created, and with an inline {@code fusion} config — which resolves at every level — each leg
 *       re-entered fused mode with the same body. The recursion is synchronous on one thread, so a ~250-byte request
 *       exhausted the stack, and OpenSearch treats {@code StackOverflowError} as fatal: every node the request reached
 *       exited its process. Measured 2026-08-26 on a 2-node cluster, both nodes killed ~0.5s apart.</li>
 * </ol>
 *
 * <p>One refusal in {@code HybridQueryBuilder.doRewriteFused} closes both: the fan-out is cut at level 0, before any leg
 * is dispatched, and the surviving count over {@code source().query()} becomes exact rather than a lower bound. Exact needs
 * a walk that descends for itself, since core's own {@code visit} implementations do not agree on descending — pinned here
 * by the {@code boosting} bodies, which are served at any depth and counted at every one.
 *
 * <p>Run 2-node: {@code ./gradlew integTest -PnumNodes=2 --tests "*HybridQueryFusedQueryPositionIT*"}.
 */
public class HybridQueryFusedQueryPositionIT extends BaseNeuralSearchIT {

    private static final String INDEX = "test-fused-query-position";
    private static final String NORM_PIPELINE = "fused-query-position-pipeline";
    private static final String GRP_FIELD = "grp";
    private static final String SCORE_FIELD = "s";
    /** Mapped but never populated: the only way to put a query in a sort is a nested filter, so a sort needs a path. */
    private static final String NESTED_FIELD = "kids";
    private static final int WINDOW_SIZE = 5;
    private static final int WINDOW_LAST_ID = 5;
    private static final int TOTAL_DOCS = 30;
    /** The refusal's own words, quoted so a reworded message cannot silently turn into a different guard firing. */
    private static final String REFUSAL = "must be part of the request's [query]";

    /**
     * Every position other than {@code query} that core rewrites with the coordinator context is refused. The chain is
     * over the fan-out ceiling and its outermost {@code window_size} is over {@code index.max_result_window}, so a
     * regression cannot pass this by refusing for either of those reasons instead: the assertion is on this guard's own
     * message, and on the ceiling's message being absent.
     */
    @SneakyThrows
    public void testFusedHybridOutsideTheRequestQuery_isRefused() {
        ensureDataset();
        int levels = overBudgetLevels();
        String chain = nestedFusedChain(levels, OVER_RESULT_WINDOW);
        Map<String, String> bodies = new LinkedHashMap<>();
        bodies.put("post_filter", "{\"query\":{\"match_all\":{}},\"post_filter\":" + chain + "}");
        bodies.put("aggs.filter", "{\"query\":{\"match_all\":{}},\"aggs\":{\"probe\":{\"filter\":" + chain + "}}}");
        bodies.put(
            "rescore",
            "{\"query\":{\"match_all\":{}},\"rescore\":{\"window_size\":10,\"query\":{\"rescore_query\":" + chain + "}}}"
        );
        bodies.put(
            "highlight_query",
            "{\"query\":{\"match_all\":{}},\"highlight\":{\"fields\":{\"" + GRP_FIELD + "\":{\"highlight_query\":" + chain + "}}}}"
        );
        bodies.put(
            "sort",
            "{\"query\":{\"match_all\":{}},\"sort\":[{\""
                + NESTED_FIELD
                + ".k\":{\"nested\":{\"path\":\""
                + NESTED_FIELD
                + "\",\"filter\":"
                + chain
                + "}}}]}"
        );

        for (Map.Entry<String, String> position : bodies.entrySet()) {
            ResponseException e = expectThrows(ResponseException.class, () -> search(position.getValue()));
            String message = body(e);

            assertEquals(position.getKey(), RestStatus.BAD_REQUEST.getStatus(), e.getResponse().getStatusLine().getStatusCode());
            assertTrue(position.getKey() + " must be refused on its position: " + message, message.contains(REFUSAL));
            assertFalse(
                position.getKey() + " is refused before the ceiling it used to evade: " + message,
                message.contains(MAX_FUSION_LEG_SEARCHES.getKey())
            );
        }
    }

    /**
     * The node kill, as a request that now gets an answer. A fused {@code hybrid} with an INLINE {@code fusion} config in
     * {@code post_filter}: every leg inherited it, resolved it, and fanned out again, synchronously on one thread, until
     * the stack was gone and the node exited. So the test asserts the two things that were lost — a prompt 400, and a
     * cluster that is still there — and it asserts them in that order deliberately: on unfixed code the first assertion
     * cannot even fail, because there is nothing left to answer the request.
     */
    @SneakyThrows
    public void testFusedHybridWithAnInlineConfigInPostFilter_isRefusedInsteadOfRecursing() {
        ensureDataset();
        int nodesBefore = clusterNodeCount();
        String body = "{\"query\":{\"match_all\":{}},\"post_filter\":" + fusedHybrid(leg(), WINDOW_SIZE) + "}";

        ResponseException e = expectThrows(ResponseException.class, () -> search(body));

        assertEquals(RestStatus.BAD_REQUEST.getStatus(), e.getResponse().getStatusLine().getStatusCode());
        assertTrue("refused on its position: " + body(e), body(e).contains(REFUSAL));
        assertEquals("every node that saw the request is still running", nodesBefore, clusterNodeCount());
        assertEquals("and still serving searches", RestStatus.OK.getStatus(), search("{\"query\":{\"match_all\":{}}}"));
    }

    /**
     * A {@code wrapper} query carries what it holds as bytes and exposes no children, so a fused {@code hybrid} inside one
     * cannot be placed in the body. It is refused rather than assumed to be in the {@code query} — assuming would readmit
     * the recursion above through {@code {"query": {"wrapper": ...}, "post_filter": {"hybrid": ...}}}, whose hybrid is
     * equally invisible. This is a deliberate divergence: the first body below was served before the guard.
     */
    @SneakyThrows
    public void testFusedHybridInsideAWrapperQuery_isRefused() {
        ensureDataset();
        Map<String, String> bodies = new LinkedHashMap<>();
        bodies.put("wrapper in query", "{\"query\":" + wrapped(fusedHybrid(leg(), WINDOW_SIZE)) + "}");
        bodies.put(
            "wrapper in post_filter",
            "{\"query\":{\"match_all\":{}},\"post_filter\":" + wrapped(fusedHybrid(leg(), WINDOW_SIZE)) + "}"
        );

        for (Map.Entry<String, String> shape : bodies.entrySet()) {
            ResponseException e = expectThrows(ResponseException.class, () -> search(shape.getValue()));

            assertEquals(shape.getKey(), RestStatus.BAD_REQUEST.getStatus(), e.getResponse().getStatusLine().getStatusCode());
            assertTrue(shape.getKey() + ": " + body(e), body(e).contains(REFUSAL));
            assertTrue("the message names the carriers that can hide it: " + body(e), body(e).contains("[wrapper]"));
        }
    }

    /**
     * A {@code template} query hides a fused {@code hybrid} for a different reason and is refused the same way: it carries
     * an unparsed map and only turns it into a query builder in a later rewrite round, and by then this request's
     * {@code source()} still holds the {@code template} — core installs the rewritten source only after the whole rewrite
     * loop has finished. Only a live cluster shows that, since it takes core's own multi-round rewrite.
     */
    @SneakyThrows
    public void testFusedHybridInsideATemplateQuery_isRefused() {
        ensureDataset();
        String body = "{\"query\":{\"template\":" + fusedHybrid(leg(), WINDOW_SIZE) + "}}";

        ResponseException e = expectThrows(ResponseException.class, () -> search(body));

        assertEquals(RestStatus.BAD_REQUEST.getStatus(), e.getResponse().getStatusLine().getStatusCode());
        assertTrue("refused on its position: " + body(e), body(e).contains(REFUSAL));
        assertTrue("the message names the carriers that can hide it: " + body(e), body(e).contains("[template]"));
    }

    /**
     * A {@code boosting} query hands its inner query to the visitor without recursing into it, so the reachability walk and
     * the leg count both descend for themselves rather than trusting it. What that buys, end to end: a fused hybrid is
     * served at any depth under such a query, and every one of them is still counted, so the ceiling refuses a body that
     * declares more than it allows even when each hybrid sits under a {@code boosting}. A walk that trusted core's traversal
     * saw one level under it and nothing below, so a hybrid nested there was admitted uncounted and bounded only against
     * its own leg request — the ceiling held per nesting level rather than per request, which is the evasion this whole
     * guard exists to stop.
     */
    @SneakyThrows
    public void testFusedHybridUnderAQueryThatDoesNotRecurse_isServedAndStillCounted() {
        ensureDataset();
        String fused = fusedHybrid(leg(), WINDOW_SIZE);

        assertEquals("one level under a boosting", RestStatus.OK.getStatus(), search("{\"query\":" + boosted(fused) + "}"));
        assertEquals("two levels under it", RestStatus.OK.getStatus(), search("{\"query\":" + boosted(boosted(fused)) + "}"));

        // The same count of two-leg fused hybrids as the nested chain, side by side instead, each hidden under a boosting.
        int hybrids = overBudgetLevels();
        List<String> branches = new ArrayList<>();
        for (int branch = 0; branch < hybrids; branch++) {
            branches.add(boosted(fused));
        }
        String body = "{\"query\":{\"bool\":{\"should\":[" + String.join(",", branches) + "]}}}";

        ResponseException e = expectThrows(ResponseException.class, () -> search(body));
        String message = body(e);

        assertEquals(RestStatus.BAD_REQUEST.getStatus(), e.getResponse().getStatusLine().getStatusCode());
        assertTrue("every hidden hybrid was counted: " + message, message.contains("declares " + (2 * hybrids) + " leg"));
        assertTrue("and it names the setting: " + message, message.contains(MAX_FUSION_LEG_SEARCHES.getKey()));
    }

    /**
     * What the guard must not break: the request's {@code query}, at the top level and nested inside a {@code bool} — which
     * is looser than what classic mode allows shard-side, and the shape users write to combine a fused hybrid with a
     * filter.
     */
    @SneakyThrows
    public void testFusedHybridInTheRequestQuery_isServed() {
        ensureDataset();
        String fused = fusedHybrid(leg(), WINDOW_SIZE);
        Map<String, String> bodies = new LinkedHashMap<>();
        bodies.put("top level", "{\"query\":" + fused + "}");
        bodies.put(
            "nested in a bool",
            "{\"query\":{\"bool\":{\"should\":[" + fused + "],\"filter\":[{\"term\":{\"" + GRP_FIELD + "\":\"A\"}}]}}}"
        );

        for (Map.Entry<String, String> shape : bodies.entrySet()) {
            Map<String, Object> response = searchForHits(shape.getValue());
            assertFalse(shape.getKey() + " must still be served with hits", hitIds(response).isEmpty());
        }
    }

    /**
     * And what it must not touch: classic {@code hybrid} outside the {@code query}. Classic mode does not fan out, so it
     * has nothing to bound — in these positions core matches it as the disjunction of its clauses, which is exactly the
     * {@code bool} the refusal above points a fused query at.
     */
    @SneakyThrows
    public void testClassicHybridOutsideTheRequestQuery_isUnaffected() {
        ensureDataset();
        Map<String, String> bodies = new LinkedHashMap<>();
        bodies.put("post_filter", "{\"query\":{\"match_all\":{}},\"post_filter\":" + classicHybrid() + "}");
        bodies.put("aggs.filter", "{\"query\":{\"match_all\":{}},\"aggs\":{\"probe\":{\"filter\":" + classicHybrid() + "}}}");

        for (Map.Entry<String, String> position : bodies.entrySet()) {
            assertEquals(position.getKey() + " is still served", RestStatus.OK.getStatus(), search(position.getValue()));
        }
    }

    /** The ceiling still fires where it is now the only guard that can: inside the request's own query. */
    @SneakyThrows
    public void testOverBudgetChainInTheRequestQuery_isStillRefusedByTheCeiling() {
        ensureDataset();
        int levels = overBudgetLevels();

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> search("{\"query\":" + nestedFusedChain(levels, WINDOW_SIZE) + "}")
        );
        String message = body(e);

        assertEquals(RestStatus.BAD_REQUEST.getStatus(), e.getResponse().getStatusLine().getStatusCode());
        assertTrue("the ceiling is what stopped it: " + message, message.contains("declares " + (2 * levels) + " leg"));
        assertTrue("and it names the setting: " + message, message.contains(MAX_FUSION_LEG_SEARCHES.getKey()));
    }

    // ------------------------------------------------ helpers ------------------------------------------------

    /** Above index.max_result_window (10000 by default): a second reason to refuse, so a pass cannot be that one. */
    private static final int OVER_RESULT_WINDOW = 20000;

    /** Smallest nesting depth whose declared leg count exceeds the default ceiling (each level adds 2 leg searches). */
    private int overBudgetLevels() {
        return DEFAULT_MAX_FUSION_LEG_SEARCHES / 2 + 1;
    }

    /** A leg that matches all docs and scores by the numeric field (deterministic, shard-independent). */
    private String leg() {
        return "{\"function_score\":{\"query\":{\"match_all\":{}},"
            + "\"field_value_factor\":{\"field\":\""
            + SCORE_FIELD
            + "\",\"modifier\":\"none\",\"missing\":1}}}";
    }

    /** {@code inner} plus one more leg, under an inline fusion config — fused mode with nothing left to resolve. */
    private String fusedHybrid(String inner, int windowSize) {
        return "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + windowSize
            + ",\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[0.5,0.5]}}},"
            + "\"queries\":["
            + inner
            + ","
            + leg()
            + "]}}";
    }

    /** The same two legs with no {@code fusion} block: classic hybrid, fused by the index's default pipeline. */
    private String classicHybrid() {
        return "{\"hybrid\":{\"queries\":[" + leg() + "," + leg() + "]}}";
    }

    /**
     * {@code boosting} hands its inner query to a query-tree visitor without recursing into it, so a walk that trusts core's
     * traversal stops one level below it. The negative clause never matches, so scores are the positive clause's.
     */
    private String boosted(String inner) {
        return "{\"boosting\":{\"positive\":" + inner + ",\"negative\":{\"match_none\":{}},\"negative_boost\":0.5}}";
    }

    /** {@code wrapper} carries its inner query as base64 bytes, so {@code visit} cannot see through it. */
    private String wrapped(String inner) {
        return "{\"wrapper\":{\"query\":\"" + Base64.getEncoder().encodeToString(inner.getBytes(StandardCharsets.UTF_8)) + "\"}}";
    }

    /**
     * A chain of {@code levels} fused hybrids declaring {@code 2 x levels} leg sub-searches. Only the outermost level takes
     * {@code outerWindow}, so the outer rewrite can be given a second reason to refuse without changing the inner levels.
     */
    private String nestedFusedChain(int levels, int outerWindow) {
        String query = leg();
        for (int level = 0; level < levels; level++) {
            query = fusedHybrid(query, level == levels - 1 ? outerWindow : WINDOW_SIZE);
        }
        return query;
    }

    /** Runs a search, returning its status. Throws {@link ResponseException} on any non-2xx, as the REST client does. */
    @SneakyThrows
    private int search(String jsonBody) {
        Request request = new Request("POST", "/" + INDEX + "/_search");
        request.setJsonEntity(jsonBody);
        request.addParameter("size", "20");
        return client().performRequest(request).getStatusLine().getStatusCode();
    }

    @SneakyThrows
    private Map<String, Object> searchForHits(String jsonBody) {
        Request request = new Request("POST", "/" + INDEX + "/_search");
        request.setJsonEntity(jsonBody);
        request.addParameter("size", "20");
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    @SneakyThrows
    private String body(ResponseException e) {
        return EntityUtils.toString(e.getResponse().getEntity());
    }

    /** How many nodes the cluster has right now — the assertion that a request did not take any of them down. */
    @SneakyThrows
    private int clusterNodeCount() {
        Response response = client().performRequest(new Request("GET", "/_cluster/health"));
        Map<String, Object> health = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(response.getEntity()),
            false
        );
        return ((Number) health.get("number_of_nodes")).intValue();
    }

    private String indexConfig() {
        return "{\"settings\":{\"number_of_shards\":3,\"number_of_replicas\":0,"
            + "\"index.search.default_pipeline\":\""
            + NORM_PIPELINE
            + "\"},"
            + "\"mappings\":{\"properties\":{\""
            + GRP_FIELD
            + "\":{\"type\":\"keyword\"},\""
            + SCORE_FIELD
            + "\":{\"type\":\"integer\"},\""
            + NESTED_FIELD
            + "\":{\"type\":\"nested\",\"properties\":{\"k\":{\"type\":\"keyword\"}}}}}}";
    }

    @SneakyThrows
    private void ensureDataset() {
        createSearchPipeline(NORM_PIPELINE, "min_max", "arithmetic_mean", Map.of());
        if (indexExists(INDEX)) {
            return;
        }
        createIndex(INDEX, indexConfig());
        for (int id = 1; id <= TOTAL_DOCS; id++) {
            String grp = id <= WINDOW_LAST_ID ? "A" : "B";
            int tier = id <= WINDOW_LAST_ID ? 3 : (id <= 17 ? 2 : 1);
            int s = tier * 1000 - id;
            Request request = new Request("PUT", "/" + INDEX + "/_doc/" + id + "?refresh=true");
            request.setJsonEntity("{\"" + GRP_FIELD + "\":\"" + grp + "\",\"" + SCORE_FIELD + "\":" + s + "}");
            int code = client().performRequest(request).getStatusLine().getStatusCode();
            assertTrue(
                "indexing doc " + id + " failed: " + code,
                code == RestStatus.OK.getStatus() || code == RestStatus.CREATED.getStatus()
            );
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> hitIds(Map<String, Object> response) {
        List<String> out = new ArrayList<>();
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        if (hitList != null) {
            for (Map<String, Object> hit : hitList) {
                out.add((String) hit.get("_id"));
            }
        }
        return out;
    }
}
