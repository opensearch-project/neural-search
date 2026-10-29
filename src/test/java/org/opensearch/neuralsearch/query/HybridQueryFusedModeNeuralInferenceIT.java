/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.opensearch.neuralsearch.util.TestUtils.TEST_DIMENSION;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_SPACE_TYPE;
import static org.opensearch.neuralsearch.util.TestUtils.createRandomVector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.stats.events.EventStatName;

import com.google.common.primitives.Floats;

import lombok.SneakyThrows;

/**
 * Regression test for <a href="https://github.com/opensearch-project/neural-search/issues/2073">#2073</a>: a fused hybrid
 * query calls a {@code neural} sub-query's model <b>once</b> per request, whatever round 2 carries.
 *
 * <p>Model calls are counted through the {@code neural_query_against_knn_requests} stat, which increments once per completed
 * neural→k-NN rewrite ({@code NeuralQueryBuilder#rewriteQueryAgainstKnnField}) — that is, once per inference that produced a
 * vector. Before the fix the count read <b>2</b> for the one shape where round 2's Tail carried the {@code neural} leg
 * verbatim: the leg had filled the window ({@code k == window_size}) and the request needed the Tail (default
 * {@code track_total_hits} on an index below the threshold). The Tail held the user's original builder, whose vector the
 * leg sub-search had resolved on a copy, so the coordinator inferred again when it rewrote round 2. The legs are now resolved
 * once, ahead of the fan-out ({@code FusedLegResolver}), and every arm reads 1:
 *
 * <ul>
 *   <li>A — fused, {@code track_total_hits: false}, {@code k == window_size}: no Tail;</li>
 *   <li>B — fused, default totals, {@code k == window_size}: Tail, leg kept as a query (the shape that used to read 2);</li>
 *   <li>C — fused, default totals, {@code k < window_size}: Tail, leg materialized to the ids it returned;</li>
 *   <li>D — a plain {@code neural} query, as the control.</li>
 * </ul>
 *
 * <p>Five documents on one shard, a window of 3: no leg reaches the 10,000 threshold, so the default shape keeps the Tail,
 * and {@code hits.total} is the union the Tail counted — which the test also asserts, so that "one inference" is shown on a
 * request that really did run round 2 with the leg in it, not on one that skipped the Tail.
 */
public class HybridQueryFusedModeNeuralInferenceIT extends BaseNeuralSearchIT {

    private static final String INDEX = "test-fused-neural-inference";
    private static final String VECTOR_FIELD = "vec";
    private static final int DOCS = 5;
    private static final int WINDOW = 3;
    private static final String QUERY_TEXT = "Hello world";
    private static final String COORDINATOR_TAG = "[fused:hybrid_0]";

    /**
     * One test for the four arms and the profiled check, so the model is registered and deployed once: the base class wipes
     * models and indices after every test, and a deploy per arm would cost more cluster time than everything else here.
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public void testFusedNeuralLeg_whenTheTailCarriesIt_thenTheModelIsCalledOncePerRequest() {
        enableStats();
        try {
            prepareDataset();
            String modelId = prepareModel();

            // One plain neural search first, so that model loading and first-call effects are attributed to no arm.
            search(plainNeural(modelId, WINDOW));

            int a = inferencesDuring(() -> search(fused(modelId, WINDOW, true)));
            assertEquals("A: no Tail, one inference", 1, a);

            Map<String, Object>[] b = new Map[1];
            int bCount = inferencesDuring(() -> b[0] = search(fused(modelId, WINDOW, false)));
            assertEquals("B: the Tail carries the window-filling neural leg as a query, and still one inference", 1, bCount);
            assertEquals("B returned the page", WINDOW, hitCount(b[0]));
            assertEquals("B's Tail counted the whole union, so round 2 really ran with the leg in it", DOCS, totalHits(b[0]));

            int c = inferencesDuring(() -> search(fused(modelId, 1, false)));
            assertEquals("C: the short leg is materialized to ids, one inference", 1, c);

            int d = inferencesDuring(() -> search(plainNeural(modelId, WINDOW)));
            assertEquals("D: a plain neural query infers once", 1, d);

            // Arm B once more, profiled: the coordinator's entry says round 2 carried a Tail (tail_built) -- so this is the
            // shape the defect lived in -- and that resolving the legs is where the inference now happens
            // (legs_resolve_wait is non-zero), while the model is still called once.
            Map<String, Object>[] profiledB = new Map[1];
            int profiledCount = inferencesDuring(() -> profiledB[0] = search(profiled(fused(modelId, WINDOW, false))));
            assertEquals("B, profiled: one inference", 1, profiledCount);
            Map<String, Object> node = coordinatorQueryNode(profiledB[0]);
            Map<String, Object> debug = (Map<String, Object>) node.get("debug");
            assertEquals("round 2 carried the Tail -- the shape that used to infer twice", Boolean.TRUE, debug.get("tail_built"));
            Map<String, Object> breakdown = (Map<String, Object>) node.get("breakdown");
            assertTrue(
                "the inference is paid while resolving the legs, ahead of the fan-out: " + breakdown,
                ((Number) breakdown.get("legs_resolve_wait")).longValue() > 0
            );
        } finally {
            disableStats();
        }
    }

    // ------------------------------------------------------------------------------------------------------------------

    @SneakyThrows
    private void prepareDataset() {
        prepareKnnIndex(INDEX, Collections.singletonList(new KNNFieldConfig(VECTOR_FIELD, TEST_DIMENSION, TEST_SPACE_TYPE)), 1);
        for (int id = 1; id <= DOCS; id++) {
            addKnnDoc(
                INDEX,
                String.valueOf(id),
                Collections.singletonList(VECTOR_FIELD),
                Collections.singletonList(Floats.asList(createRandomVector(TEST_DIMENSION)).toArray())
            );
        }
        assertEquals(DOCS, getDocCount(INDEX));
    }

    private String fused(final String modelId, final int k, final boolean totalsOff) {
        return "{"
            + (totalsOff ? "\"track_total_hits\":false," : "")
            + "\"size\":"
            + WINDOW
            + ",\"query\":{\"hybrid\":{\"fusion\":{\"window_size\":"
            + WINDOW
            + ",\"normalization\":{\"technique\":\"min_max\"},\"combination\":{\"technique\":\"arithmetic_mean\"}},"
            + "\"queries\":[{\"match_all\":{}},"
            + neuralLeg(modelId, k)
            + "]}}}";
    }

    private String profiled(final String body) {
        return "{\"profile\":true," + body.substring(1);
    }

    private String plainNeural(final String modelId, final int k) {
        return "{\"size\":" + WINDOW + ",\"query\":" + neuralLeg(modelId, k) + "}";
    }

    private String neuralLeg(final String modelId, final int k) {
        return String.format(
            Locale.ROOT,
            "{\"neural\":{\"%s\":{\"query_text\":\"%s\",\"model_id\":\"%s\",\"k\":%d}}}",
            VECTOR_FIELD,
            QUERY_TEXT,
            modelId,
            k
        );
    }

    @SneakyThrows
    private Map<String, Object> search(final String body) {
        Request request = new Request("POST", "/" + INDEX + "/_search");
        request.setJsonEntity(body);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    /**
     * How many neural→k-NN rewrites completed — one per model call that produced a vector — while {@code action} ran. Read
     * right after the search returns, with no wait: the stat is incremented on the coordinator, synchronously, inside the
     * rewrite that precedes the search phases, so a response in hand means every increment the request made is in place.
     */
    private int inferencesDuring(final Runnable action) {
        int before = inferenceCount();
        action.run();
        return inferenceCount() - before;
    }

    @SneakyThrows
    private int inferenceCount() {
        String responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
        Map<String, Object> allNodesStats = parseAggregatedNodeStatsResponse(responseBody);
        Object value = getNestedValue(allNodesStats, EventStatName.NEURAL_QUERY_AGAINST_KNN_REQUESTS);
        return value == null ? 0 : ((Number) value).intValue();
    }

    @SuppressWarnings("unchecked")
    private int hitCount(final Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        List<Object> list = (List<Object>) hits.get("hits");
        return list == null ? 0 : list.size();
    }

    @SuppressWarnings("unchecked")
    private int totalHits(final Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        Map<String, Object> total = (Map<String, Object>) hits.get("total");
        assertEquals("the Tail counted an exact union", "eq", total.get("relation"));
        return ((Number) total.get("value")).intValue();
    }

    /** The one query node of the coordinator's own profile entry, {@code [coordinator][fused:hybrid_0]}. */
    @SuppressWarnings("unchecked")
    private Map<String, Object> coordinatorQueryNode(final Map<String, Object> response) {
        Map<String, Object> profile = (Map<String, Object>) response.get("profile");
        List<Map<String, Object>> shards = (List<Map<String, Object>>) profile.get("shards");
        List<Map<String, Object>> matches = shards.stream()
            .filter(shard -> String.valueOf(shard.get("id")).endsWith(COORDINATOR_TAG))
            .toList();
        assertEquals("exactly one coordinator entry: " + shards.stream().map(shard -> shard.get("id")).toList(), 1, matches.size());
        List<Map<String, Object>> searches = (List<Map<String, Object>>) matches.get(0).get("searches");
        assertEquals(1, searches.size());
        List<Map<String, Object>> query = (List<Map<String, Object>>) searches.get(0).get("query");
        assertEquals(1, query.size());
        return query.get(0);
    }
}
