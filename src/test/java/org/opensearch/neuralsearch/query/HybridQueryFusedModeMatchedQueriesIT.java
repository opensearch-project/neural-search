/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;

import lombok.SneakyThrows;

/**
 * Proves that a fused/resolver hybrid reports {@code matched_queries} for a named sub-query, with or without the Tail.
 *
 * <p>{@code matched_queries} is a fetch-phase field built from the names registered while the query was converted on the
 * shard ({@code QueryShardContext#addNamedQuery} → {@code ParsedQuery#namedFilters()}), and {@code MatchedQueriesPhase}
 * then builds its <i>own</i> weight per registered name to decide which hits it applies to. So a named sub-query has to be
 * <b>registered</b>, never executed, for its name to be reported.
 *
 * <p>The Tail was the only thing that converted legs, so a Top-only fused query registered no leg at all and the field
 * silently vanished from a response classic hybrid always carries it in — at HTTP 200, with no warning. Worse, the
 * boundary is a request-shape detail rather than anything the user expressed about names:
 * {@code track_total_hits} one above the fused window builds the Tail and reports names, one at the window does not. The
 * legs are now carried for registration alone when the Tail is not built, which keeps a Top-only query Top-only.
 *
 * <p>Every test is paired against a classic-hybrid oracle, so the expected names are anchored to known-correct behavior
 * rather than to this feature's own implementation. Multi-shard on purpose (3 shards).
 *
 * <pre>
 *   ids 1-3 = color "blue",  s = 100 - id   (matched by blue_leg only)
 *   ids 4-6 = color "green", s = 50 - id    (matched by green_leg only)
 * </pre>
 * Each leg scores by {@code s} with {@code boost_mode: replace}, so a leg's score for a document is exactly its {@code s}
 * — shard-independent, which is what lets {@code include_named_queries_score} be compared value-for-value with classic.
 */
public class HybridQueryFusedModeMatchedQueriesIT extends BaseNeuralSearchIT {

    private static final String INDEX = "test-fused-matched-queries";
    private static final String NORM_PIPELINE = "fused-matched-queries-norm-pipeline";
    private static final String COLOR_FIELD = "color";
    private static final String SCORE_FIELD = "s";
    private static final String BLUE_LEG = "blue_leg";
    private static final String GREEN_LEG = "green_leg";
    private static final int TOTAL_DOCS = 6;
    private static final int BLUE_DOCS = 3;
    /** The whole match set fits in the window, so classic and fused return the same hits and can be compared directly. */
    private static final int FULL_WINDOW = 6;
    /** Smaller than the match set, so Top-only and Top+Tail return visibly different hit counts. */
    private static final int PARTIAL_WINDOW = 4;

    private String indexConfig() {
        return "{\"settings\":{\"number_of_shards\":3,\"number_of_replicas\":0,"
            + "\"index.search.default_pipeline\":\""
            + NORM_PIPELINE
            + "\"},"
            + "\"mappings\":{\"properties\":{\""
            + COLOR_FIELD
            + "\":{\"type\":\"keyword\"},\""
            + SCORE_FIELD
            + "\":{\"type\":\"integer\"}}}}";
    }

    @SneakyThrows
    private void ensureDataset() {
        createSearchPipeline(NORM_PIPELINE, "min_max", "arithmetic_mean", Map.of());
        if (indexExists(INDEX)) {
            return;
        }
        createIndex(INDEX, indexConfig());
        for (int id = 1; id <= TOTAL_DOCS; id++) {
            boolean blue = id <= BLUE_DOCS;
            int s = (blue ? 100 : 50) - id;
            Request request = new Request("PUT", "/" + INDEX + "/_doc/" + id + "?refresh=true");
            request.setJsonEntity("{\"" + COLOR_FIELD + "\":\"" + (blue ? "blue" : "green") + "\",\"" + SCORE_FIELD + "\":" + s + "}");
            Response response = client().performRequest(request);
            int code = response.getStatusLine().getStatusCode();
            assertTrue(
                "indexing doc " + id + " failed: " + code,
                code == RestStatus.OK.getStatus() || code == RestStatus.CREATED.getStatus()
            );
        }
    }

    /** A named leg matching one color and scoring each hit by {@code s} exactly ({@code boost_mode: replace}). */
    private String leg(String color, String queryName) {
        return "{\"function_score\":{\"query\":{\"term\":{\""
            + COLOR_FIELD
            + "\":\""
            + color
            + "\"}},\"field_value_factor\":{\"field\":\""
            + SCORE_FIELD
            + "\",\"modifier\":\"none\",\"missing\":1},\"boost_mode\":\"replace\",\"_name\":\""
            + queryName
            + "\"}}";
    }

    private String legs() {
        return leg("blue", BLUE_LEG) + "," + leg("green", GREEN_LEG);
    }

    private String nameField(String queryName) {
        return queryName == null ? "" : "\"_name\":\"" + queryName + "\",";
    }

    /** The oracle: classic hybrid converts every leg unconditionally, so it never loses a leg's name. */
    private String classicHybrid(String queryName) {
        return "{\"hybrid\":{" + nameField(queryName) + "\"queries\":[" + legs() + "]}}";
    }

    private String fusedHybrid(int windowSize, String queryName) {
        return "{\"hybrid\":{"
            + nameField(queryName)
            + "\"fusion\":{\"window_size\":"
            + windowSize
            + ",\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\"}},"
            + "\"queries\":["
            + legs()
            + "]}}";
    }

    /** {@code track_total_hits} is the Top-only/Top+Tail lever: at or below the window there is nothing left to count. */
    private String body(String query, String trackTotalHits) {
        return "{\"query\":" + query + ",\"track_total_hits\":" + trackTotalHits + "}";
    }

    @SneakyThrows
    public void testClassicHybrid_reportsTheLegThatMatchedEachHit() {
        ensureDataset();

        Map<String, Set<String>> namesById = matchedQueriesById(searchRaw(body(classicHybrid(null), "true"), 10));

        assertEquals("every document is a hit", TOTAL_DOCS, namesById.size());
        assertLegNamesPerColor(namesById);
    }

    @SneakyThrows
    public void testFusedHybrid_withTail_reportsTheSameNamesAsClassic() {
        ensureDataset();

        Map<String, Set<String>> namesById = matchedQueriesById(searchRaw(body(fusedHybrid(FULL_WINDOW, null), "true"), 10));

        assertEquals(TOTAL_DOCS, namesById.size());
        assertLegNamesPerColor(namesById);
    }

    /**
     * The regression this class exists for. {@code track_total_hits:false} with no aggregation, highlight, non-{@code _score}
     * sort or collapse expansion is the Top-only opt-out — and it used to take {@code matched_queries} with it, for every
     * hit, silently. Nothing about this request says anything about named queries.
     */
    @SneakyThrows
    public void testFusedHybrid_topOnly_stillReportsMatchedQueries() {
        ensureDataset();

        Map<String, Set<String>> namesById = matchedQueriesById(searchRaw(body(fusedHybrid(FULL_WINDOW, null), "false"), 10));

        assertEquals("the fused window is still returned in full", TOTAL_DOCS, namesById.size());
        assertLegNamesPerColor(namesById);
    }

    /**
     * The boundary made explicit: one more than the window builds the Tail, exactly the window does not. Both sides must
     * report the same names — which side of an unrelated totals threshold a request lands on is not a statement about
     * named queries. The hit counts differ (Top-only returns the window; the Tail adds the rest of the match set at
     * score 0), which is what proves the two requests really did take the two different paths.
     */
    @SneakyThrows
    public void testFusedHybrid_reportsNamesOnBothSidesOfTheTotalsCliff() {
        ensureDataset();
        String fused = fusedHybrid(PARTIAL_WINDOW, null);

        Map<String, Set<String>> topOnly = matchedQueriesById(searchRaw(body(fused, Integer.toString(PARTIAL_WINDOW)), 10));
        Map<String, Set<String>> withTail = matchedQueriesById(searchRaw(body(fused, Integer.toString(PARTIAL_WINDOW + 1)), 10));

        assertEquals("track_total_hits at the window is Top-only: only the window comes back", PARTIAL_WINDOW, topOnly.size());
        assertEquals("one above the window builds the Tail: the whole match set comes back", TOTAL_DOCS, withTail.size());
        assertLegNamesPerColor(topOnly);
        assertLegNamesPerColor(withTail);
    }

    /**
     * The other Tail trigger, exercised because it is independent of the totals one: an aggregation builds the Tail even
     * with {@code track_total_hits:false}, so the legs are executed and register their own names. Both lists are never
     * populated at once, and this is the configuration that would break if the coordinator started carrying them anyway.
     */
    @SneakyThrows
    public void testFusedHybrid_whenAggregationForcesTheTail_thenNamesAreReportedOnce() {
        ensureDataset();
        String aggregations = ",\"aggregations\":{\"by_color\":{\"terms\":{\"field\":\"" + COLOR_FIELD + "\",\"size\":10}}}";

        Map<String, Object> response = searchRaw(withSuffix(body(fusedHybrid(FULL_WINDOW, null), "false"), aggregations), 10);
        Map<String, Set<String>> namesById = matchedQueriesById(response);

        // The aggregation is what proves the path: it is computed from the Tail's match set, so a populated bucket per
        // color means the legs really were executed rather than merely registered.
        assertEquals(
            "both colors are aggregated, so the Tail ran",
            Map.of("blue", BLUE_DOCS, "green", TOTAL_DOCS - BLUE_DOCS),
            colorBuckets(response)
        );
        assertEquals(TOTAL_DOCS, namesById.size());
        assertLegNamesPerColor(namesById);
    }

    /**
     * The scoring variant reports the score of the named query itself, computed in the fetch phase from the phase's own
     * weight — so a registered-but-not-executed leg reports the same value classic does, to the float. Each leg scores by
     * {@code s} with {@code boost_mode: replace}, so the expected value is the document's own {@code s}.
     */
    @SneakyThrows
    public void testFusedHybrid_topOnly_includeNamedQueriesScore_matchesClassic() {
        ensureDataset();
        // include_named_queries_score is a response parameter as much as a source field: the source field makes the fetch
        // phase score each named query, but SearchHit renders the name→score object only when the URL parameter is present
        // (RestSearchAction copies the parameter onto the source, so passing it there covers both).
        Map<String, String> scored = Map.of("include_named_queries_score", "true");

        Map<String, Map<String, Double>> classic = matchedQueryScoresById(searchRaw(body(classicHybrid(null), "true"), 10, scored));
        Map<String, Map<String, Double>> fused = matchedQueryScoresById(
            searchRaw(body(fusedHybrid(FULL_WINDOW, null), "false"), 10, scored)
        );

        // Pin the oracle against its own known values first: the scoring variant reports an OBJECT, and reading an array
        // shape would leave every map empty and every comparison below trivially true.
        assertEquals(TOTAL_DOCS, classic.size());
        for (Map.Entry<String, Map<String, Double>> entry : classic.entrySet()) {
            int id = Integer.parseInt(entry.getKey());
            String expectedLeg = id <= BLUE_DOCS ? BLUE_LEG : GREEN_LEG;
            double expectedScore = (id <= BLUE_DOCS ? 100 : 50) - id;
            assertEquals("doc " + id + ": one leg, scored", Set.of(expectedLeg), entry.getValue().keySet());
            assertEquals("doc " + id + ": the leg's own score", expectedScore, entry.getValue().get(expectedLeg), 0.0001d);
        }

        assertEquals("the same documents are named in both modes", classic.keySet(), fused.keySet());
        for (Map.Entry<String, Map<String, Double>> entry : classic.entrySet()) {
            String id = entry.getKey();
            assertEquals("doc " + id + ": the same leg is named", entry.getValue().keySet(), fused.get(id).keySet());
            for (Map.Entry<String, Double> named : entry.getValue().entrySet()) {
                assertEquals(
                    "doc " + id + ", " + named.getKey() + ": a registered leg scores exactly as an executed one",
                    named.getValue(),
                    fused.get(id).get(named.getKey()),
                    0.0001d
                );
            }
        }
    }

    /**
     * The {@code hybrid} clause's own {@code _name} is not a casualty of the self-erase: it is inherited onto the
     * self-erased query by {@code AbstractQueryBuilder#rewrite} and registered by its {@code final} {@code toQuery}. It
     * therefore applies to every returned hit, alongside the leg that matched it.
     */
    @SneakyThrows
    public void testFusedHybrid_topOnly_whenTheHybridClauseIsNamed_thenItsOwnNameIsReportedToo() {
        ensureDataset();
        String hybridName = "my_hybrid";

        Map<String, Set<String>> classic = matchedQueriesById(searchRaw(body(classicHybrid(hybridName), "true"), 10));
        Map<String, Set<String>> fused = matchedQueriesById(searchRaw(body(fusedHybrid(FULL_WINDOW, hybridName), "false"), 10));

        assertEquals(TOTAL_DOCS, fused.size());
        for (Map.Entry<String, Set<String>> entry : fused.entrySet()) {
            String id = entry.getKey();
            assertTrue(
                "doc " + id + ": the hybrid clause's own name must be reported, got " + entry.getValue(),
                entry.getValue().contains(hybridName)
            );
            assertEquals("doc " + id + ": fused must report exactly what classic does", classic.get(id), entry.getValue());
        }
    }

    // ------------------------------------------------ helpers ------------------------------------------------

    /** ids 1..3 are blue and can only be matched by the blue leg; 4..6 likewise for green. */
    private void assertLegNamesPerColor(Map<String, Set<String>> namesById) {
        for (Map.Entry<String, Set<String>> entry : namesById.entrySet()) {
            String expected = Integer.parseInt(entry.getKey()) <= BLUE_DOCS ? BLUE_LEG : GREEN_LEG;
            assertEquals("doc " + entry.getKey() + " must report the leg that matched it", Set.of(expected), entry.getValue());
        }
    }

    /** Appends a top-level clause to an already-complete request body. */
    private String withSuffix(String body, String suffix) {
        return body.substring(0, body.length() - 1) + suffix + "}";
    }

    /** {@code color -> doc_count} for the {@code by_color} terms aggregation. */
    @SuppressWarnings("unchecked")
    private Map<String, Integer> colorBuckets(Map<String, Object> response) {
        Map<String, Integer> out = new LinkedHashMap<>();
        Map<String, Object> aggregations = (Map<String, Object>) response.get("aggregations");
        if (aggregations == null) {
            return out;
        }
        Map<String, Object> byColor = (Map<String, Object>) aggregations.get("by_color");
        for (Map<String, Object> bucket : (List<Map<String, Object>>) byColor.get("buckets")) {
            out.put((String) bucket.get("key"), ((Number) bucket.get("doc_count")).intValue());
        }
        return out;
    }

    private Map<String, Object> searchRaw(String jsonBody, int size) {
        return searchRaw(jsonBody, size, Map.of());
    }

    @SneakyThrows
    private Map<String, Object> searchRaw(String jsonBody, int size, Map<String, String> extraParameters) {
        Request request = new Request("POST", "/" + INDEX + "/_search");
        request.setJsonEntity(jsonBody);
        request.addParameter("size", Integer.toString(size));
        extraParameters.forEach(request::addParameter);
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> hits(Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        return hitList == null ? new ArrayList<>() : hitList;
    }

    /** {@code _id -> matched_queries}. An absent field reads as an empty set, which is the very loss under test. */
    @SuppressWarnings("unchecked")
    private Map<String, Set<String>> matchedQueriesById(Map<String, Object> response) {
        Map<String, Set<String>> out = new LinkedHashMap<>();
        for (Map<String, Object> hit : hits(response)) {
            Object matched = hit.get("matched_queries");
            Set<String> names = new LinkedHashSet<>();
            if (matched instanceof List) {
                names.addAll((List<String>) matched);
            } else if (matched instanceof Map) {
                names.addAll(((Map<String, Object>) matched).keySet());
            }
            out.put((String) hit.get("_id"), names);
        }
        return out;
    }

    /** {@code _id -> (name -> score)}, the {@code include_named_queries_score} shape. */
    @SuppressWarnings("unchecked")
    private Map<String, Map<String, Double>> matchedQueryScoresById(Map<String, Object> response) {
        Map<String, Map<String, Double>> out = new LinkedHashMap<>();
        for (Map<String, Object> hit : hits(response)) {
            Map<String, Double> scores = new LinkedHashMap<>();
            Object matched = hit.get("matched_queries");
            if (matched instanceof Map) {
                for (Map.Entry<String, Object> entry : ((Map<String, Object>) matched).entrySet()) {
                    scores.put(entry.getKey(), ((Number) entry.getValue()).doubleValue());
                }
            }
            out.put((String) hit.get("_id"), scores);
        }
        return out;
    }
}
