/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import lombok.SneakyThrows;

/**
 * A classic hybrid query (no {@code fusion} block) that no search pipeline would combine runs in fusion mode with the
 * default techniques, as if it carried {@code "fusion": {}}. The oracle for every routed arm is therefore the same body with
 * {@code "fusion": {}} and the same parameters: same documents, same scores, same totals. Every index is multi-shard, so
 * the legs' per-shard results and round 2 are exercised across shards.
 */
public class HybridQueryDefaultFusionIT extends BaseNeuralSearchIT {

    private static final String INDEX = "test-hybrid-default-fusion";
    private static final String INDEX_WITH_NORM_DEFAULT = "test-hybrid-default-fusion-norm-default";
    private static final String INDEX_WITH_RRF_DEFAULT = "test-hybrid-default-fusion-rrf-default";
    private static final String INDEX_WITH_RESPONSE_ONLY_DEFAULT = "test-hybrid-default-fusion-response-only-default";
    private static final String NORM_PIPELINE = "default-fusion-it-norm-pipeline";
    private static final String RRF_PIPELINE = "default-fusion-it-rrf-pipeline";
    /** Exists, and combines nothing: a response processor only. */
    private static final String RESPONSE_ONLY_PIPELINE = "default-fusion-it-response-only-pipeline";
    private static final String RESPONSE_ONLY_PIPELINE_BODY =
        "{\"response_processors\":[{\"rename_field\":{\"field\":\"absent\",\"target_field\":\"still_absent\",\"ignore_missing\":true}}]}";
    private static final String TEXT_FIELD = "text";
    private static final String CATEGORY_FIELD = "category";
    private static final int SHARDS = 3;
    private static final String LEGS = "{\"match\":{\"" + TEXT_FIELD + "\":\"hello\"}},{\"term\":{\"" + TEXT_FIELD + "\":\"place\"}}";
    private static final String CLASSIC = "{\"hybrid\":{\"queries\":[" + LEGS + "]}}";
    private static final String DEFAULT_FUSION = "{\"hybrid\":{\"fusion\":{},\"queries\":[" + LEGS + "]}}";
    private static final String DEFAULT_FUSION_PREFIX = "[hybrid] query without [fusion] runs in fusion mode with the default techniques";
    /** Documents 1, 2, 3, 5, 6 and 7 match at least one leg; 4 and 8 match neither. */
    private static final int MATCHING_DOCS = 6;

    @SneakyThrows
    private void ensureFixture() {
        createSearchPipeline(NORM_PIPELINE, "min_max", "arithmetic_mean", Map.of());
        createRRFSearchPipeline(RRF_PIPELINE, List.of(), false);
        Request pipeline = new Request("PUT", "/_search/pipeline/" + RESPONSE_ONLY_PIPELINE);
        pipeline.setJsonEntity(RESPONSE_ONLY_PIPELINE_BODY);
        client().performRequest(pipeline);
        createIndexWithDocs(INDEX, null);
        createIndexWithDocs(INDEX_WITH_NORM_DEFAULT, NORM_PIPELINE);
        createIndexWithDocs(INDEX_WITH_RRF_DEFAULT, RRF_PIPELINE);
        createIndexWithDocs(INDEX_WITH_RESPONSE_ONLY_DEFAULT, RESPONSE_ONLY_PIPELINE);
    }

    @SneakyThrows
    private void createIndexWithDocs(final String index, final String defaultPipeline) {
        if (indexExists(index)) {
            return;
        }
        String defaultSetting = Objects.isNull(defaultPipeline) ? "" : ",\"index.search.default_pipeline\":\"" + defaultPipeline + "\"";
        createIndex(
            index,
            "{\"settings\":{\"number_of_shards\":"
                + SHARDS
                + ",\"number_of_replicas\":0"
                + defaultSetting
                + "},\"mappings\":{\"properties\":{\""
                + TEXT_FIELD
                + "\":{\"type\":\"text\"},\""
                + CATEGORY_FIELD
                + "\":{\"type\":\"keyword\"}}}}"
        );
        String[][] docs = {
            { "1", "hello world hello", "a" },
            { "2", "hello there place", "a" },
            { "3", "welcome to the place", "b" },
            { "4", "nothing relevant at all", "b" },
            { "5", "hello place again", "c" },
            { "6", "another place far away", "c" },
            { "7", "say hello", "a" },
            { "8", "unrelated words", "b" } };
        for (String[] doc : docs) {
            Request request = new Request("PUT", "/" + index + "/_doc/" + doc[0] + "?refresh=true");
            request.setJsonEntity("{\"" + TEXT_FIELD + "\":\"" + doc[1] + "\",\"" + CATEGORY_FIELD + "\":\"" + doc[2] + "\"}");
            client().performRequest(request);
        }
    }

    /**
     * The pipeline states in which core combines nothing: no pipeline, {@code _none} over a normalizing index default, a named
     * pipeline that combines nothing, the same pipeline as an inline body or as the index default, and indices whose default
     * pipelines differ (core resolves those to no pipeline). Each answers exactly what {@code fusion: {}} answers, with nothing
     * left of the classic per-shard format: no duplicate documents, no delimiter scores.
     */
    @SneakyThrows
    public void testClassicHybrid_whenNoPipelineCombinesTheScores_thenAnswersAsAnEmptyFusionBlock() {
        ensureFixture();
        String inlineBody = ",\"search_pipeline\":" + RESPONSE_ONLY_PIPELINE_BODY;
        List<Object[]> arms = List.of(
            new Object[] { "no pipeline", INDEX, Map.of(), "" },
            new Object[] { "_none over a normalizing index default", INDEX_WITH_NORM_DEFAULT, Map.of("search_pipeline", "_none"), "" },
            new Object[] { "a named pipeline that combines nothing", INDEX, Map.of("search_pipeline", RESPONSE_ONLY_PIPELINE), "" },
            new Object[] { "an inline pipeline that combines nothing", INDEX, Map.of(), inlineBody },
            new Object[] { "an index default that combines nothing", INDEX_WITH_RESPONSE_ONLY_DEFAULT, Map.of(), "" },
            new Object[] { "index defaults that differ", INDEX_WITH_NORM_DEFAULT + "," + INDEX_WITH_RRF_DEFAULT, Map.of(), "" }
        );
        for (Object[] arm : arms) {
            String name = (String) arm[0];
            String endpoint = "/" + arm[1] + "/_search";
            @SuppressWarnings("unchecked")
            Map<String, String> params = (Map<String, String>) arm[2];
            String extra = (String) arm[3];

            Map<String, Object> classic = search(endpoint, "{\"size\":10,\"query\":" + CLASSIC + extra + "}", params);
            Map<String, Object> oracle = search(endpoint, "{\"size\":10,\"query\":" + DEFAULT_FUSION + extra + "}", params);

            assertEquals(name, rankedHits(oracle), rankedHits(classic));
            assertEquals(name, totalHits(oracle), totalHits(classic));
            assertEquals(name, 0, failedShards(classic));
            List<String> hits = rankedHits(classic);
            assertFalse(name, hits.isEmpty());
            Set<String> documents = new HashSet<>();
            for (Map<String, Object> hit : hitList(classic)) {
                assertTrue(name + ": each document once " + hits, documents.add(hit.get("_index") + "/" + hit.get("_id")));
                double score = ((Number) hit.get("_score")).doubleValue();
                assertTrue(name + ": a min_max + arithmetic_mean score " + hits, score >= 0.0 && score <= 1.0);
            }
        }
        assertEquals(MATCHING_DOCS, totalHits(search("/" + INDEX + "/_search", "{\"size\":10,\"query\":" + CLASSIC + "}", Map.of())));
    }

    /** A profiled request says that it ran with the default fusion; the explicit {@code fusion: {}} twin does not. */
    @SneakyThrows
    public void testClassicHybrid_whenProfiled_thenTheCoordinatorEntrySaysDefaultFusion() {
        ensureFixture();
        String endpoint = "/" + INDEX + "/_search";
        Map<String, Object> classic = search(endpoint, "{\"size\":10,\"profile\":true,\"query\":" + CLASSIC + "}", Map.of());
        assertEquals(Boolean.TRUE, coordinatorDebug(classic).get("default_fusion"));
        Map<String, Object> explicit = search(endpoint, "{\"size\":10,\"profile\":true,\"query\":" + DEFAULT_FUSION + "}", Map.of());
        assertFalse(coordinatorDebug(explicit).containsKey("default_fusion"));
    }

    /** A pipeline that combines the scores keeps the request classic: no fused coordinator entry in the profile. */
    @SneakyThrows
    public void testClassicHybrid_whenAPipelineCombinesTheScores_thenStaysClassic() {
        ensureFixture();
        String profiled = "{\"size\":10,\"profile\":true,\"query\":" + CLASSIC + "}";
        assertFalse(hasFusedCoordinatorEntry(search("/" + INDEX + "/_search", profiled, Map.of("search_pipeline", NORM_PIPELINE))));
        assertFalse(hasFusedCoordinatorEntry(search("/" + INDEX_WITH_NORM_DEFAULT + "/_search", profiled, Map.of())));
        assertFalse(hasFusedCoordinatorEntry(search("/" + INDEX_WITH_RRF_DEFAULT + "/_search", profiled, Map.of())));
    }

    /**
     * {@code size: 0} has no hits to combine, so it stays classic: its totals and buckets are the ones a normalizing pipeline
     * leaves, and {@code _count} answers as before.
     */
    @SneakyThrows
    public void testClassicHybrid_whenSizeIsZero_thenStaysClassic() {
        ensureFixture();
        String endpoint = "/" + INDEX + "/_search";
        String sizeZero = "{\"size\":0,\"query\":"
            + CLASSIC
            + ",\"aggs\":{\"by_category\":{\"terms\":{\"field\":\""
            + CATEGORY_FIELD
            + "\"}}}}";
        Map<String, Object> withoutPipeline = search(endpoint, sizeZero, Map.of());
        Map<String, Object> withPipeline = search(endpoint, sizeZero, Map.of("search_pipeline", NORM_PIPELINE));
        assertEquals(totalHits(withPipeline), totalHits(withoutPipeline));
        assertEquals(withPipeline.get("aggregations"), withoutPipeline.get("aggregations"));
        assertEquals(MATCHING_DOCS, totalHits(withoutPipeline));
        assertFalse(hasFusedCoordinatorEntry(search(endpoint, "{\"size\":0,\"profile\":true,\"query\":" + CLASSIC + "}", Map.of())));

        Request count = new Request("POST", "/" + INDEX + "/_count");
        count.setJsonEntity("{\"query\":" + CLASSIC + "}");
        Map<String, Object> countResponse = asMap(client().performRequest(count));
        assertEquals(MATCHING_DOCS, ((Number) countResponse.get("count")).intValue());
    }

    /**
     * A shape fusion mode refuses is refused with a message that says why fusion mode ran; scroll, which classic refuses too,
     * keeps classic's message; DFS, which classic refuses, is answered as {@code fusion: {}} answers it.
     */
    @SneakyThrows
    public void testClassicHybrid_whenFusionModeCannotServeTheShape_thenRefusedWithTheReason() {
        ensureFixture();
        String endpoint = "/" + INDEX + "/_search";
        ResponseException terminateAfter = expectThrows(
            ResponseException.class,
            () -> search(endpoint, "{\"size\":10,\"terminate_after\":5,\"query\":" + CLASSIC + "}", Map.of())
        );
        assertEquals(RestStatus.BAD_REQUEST.getStatus(), terminateAfter.getResponse().getStatusLine().getStatusCode());
        assertTrue(terminateAfter.getMessage(), terminateAfter.getMessage().contains(DEFAULT_FUSION_PREFIX));
        assertTrue(terminateAfter.getMessage(), terminateAfter.getMessage().contains("does not support [terminate_after]"));

        ResponseException scroll = expectThrows(
            ResponseException.class,
            () -> search(endpoint, "{\"size\":10,\"query\":" + CLASSIC + "}", Map.of("scroll", "1m"))
        );
        assertEquals(RestStatus.BAD_REQUEST.getStatus(), scroll.getResponse().getStatusLine().getStatusCode());
        assertTrue(scroll.getMessage(), scroll.getMessage().contains("Scroll operation is not supported in hybrid query"));
        assertFalse(scroll.getMessage(), scroll.getMessage().contains(DEFAULT_FUSION_PREFIX));

        Map<String, String> dfs = Map.of("search_type", "dfs_query_then_fetch");
        assertEquals(
            rankedHits(search(endpoint, "{\"size\":10,\"query\":" + DEFAULT_FUSION + "}", dfs)),
            rankedHits(search(endpoint, "{\"size\":10,\"query\":" + CLASSIC + "}", dfs))
        );
    }

    /** {@code explain} answers in fusion mode's shape, the same trees {@code fusion: {}} answers with. */
    @SneakyThrows
    public void testClassicHybrid_whenExplained_thenAnswersAsAnEmptyFusionBlock() {
        ensureFixture();
        String endpoint = "/" + INDEX + "/_search";
        List<Object> classic = explanations(search(endpoint, "{\"size\":10,\"explain\":true,\"query\":" + CLASSIC + "}", Map.of()));
        List<Object> oracle = explanations(search(endpoint, "{\"size\":10,\"explain\":true,\"query\":" + DEFAULT_FUSION + "}", Map.of()));
        assertEquals(oracle, classic);
        assertEquals(MATCHING_DOCS, classic.size());
    }

    /**
     * Each {@code _msearch} item is decided on its own: one without a pipeline, one naming a score-ranker pipeline in its body
     * (core takes an item's pipeline from the body's {@code search_pipeline}, not from the header), whose rank-based scores
     * tell a classic answer apart from a fused one.
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public void testClassicHybrid_whenMultiSearch_thenEachItemIsDecidedOnItsOwn() {
        ensureFixture();
        Request msearch = new Request("POST", "/_msearch");
        msearch.setJsonEntity(
            "{\"index\":\""
                + INDEX
                + "\"}\n{\"size\":10,\"query\":"
                + CLASSIC
                + "}\n{\"index\":\""
                + INDEX
                + "\"}\n{\"size\":10,\"search_pipeline\":\""
                + RRF_PIPELINE
                + "\",\"query\":"
                + CLASSIC
                + "}\n"
        );
        List<Map<String, Object>> items = (List<Map<String, Object>>) asMap(client().performRequest(msearch)).get("responses");
        assertEquals(2, items.size());
        String endpoint = "/" + INDEX + "/_search";
        assertEquals(rankedHits(search(endpoint, "{\"size\":10,\"query\":" + DEFAULT_FUSION + "}", Map.of())), rankedHits(items.get(0)));
        assertEquals(
            "rank-based scores, which a fused min_max page cannot have",
            rankedHits(search(endpoint, "{\"size\":10,\"query\":" + CLASSIC + "}", Map.of("search_pipeline", RRF_PIPELINE))),
            rankedHits(items.get(1))
        );
    }

    /**
     * A classic hybrid holding a fused hybrid as a sub-query is refused while a pipeline combines the outer hybrid (mixed
     * nesting); with no pipeline, the outer hybrid runs with the default fusion and answers what {@code fusion: {}} on the outer
     * hybrid answers — a fused hybrid nested in a fused hybrid.
     */
    @SneakyThrows
    public void testClassicHybrid_whenItHoldsAFusedHybridAndNoPipelineCombinesIt_thenAnswersAsTheFusedOuterHybrid() {
        ensureFixture();
        String endpoint = "/" + INDEX + "/_search";
        String inner = "{\"hybrid\":{\"fusion\":{},\"queries\":[" + LEGS + "]}}";
        String other = "{\"term\":{\"" + TEXT_FIELD + "\":\"again\"}}";
        String classicOuter = "{\"hybrid\":{\"queries\":[" + inner + "," + other + "]}}";
        String fusedOuter = "{\"hybrid\":{\"fusion\":{},\"queries\":[" + inner + "," + other + "]}}";
        assertEquals(
            rankedHits(search(endpoint, "{\"size\":10,\"query\":" + fusedOuter + "}", Map.of())),
            rankedHits(search(endpoint, "{\"size\":10,\"query\":" + classicOuter + "}", Map.of()))
        );
    }

    // ---- helpers ----

    @SneakyThrows
    private Map<String, Object> search(final String endpoint, final String body, final Map<String, String> params) {
        Request request = new Request("POST", endpoint);
        params.forEach(request::addParameter);
        request.setJsonEntity(body);
        Response response = client().performRequest(request);
        assertEquals(endpoint + " " + params, RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return asMap(response);
    }

    @SneakyThrows
    private static Map<String, Object> asMap(final Response response) {
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> hitList(final Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        return Objects.isNull(hitList) ? List.of() : hitList;
    }

    /** The page as {@code _index/_id@_score}: the ranking and the scores in one comparable value. */
    private static List<String> rankedHits(final Map<String, Object> response) {
        List<String> ranked = new ArrayList<>();
        for (Map<String, Object> hit : hitList(response)) {
            ranked.add(hit.get("_index") + "/" + hit.get("_id") + "@" + hit.get("_score"));
        }
        return ranked;
    }

    private static List<Object> explanations(final Map<String, Object> response) {
        List<Object> explanations = new ArrayList<>();
        for (Map<String, Object> hit : hitList(response)) {
            explanations.add(hit.get("_explanation"));
        }
        return explanations;
    }

    @SuppressWarnings("unchecked")
    private static int totalHits(final Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        return ((Number) ((Map<String, Object>) hits.get("total")).get("value")).intValue();
    }

    @SuppressWarnings("unchecked")
    private static int failedShards(final Map<String, Object> response) {
        return ((Number) ((Map<String, Object>) response.get("_shards")).get("failed")).intValue();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> profileShards(final Map<String, Object> response) {
        Map<String, Object> profile = (Map<String, Object>) response.get("profile");
        assertNotNull("the response carries a profile", profile);
        return (List<Map<String, Object>>) profile.get("shards");
    }

    private static boolean hasFusedCoordinatorEntry(final Map<String, Object> response) {
        return profileShards(response).stream().anyMatch(shard -> String.valueOf(shard.get("id")).startsWith("[coordinator]"));
    }

    /** The {@code debug} map of the one fused coordinator entry. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> coordinatorDebug(final Map<String, Object> response) {
        List<Map<String, Object>> coordinator = profileShards(response).stream()
            .filter(shard -> String.valueOf(shard.get("id")).startsWith("[coordinator]"))
            .toList();
        assertEquals("one fused coordinator entry", 1, coordinator.size());
        List<Map<String, Object>> searches = (List<Map<String, Object>>) coordinator.get(0).get("searches");
        List<Map<String, Object>> query = (List<Map<String, Object>>) searches.get(0).get("query");
        return (Map<String, Object>) query.get(0).get("debug");
    }
}
