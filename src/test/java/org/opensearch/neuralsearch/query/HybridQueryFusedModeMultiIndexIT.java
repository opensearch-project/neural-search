/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;

import lombok.SneakyThrows;

/**
 * End-to-end coverage of a fused/resolver hybrid searching MULTIPLE indices, where {@code _id} alone is not a unique
 * document identity.
 *
 * <p>Fusion happens on the coordinator over the legs' returned hits, so a document has to be identified by something the
 * coordinator can see. Keyed on {@code _id} alone, a document in {@code index-a} and a <b>different</b> document in
 * {@code index-b} that happen to share an {@code _id} would be fused as if they were one entity — their scores combined —
 * and the self-erased {@code _id}-only Top clause would then match both, giving both the same fused score. Keying on
 * {@code _index} + {@code _id} keeps them distinct, and the Top clause is {@code _index}-qualified so a score lands on
 * exactly the document it was computed for.
 *
 * <p>Dataset: two indices with deliberately COLLIDING ids 1..3. The {@code owner} field records which index a document
 * came from, so the assertions can tell the two sides of a collision apart. Scores are driven by a numeric field via
 * {@code function_score} to keep the fused window deterministic and shard-independent.
 */
public class HybridQueryFusedModeMultiIndexIT extends BaseNeuralSearchIT {

    private static final String INDEX_A = "test-fused-multi-a";
    private static final String INDEX_B = "test-fused-multi-b";
    private static final String NORM_PIPELINE = "fused-multi-index-norm-pipeline";
    private static final String OWNER_FIELD = "owner";
    private static final String SCORE_FIELD = "s";
    private static final int COLLIDING_IDS = 3;

    private String indexConfig() {
        return "{\"settings\":{\"number_of_shards\":2,\"number_of_replicas\":0,"
            + "\"index.search.default_pipeline\":\""
            + NORM_PIPELINE
            + "\"},"
            + "\"mappings\":{\"properties\":{\""
            + OWNER_FIELD
            + "\":{\"type\":\"keyword\"},\""
            + SCORE_FIELD
            + "\":{\"type\":\"integer\"}}}}";
    }

    @SneakyThrows
    private void ensureDataset() {
        createSearchPipeline(NORM_PIPELINE, "min_max", "arithmetic_mean", Map.of());
        if (indexExists(INDEX_A) && indexExists(INDEX_B)) {
            return;
        }
        // Same ids in both indices — every id 1..3 exists twice, once per index.
        for (String index : List.of(INDEX_A, INDEX_B)) {
            if (indexExists(index) == false) {
                createIndex(index, indexConfig());
            }
            for (int id = 1; id <= COLLIDING_IDS; id++) {
                // index-a scores higher than index-b for the same id, so the two sides are distinguishable by score too.
                int s = (INDEX_A.equals(index) ? 100 : 50) - id;
                Request request = new Request("PUT", "/" + index + "/_doc/" + id + "?refresh=true");
                request.setJsonEntity("{\"" + OWNER_FIELD + "\":\"" + index + "\",\"" + SCORE_FIELD + "\":" + s + "}");
                Response response = client().performRequest(request);
                int code = response.getStatusLine().getStatusCode();
                assertTrue(
                    "indexing " + index + "/" + id + " failed: " + code,
                    code == RestStatus.OK.getStatus() || code == RestStatus.CREATED.getStatus()
                );
            }
        }
    }

    private String leg() {
        return "{\"function_score\":{\"query\":{\"match_all\":{}},"
            + "\"field_value_factor\":{\"field\":\""
            + SCORE_FIELD
            + "\",\"modifier\":\"none\",\"missing\":1}}}";
    }

    /** Window wide enough for every document in both indices, so the fused window itself spans both. */
    private String fusedHybrid() {
        return fusedHybrid(10);
    }

    private String fusedHybrid(int windowSize) {
        return "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + windowSize
            + ","
            + "\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\"}},"
            + "\"queries\":["
            + leg()
            + ","
            + leg()
            + "]}}";
    }

    /**
     * Colliding ids across indices must stay separate documents: all 6 (3 ids x 2 indices) come back, each id appearing
     * once per index. Keyed on {@code _id} alone this collapsed to 3 fused entries.
     */
    @SneakyThrows
    public void testFusedMultiIndex_whenIdsCollide_thenDocumentsStayDistinct() {
        ensureDataset();
        String body = "{\"query\":" + fusedHybrid() + ",\"track_total_hits\":true}";

        Map<String, Object> resp = searchRaw(body, 20);

        assertEquals("every id from both indices is returned as its own document", 2 * COLLIDING_IDS, hits(resp).size());
        assertEquals("total_hits covers both indices", (long) (2 * COLLIDING_IDS), totalHits(resp));
        Map<String, Integer> perIndex = countBy(resp, "_index");
        assertEquals("index-a contributes all of its docs", Integer.valueOf(COLLIDING_IDS), perIndex.getOrDefault(INDEX_A, 0));
        assertEquals("index-b contributes all of its docs", Integer.valueOf(COLLIDING_IDS), perIndex.getOrDefault(INDEX_B, 0));
        // Each colliding id appears exactly twice — once per index — rather than being conflated into one hit.
        Map<String, Integer> perId = countBy(resp, "_id");
        for (int id = 1; id <= COLLIDING_IDS; id++) {
            assertEquals("id " + id + " appears once per index", Integer.valueOf(2), perId.getOrDefault(String.valueOf(id), 0));
        }
    }

    /**
     * The {@code owner} field is the ground truth for "which document is this": every hit's {@code owner} must match the
     * index it was returned from. A conflated identity would surface a hit whose {@code _index} and {@code owner} disagree.
     */
    @SneakyThrows
    public void testFusedMultiIndex_whenIdsCollide_thenEachHitBelongsToItsOwnIndex() {
        ensureDataset();
        String body = "{\"query\":" + fusedHybrid() + "}";

        for (Map<String, Object> hit : hits(searchRaw(body, 20))) {
            String index = (String) hit.get("_index");
            @SuppressWarnings("unchecked")
            Map<String, Object> source = (Map<String, Object>) hit.get("_source");
            assertEquals("hit from " + index + " must carry that index's document", index, source.get(OWNER_FIELD));
        }
    }

    /**
     * Fused scores must be per-document, not shared across an id collision. index-a's documents score higher than
     * index-b's for the same id, so if the two sides had been fused into one entry both would carry an identical score.
     */
    @SneakyThrows
    public void testFusedMultiIndex_whenIdsCollide_thenScoresAreNotShared() {
        ensureDataset();
        String body = "{\"query\":" + fusedHybrid() + "}";

        Map<String, Double> scoreByOwnerAndId = new LinkedHashMap<>();
        for (Map<String, Object> hit : hits(searchRaw(body, 20))) {
            Object score = hit.get("_score");
            assertNotNull("every fused hit must be scored", score);
            scoreByOwnerAndId.put(hit.get("_index") + "#" + hit.get("_id"), ((Number) score).doubleValue());
        }

        for (int id = 1; id <= COLLIDING_IDS; id++) {
            double fromA = scoreByOwnerAndId.get(INDEX_A + "#" + id);
            double fromB = scoreByOwnerAndId.get(INDEX_B + "#" + id);
            assertTrue(
                "id "
                    + id
                    + ": index-a doc (higher raw score) must outrank the index-b doc with the same _id, got "
                    + fromA
                    + " vs "
                    + fromB,
                fromA > fromB
            );
        }
    }

    /**
     * The window is not evidence about the request. With {@code window_size} below the size of the fused set, index-a's
     * higher scores fill the whole window — yet round 2 still runs against both indices, where index-b's {@code _id} 1..3
     * are waiting. While qualification was decided from the window (one distinct index there, so "no disambiguation
     * needed") each index-b document matched its index-a namesake's Top clause and was handed that document's fused score.
     * Both sides then carried an identical score, and which one a user saw came down to Lucene's tie-break.
     */
    @SneakyThrows
    public void testFusedMultiIndex_whenWindowSpansOneIndex_thenSiblingIndexDoesNotInheritScores() {
        ensureDataset();
        // index-a's scores (99/98/97) beat index-b's (49/48/47) for every id, so a window of 3 is entirely index-a.
        String body = "{\"query\":" + fusedHybrid(COLLIDING_IDS) + "}";

        Map<String, Double> scoreByIndexAndId = new LinkedHashMap<>();
        for (Map<String, Object> hit : hits(searchRaw(body, 20))) {
            scoreByIndexAndId.put(hit.get("_index") + "#" + hit.get("_id"), ((Number) hit.get("_score")).doubleValue());
        }

        for (int id = 1; id <= COLLIDING_IDS; id++) {
            double fromA = scoreByIndexAndId.get(INDEX_A + "#" + id);
            double fromB = scoreByIndexAndId.get(INDEX_B + "#" + id);
            assertTrue("id " + id + ": the windowed index-a doc must be scored, got " + fromA, fromA > 0.0d);
            assertEquals(
                "id " + id + ": index-b's doc is outside the window, so it must score 0 rather than inherit " + fromA,
                0.0d,
                fromB,
                0.0d
            );
        }
    }

    /** Aggregations over a multi-index fused search cover every document, not a conflated subset. */
    @SneakyThrows
    public void testFusedMultiIndex_aggregationsCoverBothIndices() {
        ensureDataset();
        String body = "{\"query\":"
            + fusedHybrid()
            + ",\"aggregations\":{\"by_owner\":{\"terms\":{\"field\":\""
            + OWNER_FIELD
            + "\",\"size\":10}}},\"track_total_hits\":true}";

        Map<String, Integer> buckets = ownerBuckets(searchRaw(body, 20));

        assertEquals("agg counts index-a docs", Integer.valueOf(COLLIDING_IDS), buckets.getOrDefault(INDEX_A, 0));
        assertEquals("agg counts index-b docs", Integer.valueOf(COLLIDING_IDS), buckets.getOrDefault(INDEX_B, 0));
    }

    // ------------------------------------------------ helpers ------------------------------------------------

    @SneakyThrows
    private Map<String, Object> searchRaw(String jsonBody, int size) {
        Request request = new Request("POST", "/" + INDEX_A + "," + INDEX_B + "/_search");
        request.setJsonEntity(jsonBody);
        request.addParameter("size", Integer.toString(size));
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> hits(Map<String, Object> resp) {
        Map<String, Object> hits = (Map<String, Object>) resp.get("hits");
        List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        return hitList == null ? new ArrayList<>() : hitList;
    }

    private Map<String, Integer> countBy(Map<String, Object> resp, String field) {
        Map<String, Integer> out = new LinkedHashMap<>();
        for (Map<String, Object> hit : hits(resp)) {
            out.merge((String) hit.get(field), 1, Integer::sum);
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Integer> ownerBuckets(Map<String, Object> resp) {
        Map<String, Integer> out = new LinkedHashMap<>();
        Map<String, Object> aggs = (Map<String, Object>) resp.get("aggregations");
        if (aggs == null) {
            return out;
        }
        Map<String, Object> byOwner = (Map<String, Object>) aggs.get("by_owner");
        for (Map<String, Object> bucket : (List<Map<String, Object>>) byOwner.get("buckets")) {
            out.put((String) bucket.get("key"), ((Number) bucket.get("doc_count")).intValue());
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private long totalHits(Map<String, Object> resp) {
        Map<String, Object> hits = (Map<String, Object>) resp.get("hits");
        Object totalObj = hits.get("total");
        if (totalObj instanceof Map) {
            Object value = ((Map<String, Object>) totalObj).get("value");
            return value == null ? -1 : ((Number) value).longValue();
        }
        return -1;
    }
}
