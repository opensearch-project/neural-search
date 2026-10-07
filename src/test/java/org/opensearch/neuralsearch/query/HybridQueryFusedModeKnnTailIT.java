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
import java.util.Set;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;

import lombok.SneakyThrows;

/**
 * End-to-end coverage of how a fused/resolver hybrid renders an ANN ({@code knn}/{@code neural}) leg in the Tail.
 *
 * <p>An ANN leg the window did not truncate has returned its whole Lucene match set, so re-running it in the Tail would only
 * re-walk the HNSW graph to recount what the fan-out already retrieved. The coordinator therefore replaces such a leg with a
 * direct address of its returned hits — and because the Tail is a {@code filter}, that address decides the match set that
 * {@code total_hits}, every aggregation, and the score-0 region of the hit list are computed from. A leg that <i>filled</i>
 * the window may have matched documents it never returned, so it is kept as the real query and counted for real instead.
 *
 * <p>Addressed by {@code _id} alone, that set silently grew: on a multi-index search every same-{@code _id} document in a
 * sibling index passed the filter, so the numbers the Tail exists to make correct were the ones it inflated. This is not
 * the Top's identity bug and was not fixed by qualifying the Top — the Tail is built from the raw leg hits, never from the
 * ranked window's indices — so it needs its own end-to-end proof.
 *
 * <p>Dataset: two indices with deliberately COLLIDING ids 1..3. Only {@code index-a}'s documents carry a vector, so the
 * ANN leg's complete match set is exactly those three documents — nothing is truncated by {@code k} or by the leg's
 * {@code size} ({@code window_size} is 10 against 3 returned hits), which keeps this test about <i>addressing</i> and not
 * about materialization depth. {@code index-b}'s documents exist, share the ids, and must stay out of the match set.
 */
public class HybridQueryFusedModeKnnTailIT extends BaseNeuralSearchIT {

    private static final String INDEX_A = "test-fused-knn-tail-a";
    private static final String INDEX_B = "test-fused-knn-tail-b";
    private static final String NORM_PIPELINE = "fused-knn-tail-norm-pipeline";
    private static final String OWNER_FIELD = "owner";
    private static final String SCORE_FIELD = "s";
    private static final String VECTOR_FIELD = "vec";
    private static final String OWNER_A = "owner-a";
    private static final String OWNER_B = "owner-b";
    private static final int COLLIDING_IDS = 3;
    private static final int WINDOW_SIZE = 10;
    private static final String VECTOR_LEG_NAME = "vector_leg";
    private static final String OWNER_LEG_NAME = "owner_leg";

    private String indexConfig() {
        return "{\"settings\":{\"index\":{\"knn\":true,\"number_of_shards\":1,\"number_of_replicas\":0,"
            + "\"search.default_pipeline\":\""
            + NORM_PIPELINE
            + "\"}},"
            + "\"mappings\":{\"properties\":{\""
            + OWNER_FIELD
            + "\":{\"type\":\"keyword\"},\""
            + SCORE_FIELD
            + "\":{\"type\":\"integer\"},\""
            + VECTOR_FIELD
            + "\":{\"type\":\"knn_vector\",\"dimension\":2,"
            + "\"method\":{\"name\":\"hnsw\",\"space_type\":\"l2\",\"engine\":\"lucene\"}}}}}";
    }

    /**
     * Both indices hold ids 1..3 with the same mapping, but only index-a's documents carry a vector — so the ANN leg
     * retrieves index-a's three documents and nothing else, while index-b's three same-{@code _id} documents are exactly
     * the ones a bare {@code _id} address would drag in.
     */
    @SneakyThrows
    private void ensureDataset() {
        createSearchPipeline(NORM_PIPELINE, "min_max", "arithmetic_mean", Map.of());
        if (indexExists(INDEX_A) && indexExists(INDEX_B)) {
            return;
        }
        for (String index : List.of(INDEX_A, INDEX_B)) {
            if (indexExists(index) == false) {
                createIndex(index, indexConfig());
            }
            boolean withVector = INDEX_A.equals(index);
            for (int id = 1; id <= COLLIDING_IDS; id++) {
                String owner = withVector ? OWNER_A : OWNER_B;
                int s = (withVector ? 100 : 50) - id;
                String vector = withVector ? ",\"" + VECTOR_FIELD + "\":[1." + id + ",1.0]" : "";
                Request request = new Request("PUT", "/" + index + "/_doc/" + id + "?refresh=true");
                request.setJsonEntity("{\"" + OWNER_FIELD + "\":\"" + owner + "\",\"" + SCORE_FIELD + "\":" + s + vector + "}");
                Response response = client().performRequest(request);
                int code = response.getStatusLine().getStatusCode();
                assertTrue(
                    "indexing " + index + "/" + id + " failed: " + code,
                    code == RestStatus.OK.getStatus() || code == RestStatus.CREATED.getStatus()
                );
            }
        }
    }

    /** A materializable leg: {@code knn} is replaced in the Tail by an address of the hits it returned. */
    private String knnLeg(String filter) {
        return knnLeg(filter, WINDOW_SIZE);
    }

    /**
     * The same leg with an explicit {@code k}. Fused mode deliberately does not rewrite a leg's {@code k}, so a {@code k}
     * above {@code window_size} is how a leg comes to match documents it never returns.
     */
    private String knnLeg(String filter, int k) {
        return "{\"knn\":{\"" + VECTOR_FIELD + "\":{\"vector\":[1.1,1.0],\"k\":" + k + filter + "}}}";
    }

    /** The same leg carrying a {@code _name}, which {@code matched_queries} must report it under. */
    private String namedKnnLeg(String queryName) {
        return knnLeg(",\"_name\":\"" + queryName + "\"");
    }

    private String namedKnnLeg(String queryName, int k) {
        return knnLeg(",\"_name\":\"" + queryName + "\"", k);
    }

    /** A non-materializable leg, kept as the real query in the Tail. Matches index-a's documents only. */
    private String ownerLeg() {
        return ownerLeg("none", null);
    }

    /**
     * The owner leg, optionally named and with a chosen {@code field_value_factor} modifier. {@code reciprocal} inverts the
     * ranking to {@code 1/s}, which is how a document the ANN leg ranks last is pulled into the fused window.
     */
    private String ownerLeg(String modifier, String queryName) {
        return "{\"function_score\":{\"query\":{\"term\":{\""
            + OWNER_FIELD
            + "\":\""
            + OWNER_A
            + "\"}},\"field_value_factor\":{\"field\":\""
            + SCORE_FIELD
            + "\",\"modifier\":\""
            + modifier
            + "\",\"missing\":1}"
            + (queryName == null ? "" : ",\"_name\":\"" + queryName + "\"")
            + "}}";
    }

    private String fusedHybrid(String... legs) {
        return fusedHybrid(WINDOW_SIZE, legs);
    }

    private String fusedHybrid(int windowSize, String... legs) {
        return "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + windowSize
            + ",\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\"}},"
            + "\"queries\":["
            + String.join(",", legs)
            + "]}}";
    }

    /**
     * The regression this class exists for. Both legs match index-a's three documents and nothing else, so the fused
     * match set is those three. While the materialized ANN leg was addressed by {@code _id} alone, index-b's three
     * same-{@code _id} documents also passed the Tail filter: {@code total_hits} read 6 instead of 3, the {@code owner}
     * aggregation reported a bucket for an index no leg had matched, and the user got three score-0 hits from it.
     */
    @SneakyThrows
    public void testFusedKnnTail_whenIdsCollideAcrossIndices_thenSiblingDocsStayOutOfTheMatchSet() {
        ensureDataset();
        String body = "{\"query\":"
            + fusedHybrid(knnLeg(""), ownerLeg())
            + ",\"aggregations\":{\"by_owner\":{\"terms\":{\"field\":\""
            + OWNER_FIELD
            + "\",\"size\":10}}},\"track_total_hits\":true}";

        Map<String, Object> response = searchRaw(body, 20);

        assertEquals("only the documents the legs matched are hits", COLLIDING_IDS, hits(response).size());
        assertEquals("total_hits counts the legs' match set, not their _id twins", (long) COLLIDING_IDS, totalHits(response));
        for (Map<String, Object> hit : hits(response)) {
            assertEquals("a hit from an index no leg matched means the Tail was addressed by _id alone", INDEX_A, hit.get("_index"));
        }
        Map<String, Integer> buckets = ownerBuckets(response);
        assertEquals("the aggregation covers index-a's documents", Integer.valueOf(COLLIDING_IDS), buckets.getOrDefault(OWNER_A, 0));
        assertEquals("and counts nothing from index-b", Integer.valueOf(0), buckets.getOrDefault(OWNER_B, 0));
    }

    /**
     * An ANN leg that matched nothing must keep matching nothing. Its Tail clause is an explicit {@code match_none}: a leg
     * rendered as an empty {@code bool{should: []}} would compile to {@code MatchAllDocsQuery} and make the Tail match
     * every document in both indices — so {@code total_hits} and the aggregation would report the whole corpus instead of
     * the one leg that actually matched. The {@code filter} on the kNN leg matches no owner, so the leg returns zero hits
     * while the second leg still fuses normally.
     */
    @SneakyThrows
    public void testFusedKnnTail_whenAnnLegMatchesNothing_thenTailDoesNotFallBackToMatchAll() {
        ensureDataset();
        String noOwnerFilter = ",\"filter\":{\"term\":{\"" + OWNER_FIELD + "\":\"owner-nobody\"}}";
        String body = "{\"query\":"
            + fusedHybrid(knnLeg(noOwnerFilter), ownerLeg())
            + ",\"aggregations\":{\"by_owner\":{\"terms\":{\"field\":\""
            + OWNER_FIELD
            + "\",\"size\":10}}},\"track_total_hits\":true}";

        Map<String, Object> response = searchRaw(body, 20);

        assertEquals("an empty ANN leg contributes nothing, it does not open the Tail up", COLLIDING_IDS, hits(response).size());
        assertEquals("total_hits stays the surviving leg's match set", (long) COLLIDING_IDS, totalHits(response));
        Map<String, Integer> buckets = ownerBuckets(response);
        assertEquals(Integer.valueOf(COLLIDING_IDS), buckets.getOrDefault(OWNER_A, 0));
        assertEquals("an empty ANN leg must not turn into match_all", Integer.valueOf(0), buckets.getOrDefault(OWNER_B, 0));
    }

    /**
     * Scores must still be per-document with the ANN leg materialized: the fused window is index-a's documents, and the
     * closest vector to the query must rank first. Guards against the qualification narrowing the Tail so far that it
     * filters out the Top it is supposed to accompany — every window document has to survive its own Tail.
     */
    @SneakyThrows
    public void testFusedKnnTail_whenAnnLegIsMaterialized_thenWindowDocsSurviveTheirOwnTail() {
        ensureDataset();
        String body = "{\"query\":" + fusedHybrid(knnLeg(""), ownerLeg()) + ",\"track_total_hits\":true}";

        Map<String, Object> response = searchRaw(body, 20);

        Map<String, Double> scoreById = new LinkedHashMap<>();
        for (Map<String, Object> hit : hits(response)) {
            scoreById.put((String) hit.get("_id"), ((Number) hit.get("_score")).doubleValue());
        }
        assertEquals("every window document is returned", COLLIDING_IDS, scoreById.size());
        // The query vector is index-a's doc 2, so it is the nearest neighbour and the highest raw ANN score.
        assertTrue(
            "the document nearest the query vector must outrank the farthest, got " + scoreById,
            scoreById.get("2") > scoreById.get("3")
        );
    }

    /**
     * A materialized ANN leg answers to its own {@code _name}. The substitute is a fresh builder addressing the returned
     * hits, so it carried no name at all and the leg silently lost {@code matched_queries} — in <i>every</i> configuration,
     * Tail included, since materialization happens on both paths. What it reports is the documents the leg returned, which
     * is the same bound the match set already accepts for a materialized leg.
     */
    @SneakyThrows
    public void testFusedKnnTail_whenAnnLegIsNamed_thenTheMaterializedSubstituteKeepsTheName() {
        ensureDataset();
        String body = "{\"query\":"
            + fusedHybrid(namedKnnLeg(VECTOR_LEG_NAME), ownerLeg())
            + ",\"aggregations\":{\"by_owner\":{\"terms\":{\"field\":\""
            + OWNER_FIELD
            + "\",\"size\":10}}},\"track_total_hits\":true}";

        Map<String, Object> response = searchRaw(body, 20);

        assertEquals(COLLIDING_IDS, hits(response).size());
        for (Map<String, Object> hit : hits(response)) {
            assertEquals(
                "the materialized ANN leg must still report its name for " + hit.get("_id"),
                List.of(VECTOR_LEG_NAME),
                matchedQueries(hit)
            );
        }
    }

    /** Both halves at once: the ANN leg is materialized <i>and</i> the request is Top-only, so nothing executes the legs. */
    @SneakyThrows
    public void testFusedKnnTail_whenTopOnlyAndAnnLegIsNamed_thenTheNameIsStillReported() {
        ensureDataset();
        String body = "{\"query\":" + fusedHybrid(namedKnnLeg(VECTOR_LEG_NAME), ownerLeg()) + ",\"track_total_hits\":false}";

        Map<String, Object> response = searchRaw(body, 20);

        assertEquals("the fused window is index-a's three documents", COLLIDING_IDS, hits(response).size());
        for (Map<String, Object> hit : hits(response)) {
            assertEquals(
                "a Top-only query registers the leg without executing it, for " + hit.get("_id"),
                List.of(VECTOR_LEG_NAME),
                matchedQueries(hit)
            );
        }
    }

    /**
     * What a materialized leg's {@code _name} is worth under {@code include_named_queries_score}. The substitute is a
     * {@code bool} of {@code filter} clauses addressing the leg's returned ids, and {@code MatchedQueriesPhase} reports each
     * name from a weight built out of the named query alone — a query with no scoring clause, hence {@code 0.0}. The ANN
     * similarity is not recoverable here: the shard never sees the vector query, and re-running it for a reporting field is
     * the graph walk materialization exists to avoid.
     *
     * <p>The non-materialized leg in the same request is the control: it goes to the shard as the real
     * {@code function_score} query, so its name reports the score it actually computes. The delta is a property of
     * materialization, not of fused mode.
     */
    @SneakyThrows
    public void testFusedKnnTail_whenIncludeNamedQueriesScore_thenMaterializedLegReportsZeroAndRealLegReportsItsScore() {
        ensureDataset();
        String body = "{\"query\":"
            + fusedHybrid(namedKnnLeg(VECTOR_LEG_NAME), ownerLeg("none", OWNER_LEG_NAME))
            + ",\"track_total_hits\":true}";

        Map<String, Object> response = searchRaw(body, 20, Map.of("include_named_queries_score", "true"));

        assertEquals(COLLIDING_IDS, hits(response).size());
        for (Map<String, Object> hit : hits(response)) {
            Map<String, Double> scores = matchedQueryScores(hit);
            String id = (String) hit.get("_id");
            // Pin the oracle first: reading the array shape here would leave the map empty and every check below vacuous.
            assertEquals(
                "doc " + id + ": both legs are named, so both are reported",
                Set.of(VECTOR_LEG_NAME, OWNER_LEG_NAME),
                scores.keySet()
            );
            assertEquals("doc " + id + ": the materialized ANN leg has no scoring clause", 0.0d, scores.get(VECTOR_LEG_NAME), 0.0d);
            assertTrue(
                "doc " + id + ": the leg that reached the shard intact scores for real, got " + scores.get(OWNER_LEG_NAME),
                scores.get(OWNER_LEG_NAME) > 0.0d
            );
        }
    }

    /**
     * The truncation bound, made executable. {@code newLegRequest} caps every leg at {@code size = window_size}, so a
     * substitute can only stand for {@code min(matches, window_size)} documents — while the leg's own {@code k}, which fused
     * mode deliberately does not rewrite, decides how many it actually matched. With {@code k} (3) above
     * {@code window_size} (2) the ANN leg matches all three of index-a's documents and returns two, so an address of what it
     * returned is <i>not</i> its match set. That leg is therefore kept as the real query in the Tail and re-walked, and the
     * document outside its returned set reports the ANN name exactly as classic hybrid does.
     *
     * <p>The window-wide run is the control: the leg came back short of the window, so it was not truncated, it <i>is</i>
     * materialized, and the name still arrives — which is what makes the run above a truncation test rather than
     * materialization being switched off. Doc 1 is the control in the other direction: it was inside the returned set at
     * {@code window_size} 2 either way.
     *
     * <p>The reciprocal owner leg ranks {@code 1/s}, i.e. doc 3 first, so doc 3 enters the fused window even at
     * {@code window_size} 2.
     */
    public void testFusedKnnTail_whenAnnLegMatchedBeyondItsWindow_thenTheTruncatedDocStillReportsTheAnnName() {
        ensureDataset();
        int annK = COLLIDING_IDS;              // the ANN leg matches every one of index-a's documents...
        int truncatingWindow = annK - 1;       // ...and window_size lets it return one fewer than it matched.

        Map<String, List<String>> truncated = matchedQueriesById(annK, truncatingWindow);
        Map<String, List<String>> exact = matchedQueriesById(annK, WINDOW_SIZE);

        // Precondition: doc 3 has to come back from both runs, or the comparison below is about nothing. Note that a returned
        // document is not the same as a ranked one — the Tail is a filter, so the response is the legs' union and can exceed
        // window_size; the ranked window bounds the scored Top, not the hit count.
        assertTrue(
            "doc 3 must be returned by both runs, got " + truncated + " and " + exact,
            truncated.containsKey("3") && exact.containsKey("3")
        );

        assertEquals(
            "doc 3 was matched by the ANN leg (k=3) but sat outside the two hits window_size=2 let it return, so that leg "
                + "cannot be materialized and goes to the shard as the real query — reporting the name classic hybrid reports",
            List.of(OWNER_LEG_NAME, VECTOR_LEG_NAME),
            sorted(truncated.get("3"))
        );
        assertTrue(
            "the control: at window_size >= k the leg was not truncated, so it IS materialized and the name still arrives — "
                + "which is what makes the assertion above a truncation test and not materialization switched off, got "
                + exact,
            exact.get("3").contains(VECTOR_LEG_NAME)
        );
        assertTrue(
            "doc 1: a leg that is NOT materializable reaches the shard as the real query, so its name covers its whole "
                + "match set and not just the hits it returned, got "
                + truncated,
            truncated.get("1").contains(OWNER_LEG_NAME)
        );
        assertTrue(
            "doc 1 was inside the ANN leg's returned set even at window_size=2, so it keeps that name, got " + truncated,
            truncated.get("1").contains(VECTOR_LEG_NAME)
        );
    }

    /**
     * The silent wrong answer the bound closes, on the numbers rather than on a name. The Tail is a {@code filter}, so it
     * <i>is</i> the match set {@code total_hits} and every aggregation bucket are computed from. With {@code k} (3) above
     * {@code window_size} (2) the ANN leg matched all three of index-a's documents and returned two, and the other leg
     * contributes only doc 1 — so while the truncated leg was materialized on the strength of its writeable name alone, the
     * third document was in nobody's match set: {@code total_hits} read 2 and the {@code owner-a} bucket counted 2, at
     * HTTP 200, where classic hybrid counts 3.
     *
     * <p>This is the shape a {@code neural} leg over a {@code rank_features} semantic field always has — it rewrites into
     * {@code neural_sparse}, whose match set is every document holding a query token, and the coordinator cannot tell it
     * from a dense leg because fused mode substitutes the Tail before the legs are rewritten. A {@code knn} leg with an
     * explicit {@code k} reproduces it without needing a deployed model.
     *
     * <p>The window-wide run is the control: the leg was not truncated there, so it is still materialized and must reach the
     * same numbers. Both runs agreeing is the parity claim; the pre-fix run disagreed by exactly the truncated document.
     */
    public void testFusedKnnTail_whenAnnLegMatchedBeyondItsWindow_thenTotalHitsAndAggsStillCountTheMatchSet() {
        ensureDataset();
        int annK = COLLIDING_IDS;
        int truncatingWindow = annK - 1;

        Map<String, Object> truncated = countingRun(annK, truncatingWindow);
        Map<String, Object> exact = countingRun(annK, WINDOW_SIZE);

        assertEquals(
            "the ANN leg matched all three of index-a's documents, so all three are in the Tail's match set even though "
                + "window_size=2 let the leg return only two",
            (long) COLLIDING_IDS,
            totalHits(truncated)
        );
        assertEquals("every counted document is index-a's", Map.of(OWNER_A, COLLIDING_IDS), ownerBuckets(truncated));
        // The control run cannot be truncated, so it fixes the oracle: a window wide enough to hold the leg's whole match
        // set must produce the same numbers, or the assertions above are pinning an accident of this window rather than the
        // leg's match set.
        assertEquals("a window wider than k must count the same match set", totalHits(exact), totalHits(truncated));
        assertEquals(ownerBuckets(exact), ownerBuckets(truncated));
    }

    /**
     * The fused query for the counting run: the ANN leg at an explicit {@code k}, plus a leg narrow enough that the ANN
     * leg is the only thing that can bring the truncated document into the match set. {@code s} is {@code 100 - id} in
     * index-a and {@code 50 - id} in index-b, so {@code s = 99} is index-a's doc 1 alone.
     */
    @SneakyThrows
    private Map<String, Object> countingRun(int annK, int windowSize) {
        String narrowLeg = "{\"term\":{\"" + SCORE_FIELD + "\":99}}";
        String body = "{\"query\":"
            + fusedHybrid(windowSize, knnLeg("", annK), narrowLeg)
            + ",\"aggregations\":{\"by_owner\":{\"terms\":{\"field\":\""
            + OWNER_FIELD
            + "\",\"size\":10}}},\"track_total_hits\":true}";
        return searchRaw(body, 20);
    }

    /** A copy of the list in a stable order, so an assertion on a name set does not depend on clause order. */
    private List<String> sorted(List<String> names) {
        List<String> copy = new ArrayList<>(names);
        copy.sort(String::compareTo);
        return copy;
    }

    /** {@code matched_queries} per returned document, for the two-named-leg fused query at a given {@code k} and window. */
    @SneakyThrows
    private Map<String, List<String>> matchedQueriesById(int annK, int windowSize) {
        String body = "{\"query\":"
            + fusedHybrid(windowSize, namedKnnLeg(VECTOR_LEG_NAME, annK), ownerLeg("reciprocal", OWNER_LEG_NAME))
            + ",\"track_total_hits\":true}";
        Map<String, List<String>> namesById = new LinkedHashMap<>();
        for (Map<String, Object> hit : hits(searchRaw(body, 20))) {
            namesById.put((String) hit.get("_id"), matchedQueries(hit));
        }
        return namesById;
    }

    // ------------------------------------------------ helpers ------------------------------------------------

    /** A hit's {@code matched_queries}; an absent field reads as empty, which is the loss under test. */
    @SuppressWarnings("unchecked")
    private List<String> matchedQueries(Map<String, Object> hit) {
        Object matched = hit.get("matched_queries");
        return matched instanceof List ? (List<String>) matched : new ArrayList<>();
    }

    /**
     * A hit's {@code matched_queries} in the {@code include_named_queries_score} shape — an object of name to score rather
     * than an array of names. A wrong shape reads as an empty map, so callers must assert the key set before the values.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Double> matchedQueryScores(Map<String, Object> hit) {
        Map<String, Double> out = new LinkedHashMap<>();
        Object matched = hit.get("matched_queries");
        if (matched instanceof Map == false) {
            return out;
        }
        for (Map.Entry<String, Object> entry : ((Map<String, Object>) matched).entrySet()) {
            out.put(entry.getKey(), ((Number) entry.getValue()).doubleValue());
        }
        return out;
    }

    private Map<String, Object> searchRaw(String jsonBody, int size) {
        return searchRaw(jsonBody, size, Map.of());
    }

    @SneakyThrows
    private Map<String, Object> searchRaw(String jsonBody, int size, Map<String, String> params) {
        Request request = new Request("POST", "/" + INDEX_A + "," + INDEX_B + "/_search");
        request.setJsonEntity(jsonBody);
        request.addParameter("size", Integer.toString(size));
        params.forEach(request::addParameter);
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

    @SuppressWarnings("unchecked")
    private Map<String, Integer> ownerBuckets(Map<String, Object> response) {
        Map<String, Integer> out = new LinkedHashMap<>();
        Map<String, Object> aggregations = (Map<String, Object>) response.get("aggregations");
        if (aggregations == null) {
            return out;
        }
        Map<String, Object> byOwner = (Map<String, Object>) aggregations.get("by_owner");
        for (Map<String, Object> bucket : (List<Map<String, Object>>) byOwner.get("buckets")) {
            out.put((String) bucket.get("key"), ((Number) bucket.get("doc_count")).intValue());
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private long totalHits(Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        Object total = hits.get("total");
        if (total instanceof Map) {
            Object value = ((Map<String, Object>) total).get("value");
            return value == null ? -1 : ((Number) value).longValue();
        }
        return -1;
    }
}
