/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getNestedHits;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.processor.normalization.RRFScoreNormalizer;

import lombok.SneakyThrows;

/**
 * End-to-end integration test for the resolver (fused) mode of the {@code hybrid} query — the score-normalization family
 * (min_max, z_score, l2) combined by arithmetic_mean, plus rank-based rrf, top-level. Exercises the full coordinator flow:
 * parse the {@code fusion} parameter, fan the legs out as a MultiSearch, fuse on the coordinator via the shared fusion
 * core, and self-erase into a standard query that returns fused results.
 *
 * <p>Happy path plus the classic-vs-fused differential in
 * {@link #testFusedMode_forEveryNormalizationTechnique_thenMatchesClassicPipeline}, and rrf from both config sources (an
 * attached score-ranker-processor pipeline and an inline block); the profiler is covered by
 * {@link HybridQueryFusedModeProfileIT}, and broader coverage (nested, aggregations, explain, min_score,
 * geometric/harmonic mean) is scoped to later PRs.
 */
public class HybridQueryFusedModeIT extends BaseNeuralSearchIT {

    private static final String TEXT_FIELD = "text";
    /** Own index: the PIT test mutates the doc set mid-test, which would break the exact hit counts asserted above. */
    private static final String INDEX_FOR_PIT = "test-hybrid-fused-pit";
    private static final String RANK_FIELD = "rank";
    /** Documents in the PIT index. The fused window is bound to this count so a post-PIT doc can only enter it by
     *  evicting a real one — which is what lets this test detect legs that ignore the PIT. */
    private static final int PIT_DOCS = 3;
    /** Own index: the slice test partitions its corpus, so it must not share documents with the PIT test's mutations. */
    private static final String INDEX_FOR_SLICE = "test-hybrid-fused-slice";
    /** Enough documents that both of the index's shards hold some, so slicing partitions the corpus non-trivially. */
    private static final int SLICE_DOCS = 20;
    private static final int SLICES = 2;
    /** Own index: collapse needs a low-cardinality keyword to group on, which the text-only indices above lack. */
    private static final String INDEX_FOR_COLLAPSE = "test-hybrid-fused-collapse";
    private static final String GRP_FIELD = "grp";
    private static final int COLLAPSE_GROUPS = 2;
    private static final int DOCS_PER_GROUP = 2;
    /** Own index, and deliberately single-shard: the classic-vs-fused score comparison is only exact on one shard. */
    private static final String INDEX_FOR_FAMILY_PARITY = "test-hybrid-fused-family-parity";
    private static final int FAMILY_PARITY_DOCS = 6;
    private static final String INDEX_WITH_DEFAULT_NORM = "test-hybrid-fused-default-norm";
    private static final String INDEX_NO_PIPELINE = "test-hybrid-fused-inline-config";
    private static final String INDEX_WITH_DEFAULT_RRF = "test-hybrid-fused-default-rrf";
    private static final String INDEX_RRF_PARITY = "test-hybrid-fused-rrf-parity";
    private static final String NORM_PIPELINE = "fused-mode-norm-pipeline";
    private static final String RRF_PIPELINE = "fused-mode-rrf-pipeline";
    /** Candidate window for the parity test, comfortably above the match count so neither path truncates. */
    private static final int WINDOW = 50;

    private String indexConfigWithDefaultPipeline(String pipelineId) {
        return "{\"settings\":{\"number_of_shards\":3,\"number_of_replicas\":0,\"index.search.default_pipeline\":\""
            + pipelineId
            + "\"},\"mappings\":{\"properties\":{\""
            + TEXT_FIELD
            + "\":{\"type\":\"text\"}}}}";
    }

    private String indexConfigWithoutPipeline() {
        return indexConfigWithoutPipeline(3);
    }

    private String indexConfigWithoutPipeline(int shards) {
        return "{\"settings\":{\"number_of_shards\":"
            + shards
            + ",\"number_of_replicas\":0},\"mappings\":{\"properties\":{\""
            + TEXT_FIELD
            + "\":{\"type\":\"text\"}}}}";
    }

    @SneakyThrows
    private void addFourDocs(String index) {
        addDocument(index, "1", TEXT_FIELD, "hello world hello", null, null);
        addDocument(index, "2", TEXT_FIELD, "hello there place", null, null);
        addDocument(index, "3", TEXT_FIELD, "welcome to the place", null, null);
        addDocument(index, "4", TEXT_FIELD, "nothing relevant at all", null, null);
    }

    /**
     * A fused two-leg hybrid query. Presence of the {@code fusion} block enables the resolver; {@code source: pipeline}
     * tells it to read the normalization/combination config from the attached search pipeline (here, the index default)
     * — the same config an existing classic-hybrid user already has.
     */
    private HybridQueryBuilder fusedTwoLegQuery() {
        HybridQueryBuilder fused = new HybridQueryBuilder().fusion(Map.of("source", "pipeline"));
        fused.add(new MatchQueryBuilder(TEXT_FIELD, "hello"));
        fused.add(new TermQueryBuilder(TEXT_FIELD, "place"));
        return fused;
    }

    /**
     * The same two-leg fused query, but with the fusion config supplied <b>inline</b> on the query body instead of read
     * from a pipeline. An inline {@code normalization}/{@code combination} block enables the resolver and takes
     * precedence over any attached pipeline — so this needs no {@code index.search.default_pipeline} at all.
     */
    private HybridQueryBuilder fusedTwoLegInlineConfigQuery() {
        HybridQueryBuilder fused = new HybridQueryBuilder().fusion(
            Map.of("normalization", Map.of("technique", "min_max"), "combination", Map.of("technique", "arithmetic_mean"))
        );
        fused.add(new MatchQueryBuilder(TEXT_FIELD, "hello"));
        fused.add(new TermQueryBuilder(TEXT_FIELD, "place"));
        return fused;
    }

    @SneakyThrows
    public void testFusedMode_whenIndexDefaultNormalizationPipeline_thenFusesMinMaxArithmeticMean() {
        // Classic min_max + arithmetic_mean normalization pipeline, attached as the index default — unchanged from what
        // an existing hybrid user has today. The fused query reads this config at coordinator rewrite and self-erases.
        createSearchPipeline(NORM_PIPELINE, "min_max", "arithmetic_mean", Map.of());
        if (indexExists(INDEX_WITH_DEFAULT_NORM) == false) {
            createIndex(INDEX_WITH_DEFAULT_NORM, indexConfigWithDefaultPipeline(NORM_PIPELINE));
            addFourDocs(INDEX_WITH_DEFAULT_NORM);
        }

        Map<String, Object> response = search(INDEX_WITH_DEFAULT_NORM, fusedTwoLegQuery(), 10);

        // docs 1 (hello x2), 2 (hello + place), 3 (place) match at least one leg; doc 4 matches neither.
        assertEquals(3, getHitCount(response));
        List<Map<String, Object>> hits = getNestedHits(response);
        // doc 2 matches BOTH legs -> ranks first under min_max + arithmetic mean.
        assertEquals("2", hits.get(0).get("_id"));
        // scores are fused, strictly positive for a matched doc, and in descending order.
        double previous = Double.MAX_VALUE;
        for (Map<String, Object> hit : hits) {
            double score = ((Number) hit.get("_score")).doubleValue();
            assertTrue("fused scores must be descending", score <= previous);
            assertTrue("fused score must be > 0 for a matched doc", score > 0.0);
            previous = score;
        }
    }

    @SneakyThrows
    public void testFusedMode_whenInlineNormalizationConfig_thenFusesWithoutAnyPipeline() {
        // Resolver (fused) mode driven entirely by an inline `fusion` block — no search pipeline, no index default. This
        // exercises the FusionSpec.fromInlineFusion path (distinct from the pipeline-resolution path above), proving the
        // config can travel on the query body alone.
        if (indexExists(INDEX_NO_PIPELINE) == false) {
            createIndex(INDEX_NO_PIPELINE, indexConfigWithoutPipeline());
            addFourDocs(INDEX_NO_PIPELINE);
        }

        Map<String, Object> response = search(INDEX_NO_PIPELINE, fusedTwoLegInlineConfigQuery(), 10);

        // Same corpus/legs as the pipeline test: docs 1,2,3 match at least one leg; doc 4 matches neither.
        assertEquals(3, getHitCount(response));
        List<Map<String, Object>> hits = getNestedHits(response);
        // doc 2 matches BOTH legs -> ranks first under min_max + arithmetic mean, identical to the pipeline-config path.
        assertEquals("2", hits.get(0).get("_id"));
        double previous = Double.MAX_VALUE;
        for (Map<String, Object> hit : hits) {
            double score = ((Number) hit.get("_score")).doubleValue();
            assertTrue("fused scores must be descending", score <= previous);
            assertTrue("fused score must be > 0 for a matched doc", score > 0.0);
            previous = score;
        }
    }

    /**
     * A user-supplied point-in-time must be honored by the whole fused flow, legs included.
     *
     * <p>Fused mode opens N leg searches plus the round-2 self-erased query, so without a shared view those are N+1
     * independent reader instants and a concurrently indexed document can appear in some of them but not others. Passing
     * the request's PIT down to every leg makes them all read one immutable snapshot.
     *
     * <p>The probe: take a PIT, then index a document that ranks ABOVE everything already there. Through the PIT it must
     * stay invisible, while a live search sees it.
     *
     * <p>Three details make this a real regression guard rather than a tautology:
     * <ul>
     *   <li>Both legs score by a numeric field via {@code function_score}, so the fused order is exactly the field order —
     *       deterministic and shard-independent, unlike BM25 (whose min_max floor can actually sink a newly added short
     *       document to the BOTTOM of a leg, which would hide the defect entirely).</li>
     *   <li>{@code window_size} is bound to the existing document count, so a document that should be invisible can only
     *       enter the window by evicting a real one.</li>
     *   <li>The query is Top-only ({@code track_total_hits:false}); with the Tail present the legs would be re-matched
     *       directly and return the real documents regardless of what the Top holds, masking the defect.</li>
     * </ul>
     * Top-only, the returned hits ARE the fused window: if the legs ignored the PIT they would rank the new document in,
     * round-2 would read the PIT, fail to match that id, and the request would return FEWER hits than the window holds.
     * Verified by mutation — removing the PIT passthrough from the legs makes this test fail with 2 hits instead of 3.
     */
    @SneakyThrows
    public void testFusedMode_whenPointInTimeSupplied_thenLegsAndRoundTwoShareOneSnapshot() {
        if (indexExists(INDEX_FOR_PIT) == false) {
            createIndex(INDEX_FOR_PIT, indexConfigWithRankField());
            for (int id = 1; id <= PIT_DOCS; id++) {
                indexRankedDoc(id, id * 10);
            }
        }
        String pitId = createPointInTime(INDEX_FOR_PIT);
        try {
            assertEquals("the Top-only fused window holds every document", PIT_DOCS, getHitCount(searchWithPit(pitId)));

            // A document that outranks everything present, so live legs would put it at the head of the window.
            indexRankedDoc(PIT_DOCS + 1, 100_000);

            Map<String, Object> throughPit = searchWithPit(pitId);
            assertEquals(
                "PIT snapshot must not see the doc indexed after it was taken, and no window slot may be lost to it",
                PIT_DOCS,
                getHitCount(throughPit)
            );
            for (Map<String, Object> hit : getNestedHits(throughPit)) {
                assertNotEquals("the post-PIT doc must not leak into the fused window", String.valueOf(PIT_DOCS + 1), hit.get("_id"));
            }

            // Sanity: a live search does rank it first, so the assertions above are about the snapshot — not about the
            // document failing to index or to match the legs.
            List<Map<String, Object>> liveHits = getNestedHits(searchLive());
            assertEquals("a live search ranks the new doc first", String.valueOf(PIT_DOCS + 1), liveHits.get(0).get("_id"));
        } finally {
            deletePointInTime(pitId);
        }
    }

    private String indexConfigWithRankField() {
        return indexConfigWithRankField(3);
    }

    private String indexConfigWithRankField(int shards) {
        return "{\"settings\":{\"number_of_shards\":"
            + shards
            + ",\"number_of_replicas\":0},\"mappings\":{\"properties\":{\""
            + RANK_FIELD
            + "\":{\"type\":\"integer\"}}}}";
    }

    private void indexRankedDoc(int id, int rank) {
        indexRankedDoc(INDEX_FOR_PIT, id, rank);
    }

    @SneakyThrows
    private void indexRankedDoc(String index, int id, int rank) {
        Request request = new Request("PUT", "/" + index + "/_doc/" + id + "?refresh=true");
        request.setJsonEntity("{\"" + RANK_FIELD + "\":" + rank + "}");
        Response response = client().performRequest(request);
        int code = response.getStatusLine().getStatusCode();
        assertTrue("indexing doc " + id + " failed: " + code, code == RestStatus.OK.getStatus() || code == RestStatus.CREATED.getStatus());
    }

    /** Open a point-in-time over the index and return its id. */
    @SneakyThrows
    private String createPointInTime(String index) {
        Request request = new Request("POST", "/" + index + "/_search/point_in_time?keep_alive=5m");
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        Map<String, Object> body = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(response.getEntity()),
            false
        );
        String pitId = (String) body.get("pit_id");
        assertNotNull("pit_id must be returned", pitId);
        return pitId;
    }

    @SneakyThrows
    private void deletePointInTime(String pitId) {
        Request request = new Request("DELETE", "/_search/point_in_time");
        request.setJsonEntity("{\"pit_id\":[\"" + pitId + "\"]}");
        client().performRequest(request);
    }

    /** Two legs that both score by the numeric rank field, so the fused order is exactly the rank order. */
    private String rankedFusedQuery() {
        return rankedFusedQuery(PIT_DOCS);
    }

    private String rankedFusedQuery(int windowSize) {
        String leg = "{\"function_score\":{\"query\":{\"match_all\":{}},\"field_value_factor\":{\"field\":\""
            + RANK_FIELD
            + "\",\"modifier\":\"none\",\"missing\":1}}}";
        return "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + windowSize
            + ",\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\"}},"
            + "\"queries\":["
            + leg
            + ","
            + leg
            + "]}}";
    }

    /**
     * Fused search against a PIT, Top-only. The index deliberately does NOT appear in the path: a PIT already pins its own
     * indices and core rejects a PIT request that also names them.
     */
    @SneakyThrows
    private Map<String, Object> searchWithPit(String pitId) {
        return searchRaw(
            "/_search",
            "{\"pit\":{\"id\":\"" + pitId + "\",\"keep_alive\":\"5m\"},\"track_total_hits\":false,\"query\":" + rankedFusedQuery() + "}"
        );
    }

    /** The same fused query without a PIT, for the live-visibility contrast. */
    @SneakyThrows
    private Map<String, Object> searchLive() {
        return searchRaw("/" + INDEX_FOR_PIT + "/_search", "{\"track_total_hits\":false,\"query\":" + rankedFusedQuery() + "}");
    }

    private Map<String, Object> searchRaw(String endpoint, String jsonBody) {
        return searchRaw(endpoint, jsonBody, 10);
    }

    @SneakyThrows
    private Map<String, Object> searchRaw(String endpoint, String jsonBody, int size) {
        Request request = new Request("POST", endpoint);
        request.addParameter("size", String.valueOf(size));
        request.setJsonEntity(jsonBody);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    /**
     * {@code slice} must reach every leg. A sliced request returns only its slice, so legs that ignored the slice would
     * fill the window with documents belonging to other slices, and each slice would come back an arbitrary fraction of
     * itself. That is why {@code CandidateScope} classifies {@code sliceBuilder} as propagated rather than ignored.
     *
     * <p>Slicing is legal only over a scroll or a point-in-time, and fused mode refuses scroll
     * ({@link #testFusedMode_whenScroll_thenRejectedWithValidationError}), so two slices over one PIT is the only shape
     * that reaches this code at all — before this test the word "slice" appeared in no fused test.
     *
     * <p>The probe: learn each slice's true membership from core alone with a plain {@code match_all} over the same PIT,
     * then run the fused query per slice with a {@code window_size} that covers the largest slice but is still smaller
     * than the corpus. With the slice propagated, each slice's window holds exactly the documents that slice owns.
     * Without it, both legs return the global top {@code window_size} by rank and round 2's slice filter keeps only the
     * part that happens to fall inside the slice, so the slices together return fewer than {@link #SLICE_DOCS} documents
     * and the per-slice equality fails.
     *
     * <p>Two details keep it honest: {@code window_size} is asserted to be below the corpus size, since a window that
     * covered everything would make an unsliced leg indistinguishable from a sliced one; and the query is Top-only
     * ({@code track_total_hits:false}), because with the Tail present round 2 re-matches the legs directly under the
     * shard's slice filter and returns the whole slice regardless of what the Top holds.
     */
    @SneakyThrows
    public void testFusedMode_whenSlicedOverPointInTime_thenEachSliceReturnsItsOwnDocuments() {
        ensureSliceDataset();
        String pitId = createPointInTime(INDEX_FOR_SLICE);
        try {
            // Ground truth, established by core with no hybrid query involved: which documents each slice owns.
            List<Set<String>> membership = new ArrayList<>();
            for (int slice = 0; slice < SLICES; slice++) {
                membership.add(idsOf(searchRaw("/_search", slicedPitBody(pitId, slice, "{\"match_all\":{}}"), SLICE_DOCS * 2)));
            }
            Set<String> union = new HashSet<>();
            int largestSlice = 0;
            for (Set<String> slice : membership) {
                for (String id : slice) {
                    assertTrue("core's own slices must not overlap", union.add(id));
                }
                largestSlice = Math.max(largestSlice, slice.size());
            }
            assertEquals("core's slices must together cover the corpus", SLICE_DOCS, union.size());
            assertTrue(
                "the window must stay below the corpus size or an unsliced leg would cover every slice anyway",
                largestSlice < SLICE_DOCS
            );

            for (int slice = 0; slice < SLICES; slice++) {
                Set<String> fused = idsOf(
                    searchRaw("/_search", slicedPitBody(pitId, slice, rankedFusedQuery(largestSlice)), SLICE_DOCS * 2)
                );
                assertEquals("slice " + slice + " must return exactly the documents it owns", membership.get(slice), fused);
            }
        } finally {
            deletePointInTime(pitId);
        }
    }

    /**
     * Fused mode refuses {@code scroll} with a validation error instead of letting it fail deep in the shards. A scroll
     * pages one reader snapshot and that snapshot covers round 2 alone: the legs that chose the window ran as separate
     * one-shot searches against their own reader instants and are never re-run, so later pages would be paged out of a
     * ranking round 1 cannot reproduce. Classic hybrid refuses scroll too.
     *
     * <p>What this pins is the diagnosis, not just the failure. Left unguarded, {@code scroll} plus {@code slice} reaches
     * the shards, where slicing rejects the scroll-less leg requests and the user gets "all shards failed" naming the
     * slice — a whole-request failure with a misleading cause. The error asserted here names {@code scroll} as the
     * unsupported parameter and points at {@code point_in_time}, which fused mode does support
     * ({@link #testFusedMode_whenPointInTimeSupplied_thenLegsAndRoundTwoShareOneSnapshot}).
     */
    @SneakyThrows
    public void testFusedMode_whenScroll_thenRejectedWithValidationError() {
        ensureSliceDataset();
        Request request = new Request("POST", "/" + INDEX_FOR_SLICE + "/_search");
        request.addParameter("scroll", "1m");
        request.addParameter("size", "10");
        // No track_total_hits here: core itself rejects disabling it in a scroll context, which would pre-empt the
        // rejection under test.
        request.setJsonEntity("{\"query\":" + rankedFusedQuery(SLICE_DOCS) + "}");

        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));

        assertEquals(RestStatus.BAD_REQUEST.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
        String body = EntityUtils.toString(exception.getResponse().getEntity());
        assertTrue("the error must name the unsupported parameter, not a shard-level symptom: " + body, body.contains("[scroll]"));
        assertTrue("the error must point at the shape that does work: " + body, body.contains("point_in_time"));
    }

    /** A PIT-scoped search body for one slice. The index never appears in the path: a PIT already pins its own indices. */
    private String slicedPitBody(String pitId, int slice, String query) {
        return "{\"pit\":{\"id\":\""
            + pitId
            + "\",\"keep_alive\":\"5m\"},\"slice\":{\"id\":"
            + slice
            + ",\"max\":"
            + SLICES
            + "},\"track_total_hits\":false,\"query\":"
            + query
            + "}";
    }

    private Set<String> idsOf(Map<String, Object> response) {
        Set<String> ids = new HashSet<>();
        for (Map<String, Object> hit : getNestedHits(response)) {
            ids.add((String) hit.get("_id"));
        }
        return ids;
    }

    @SneakyThrows
    private void ensureSliceDataset() {
        if (indexExists(INDEX_FOR_SLICE)) {
            return;
        }
        // Two shards and two slices, so each slice maps to exactly one shard and the partition is deterministic.
        createIndex(INDEX_FOR_SLICE, indexConfigWithRankField(SLICES));
        for (int id = 1; id <= SLICE_DOCS; id++) {
            indexRankedDoc(INDEX_FOR_SLICE, id, id * 10);
        }
    }

    /**
     * Collapse GROUPING over a fused query must match classic hybrid exactly — it is a plain query-phase operation over
     * the fused ranking, with no fused-specific handling. This is a real parity guard, not a limitation pin.
     */
    @SneakyThrows
    public void testFusedMode_whenCollapse_thenGroupingMatchesClassic() {
        ensureCollapseDataset();
        String collapse = "\"collapse\":{\"field\":\"" + GRP_FIELD + "\"}";

        List<Map<String, Object>> fusedHits = getNestedHits(
            searchRaw("/" + INDEX_FOR_COLLAPSE + "/_search", "{\"query\":" + collapseFusedQuery() + "," + collapse + "}")
        );
        List<Map<String, Object>> classicHits = getNestedHits(
            searchRaw("/" + INDEX_FOR_COLLAPSE + "/_search", "{\"query\":" + collapseClassicQuery() + "," + collapse + "}")
        );

        assertEquals("collapse yields one hit per group", COLLAPSE_GROUPS, fusedHits.size());
        assertEquals("fused and classic collapse to the same number of groups", classicHits.size(), fusedHits.size());
        // Same groups, in the same order, represented by the same documents.
        for (int i = 0; i < fusedHits.size(); i++) {
            assertEquals("group key order matches classic", collapseKey(classicHits.get(i)), collapseKey(fusedHits.get(i)));
            assertEquals("group representative matches classic", classicHits.get(i).get("_id"), fusedHits.get(i).get("_id"));
        }
    }

    /**
     * With {@code collapse.inner_hits}, an expanded member that is <b>inside the fused window</b> carries ITS OWN fused
     * score — the same score it receives in the ungrouped fused search, on the same scale as the group representative.
     * Every document here is inside the default window, which is what makes that unconditional in this test; the
     * out-of-window case is
     * {@link #testFusedMode_whenCollapseInnerHitsAndMemberOutsideWindow_thenGroupStillExpandsFully}.
     *
     * <p>Core's {@code ExpandSearchPhase} re-runs {@code source().query()} once per collapse group. For a fused request
     * that query is the self-erased Top (plus Tail), where each ranked document has its own {@code constant_score} clause,
     * so each member is scored by its own clause rather than the representative's.
     *
     * <p>This differs from classic hybrid, which re-runs the real sub-queries and reports their raw, un-normalized scores
     * — putting members on a different scale from the representative (classic yields a representative at {@code 1.0}
     * beside members at {@code 198.0}). Fused is self-consistent instead, which is why this asserts the fused values
     * rather than classic parity.
     */
    @SneakyThrows
    public void testFusedMode_whenCollapseInnerHits_thenMembersCarryTheirOwnFusedScore() {
        ensureCollapseDataset();

        // Ground truth: the fused score each document receives with no collapse applied.
        Map<String, Double> fusedScoreById = new HashMap<>();
        for (Map<String, Object> hit : getNestedHits(
            searchRaw("/" + INDEX_FOR_COLLAPSE + "/_search", "{\"query\":" + collapseFusedQuery() + "}")
        )) {
            fusedScoreById.put((String) hit.get("_id"), ((Number) hit.get("_score")).doubleValue());
        }
        assertEquals("every document is fused", COLLAPSE_GROUPS * DOCS_PER_GROUP, fusedScoreById.size());

        String body = "{\"query\":"
            + collapseFusedQuery()
            + ",\"collapse\":{\"field\":\""
            + GRP_FIELD
            + "\",\"inner_hits\":{\"name\":\"members\",\"size\":10}}}";

        List<Map<String, Object>> groups = getNestedHits(searchRaw("/" + INDEX_FOR_COLLAPSE + "/_search", body));

        assertEquals(COLLAPSE_GROUPS, groups.size());
        for (Map<String, Object> group : groups) {
            List<Map<String, Object>> members = innerHits(group, "members");
            assertEquals("each group expands to all of its documents", DOCS_PER_GROUP, members.size());
            for (Map<String, Object> member : members) {
                String memberId = (String) member.get("_id");
                assertEquals(
                    "expanded member " + memberId + " must carry its own fused score, not the representative's",
                    fusedScoreById.get(memberId),
                    ((Number) member.get("_score")).doubleValue(),
                    1e-6
                );
            }
            // The representative is the group's best-scoring member, consistent with the fused ranking.
            double representativeScore = ((Number) group.get("_score")).doubleValue();
            for (Map<String, Object> member : members) {
                assertTrue(
                    "no member may outrank its group representative",
                    ((Number) member.get("_score")).doubleValue() <= representativeScore + 1e-6
                );
            }
        }
    }

    /**
     * A collapse group's members are whatever share the representative's collapse key, which has nothing to do with the
     * fused window — so expanding a group must not be limited to the window. Core's {@code ExpandSearchPhase} issues one
     * search per returned group whose query is the self-erased fused query under a group-key filter; with Top only, a
     * member that ranked outside the window matches no {@code constant_score} clause and silently disappears from the
     * expansion, where classic hybrid (and any other query type) returns the whole group. That is why
     * {@code collapse.inner_hits} forces the Tail on.
     *
     * <p>The probe: {@code window_size} one short of the document count, so exactly one member — the lowest-ranked, in the
     * last group — sits outside the window, plus {@code track_total_hits:false} so nothing else would keep the Tail. Every
     * document must still come back in its group's expansion. Without the Tail trigger the last group expands to one
     * member instead of {@link #DOCS_PER_GROUP}.
     *
     * <p>Also pins the score split that remains, because it is a real difference from classic and is documented as such:
     * an in-window member carries its own fused score, while the out-of-window member has no Top clause to score it and
     * expands at {@code 0.0}.
     */
    @SneakyThrows
    public void testFusedMode_whenCollapseInnerHitsAndMemberOutsideWindow_thenGroupStillExpandsFully() {
        ensureCollapseDataset();
        int totalDocs = COLLAPSE_GROUPS * DOCS_PER_GROUP;
        int window = totalDocs - 1;

        // Ground truth: the fused score of each document that IS in the window, from the same Top-only query ungrouped.
        Map<String, Double> fusedScoreById = new HashMap<>();
        for (Map<String, Object> hit : getNestedHits(
            searchRaw("/" + INDEX_FOR_COLLAPSE + "/_search", "{\"track_total_hits\":false,\"query\":" + collapseFusedQuery(window) + "}")
        )) {
            fusedScoreById.put((String) hit.get("_id"), ((Number) hit.get("_score")).doubleValue());
        }
        assertEquals("the window holds one document fewer than the corpus", window, fusedScoreById.size());

        String body = "{\"track_total_hits\":false,\"query\":"
            + collapseFusedQuery(window)
            + ",\"collapse\":{\"field\":\""
            + GRP_FIELD
            + "\",\"inner_hits\":{\"name\":\"members\",\"size\":10}}}";

        List<Map<String, Object>> groups = getNestedHits(searchRaw("/" + INDEX_FOR_COLLAPSE + "/_search", body));

        assertEquals(COLLAPSE_GROUPS, groups.size());
        Set<String> expanded = new HashSet<>();
        for (Map<String, Object> group : groups) {
            List<Map<String, Object>> members = innerHits(group, "members");
            assertEquals("a group expands to all of its members, window membership notwithstanding", DOCS_PER_GROUP, members.size());
            for (Map<String, Object> member : members) {
                String memberId = (String) member.get("_id");
                assertTrue("no document may be expanded twice", expanded.add(memberId));
                double memberScore = ((Number) member.get("_score")).doubleValue();
                if (fusedScoreById.containsKey(memberId)) {
                    assertEquals(
                        "in-window member " + memberId + " carries its own fused score",
                        fusedScoreById.get(memberId),
                        memberScore,
                        1e-6
                    );
                } else {
                    // Documented limitation: fusion ranked the window, so a document outside it has no fused score to
                    // carry. It matches the Tail, which is a filter and therefore contributes nothing to the score.
                    assertEquals("the out-of-window member matches only the non-scoring Tail", 0.0, memberScore, 1e-6);
                }
            }
        }
        assertEquals("every document is expanded into exactly one group", totalDocs, expanded.size());
    }

    private String indexConfigWithGroupField() {
        return "{\"settings\":{\"number_of_shards\":2,\"number_of_replicas\":0,\"index.search.default_pipeline\":\""
            + NORM_PIPELINE
            + "\"},\"mappings\":{\"properties\":{\""
            + GRP_FIELD
            + "\":{\"type\":\"keyword\"},\""
            + RANK_FIELD
            + "\":{\"type\":\"integer\"}}}}";
    }

    @SneakyThrows
    private void ensureCollapseDataset() {
        // Classic hybrid needs a normalization pipeline; the index default supplies it for the comparison test.
        createSearchPipeline(NORM_PIPELINE, "min_max", "arithmetic_mean", Map.of());
        if (indexExists(INDEX_FOR_COLLAPSE)) {
            return;
        }
        createIndex(INDEX_FOR_COLLAPSE, indexConfigWithGroupField());
        int id = 1;
        for (int group = 0; group < COLLAPSE_GROUPS; group++) {
            for (int member = 0; member < DOCS_PER_GROUP; member++) {
                Request request = new Request("PUT", "/" + INDEX_FOR_COLLAPSE + "/_doc/" + id + "?refresh=true");
                // Distinct ranks so both the fused order and the per-group representative are deterministic.
                request.setJsonEntity("{\"" + GRP_FIELD + "\":\"g" + group + "\",\"" + RANK_FIELD + "\":" + (100 - id) + "}");
                Response response = client().performRequest(request);
                int code = response.getStatusLine().getStatusCode();
                assertTrue(
                    "indexing doc " + id + " failed: " + code,
                    code == RestStatus.OK.getStatus() || code == RestStatus.CREATED.getStatus()
                );
                id++;
            }
        }
    }

    /** Two legs scoring by the numeric rank field, so the fused order is deterministic and shard-independent. */
    private String collapseLeg() {
        return "{\"function_score\":{\"query\":{\"match_all\":{}},\"field_value_factor\":{\"field\":\""
            + RANK_FIELD
            + "\",\"modifier\":\"none\",\"missing\":1}}}";
    }

    private String collapseFusedQuery() {
        return collapseFusedQuery(0);
    }

    /** The collapse-dataset fused query; {@code windowSize} of 0 leaves {@code window_size} at its default. */
    private String collapseFusedQuery(int windowSize) {
        return "{\"hybrid\":{\"fusion\":{"
            + (windowSize > 0 ? "\"window_size\":" + windowSize + "," : "")
            + "\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\"}},"
            + "\"queries\":["
            + collapseLeg()
            + ","
            + collapseLeg()
            + "]}}";
    }

    private String collapseClassicQuery() {
        return "{\"hybrid\":{\"queries\":[" + collapseLeg() + "," + collapseLeg() + "]}}";
    }

    @SuppressWarnings("unchecked")
    private String collapseKey(Map<String, Object> hit) {
        List<Object> fields = (List<Object>) ((Map<String, Object>) hit.get("fields")).get(GRP_FIELD);
        return String.valueOf(fields.get(0));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> innerHits(Map<String, Object> hit, String name) {
        Map<String, Object> inner = (Map<String, Object>) hit.get("inner_hits");
        assertNotNull("collapse.inner_hits must be present", inner);
        Map<String, Object> named = (Map<String, Object>) inner.get(name);
        Map<String, Object> hits = (Map<String, Object>) named.get("hits");
        return (List<Map<String, Object>>) hits.get("hits");
    }

    /**
     * End-to-end differential test for the whole score-normalization family: for each of {@code min_max}, {@code z_score}
     * and {@code l2}, the same two legs fused on the coordinator must return the same documents, in the same order, with
     * the same scores as the classic shard-side {@code normalization-processor} pipeline using that technique. This is the
     * plumbing counterpart to {@code CoordinatorScoreFusionDifferentialTests}, which pins the arithmetic in isolation:
     * together they say the shared cores are both correct and actually reached from both paths.
     *
     * <p>Three properties of the fixture make the comparison well-defined:
     * <ul>
     *   <li><b>One shard.</b> {@code l2} accumulates its sum of squares in {@code float} and float addition is not
     *       associative, so classic's per-shard accumulation and the coordinator's accumulation over the merged leg are
     *       bit-identical on one shard and may differ in the last bit across shards.</li>
     *   <li><b>Scores come from a numeric field, not BM25.</b> Raw leg scores are then exactly the {@code rank} values,
     *       so no score depends on per-shard term statistics and the expected ordering is readable from the data.</li>
     *   <li><b>The legs match different document sets.</b> Leg B is filtered to the top ranks, so most documents match
     *       one leg only — which is what exercises the "leg did not match" path on both sides rather than comparing two
     *       fully-overlapping legs where normalization differences would largely cancel.</li>
     * </ul>
     */
    @SneakyThrows
    public void testFusedMode_forEveryNormalizationTechnique_thenMatchesClassicPipeline() {
        if (indexExists(INDEX_FOR_FAMILY_PARITY) == false) {
            createIndex(INDEX_FOR_FAMILY_PARITY, indexConfigWithRankField(1));
            for (int id = 1; id <= FAMILY_PARITY_DOCS; id++) {
                indexRankedDoc(INDEX_FOR_FAMILY_PARITY, id, id * 10);
            }
        }

        for (String technique : List.of("min_max", "z_score", "l2")) {
            String pipeline = "fused-mode-parity-" + technique;
            createSearchPipeline(pipeline, technique, "arithmetic_mean", Map.of());

            List<Map<String, Object>> classicHits = getNestedHits(
                searchRawWithParams(
                    "/" + INDEX_FOR_FAMILY_PARITY + "/_search",
                    "{\"query\":" + familyParityQuery(null) + "}",
                    Map.of("search_pipeline", pipeline)
                )
            );
            List<Map<String, Object>> fusedHits = getNestedHits(
                searchRaw("/" + INDEX_FOR_FAMILY_PARITY + "/_search", "{\"query\":" + familyParityQuery(technique) + "}")
            );

            assertEquals("same document count for " + technique, classicHits.size(), fusedHits.size());
            assertTrue("fixture must return documents for " + technique, classicHits.isEmpty() == false);
            for (int i = 0; i < classicHits.size(); i++) {
                String classicId = (String) classicHits.get(i).get("_id");
                double classicScore = ((Number) classicHits.get(i).get("_score")).doubleValue();
                assertEquals("rank " + i + " document for " + technique, classicId, fusedHits.get(i).get("_id"));
                // Relative tolerance: the two paths run the same float arithmetic, but the scores travel through JSON as
                // doubles, so pin agreement to float precision rather than to the exact decimal rendering.
                assertEquals(
                    "fused score for doc " + classicId + " with " + technique,
                    classicScore,
                    ((Number) fusedHits.get(i).get("_score")).doubleValue(),
                    Math.max(1e-6, Math.abs(classicScore) * 1e-5)
                );
            }
        }
    }

    /**
     * Two legs over the rank field: leg A matches everything, leg B only the top ranks. Passing a {@code technique}
     * produces the fused form (inline {@code fusion} block, coordinator path); passing null produces the classic form,
     * which takes its technique from the {@code search_pipeline} request parameter instead.
     */
    private String familyParityQuery(String normalizationTechnique) {
        String legScoringByRank = "{\"function_score\":{\"query\":{\"match_all\":{}},\"field_value_factor\":{\"field\":\""
            + RANK_FIELD
            + "\",\"modifier\":\"none\",\"missing\":1}}}";
        String legScoringTopRanksOnly = "{\"function_score\":{\"query\":{\"range\":{\""
            + RANK_FIELD
            + "\":{\"gte\":"
            + (FAMILY_PARITY_DOCS * 10 / 2)
            + "}}},\"field_value_factor\":{\"field\":\""
            + RANK_FIELD
            + "\",\"modifier\":\"sqrt\",\"missing\":1}}}";
        String fusionBlock = Objects.isNull(normalizationTechnique)
            ? ""
            : "\"fusion\":{\"window_size\":"
                + FAMILY_PARITY_DOCS
                + ",\"normalization\":{\"technique\":\""
                + normalizationTechnique
                + "\"},\"combination\":{\"technique\":\"arithmetic_mean\"}},";
        return "{\"hybrid\":{" + fusionBlock + "\"queries\":[" + legScoringByRank + "," + legScoringTopRanksOnly + "]}}";
    }

    @SneakyThrows
    private Map<String, Object> searchRawWithParams(String endpoint, String jsonBody, Map<String, String> params) {
        Request request = new Request("POST", endpoint);
        request.addParameter("size", "10");
        params.forEach(request::addParameter);
        request.setJsonEntity(jsonBody);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    /** The same two legs, with RRF config supplied inline. RRF takes no normalization clause. */
    private HybridQueryBuilder fusedTwoLegInlineRrfQuery(int rankConstant) {
        HybridQueryBuilder fused = new HybridQueryBuilder().fusion(
            Map.of("combination", Map.of("technique", "rrf", "rank_constant", rankConstant))
        );
        fused.add(new MatchQueryBuilder(TEXT_FIELD, "hello"));
        fused.add(new TermQueryBuilder(TEXT_FIELD, "place"));
        return fused;
    }

    /**
     * Assert every fused score is exactly a sum of RRF rank scores for the given rank constant. Each leg here returns two
     * hits, so a doc's score is {@code scoreForRank(0|1)} if it matched one leg and the sum of two such values if it
     * matched both — layout-independent, unlike the individual ranks, which depend on how the 3 shards split the corpus.
     */
    private void assertScoresAreRrfRankSums(List<Map<String, Object>> hits, int rankConstant) {
        float rank0 = RRFScoreNormalizer.scoreForRank(0, rankConstant);
        float rank1 = RRFScoreNormalizer.scoreForRank(1, rankConstant);
        Set<Double> oneLeg = Set.of((double) rank0, (double) rank1);
        Set<Double> twoLegs = Set.of((double) (rank0 + rank0), (double) (rank0 + rank1), (double) (rank1 + rank1));
        double previous = Double.MAX_VALUE;
        for (Map<String, Object> hit : hits) {
            // _score comes back as a float-precision JSON number; compare in float space.
            double score = ((Number) hit.get("_score")).floatValue();
            assertTrue("fused scores must be descending", score <= previous);
            boolean expected = "2".equals(hit.get("_id")) ? twoLegs.contains(score) : oneLeg.contains(score);
            assertTrue(
                "doc " + hit.get("_id") + " score " + score + " is not an RRF rank-score sum for rank_constant " + rankConstant,
                expected
            );
            previous = score;
        }
    }

    @SneakyThrows
    public void testFusedMode_whenIndexDefaultScoreRankerPipeline_thenFusesRrf() {
        // Classic score-ranker-processor (rrf) pipeline attached as the index default — again unchanged from what an
        // existing RRF user has. The fused query reads it at coordinator rewrite and fuses by rank, not by normalized
        // score, so every returned score must be an exact sum of rank scores.
        createRRFSearchPipeline(RRF_PIPELINE, List.of(), RRFScoreNormalizer.DEFAULT_RANK_CONSTANT, false);
        if (indexExists(INDEX_WITH_DEFAULT_RRF) == false) {
            createIndex(INDEX_WITH_DEFAULT_RRF, indexConfigWithDefaultPipeline(RRF_PIPELINE));
            addFourDocs(INDEX_WITH_DEFAULT_RRF);
        }

        Map<String, Object> response = search(INDEX_WITH_DEFAULT_RRF, fusedTwoLegQuery(), 10);

        assertEquals(3, getHitCount(response));
        List<Map<String, Object>> hits = getNestedHits(response);
        // doc 2 is the only doc matching both legs, so its rank-score sum beats any single-leg score whatever the ranks.
        assertEquals("2", hits.get(0).get("_id"));
        assertScoresAreRrfRankSums(hits, RRFScoreNormalizer.DEFAULT_RANK_CONSTANT);
    }

    @SneakyThrows
    public void testFusedMode_whenInlineRrfConfig_thenRankConstantIsHonored() {
        // Inline rrf with a non-default rank constant, no pipeline at all. Reusing the pipeline-less index proves both
        // that FusionSpec.fromInlineFusion carries the rrf shape and that the rank constant reaches the fusion core —
        // the scores match the k=1 formulas, which are far larger than the k=60 ones.
        if (indexExists(INDEX_NO_PIPELINE) == false) {
            createIndex(INDEX_NO_PIPELINE, indexConfigWithoutPipeline());
            addFourDocs(INDEX_NO_PIPELINE);
        }

        Map<String, Object> response = search(INDEX_NO_PIPELINE, fusedTwoLegInlineRrfQuery(1), 10);

        assertEquals(3, getHitCount(response));
        List<Map<String, Object>> hits = getNestedHits(response);
        assertEquals("2", hits.get(0).get("_id"));
        assertScoresAreRrfRankSums(hits, 1);
    }

    /**
     * Corpus for the classic-vs-fused parity test. Every document has a distinct (term frequency, length) pair per leg,
     * so neither leg ties two documents on score. That matters because a within-leg tie is the one place the two paths
     * deliberately disagree — classic orders it by Lucene docId, fused by ascending fusion key — and the test asserts
     * tie-freeness up front so a future scoring change surfaces as "the corpus developed a tie" rather than as an
     * unexplained parity failure. Docs 1-4 match both legs, 5 and 8 only leg A, 6 only leg B, 7 neither.
     */
    @SneakyThrows
    private void addParityDocs(String index) {
        addDocument(index, "1", TEXT_FIELD, "hello place", null, null);
        addDocument(index, "2", TEXT_FIELD, "hello hello place", null, null);
        addDocument(index, "3", TEXT_FIELD, "hello place place", null, null);
        addDocument(index, "4", TEXT_FIELD, "hello hello hello place place place place", null, null);
        addDocument(index, "5", TEXT_FIELD, "hello hello hello hello", null, null);
        addDocument(index, "6", TEXT_FIELD, "place place place place place", null, null);
        addDocument(index, "7", TEXT_FIELD, "alpha beta gamma delta", null, null);
        addDocument(index, "8", TEXT_FIELD, "hello alpha beta gamma delta", null, null);
    }

    private static final List<QueryBuilder> PARITY_LEGS = List.of(
        new MatchQueryBuilder(TEXT_FIELD, "hello"),
        new MatchQueryBuilder(TEXT_FIELD, "place")
    );

    private HybridQueryBuilder withParityLegs(HybridQueryBuilder builder) {
        PARITY_LEGS.forEach(builder::add);
        return builder;
    }

    /** (_id -> _score) in the order returned, preserving duplicates in the score sequence. */
    private List<Map.Entry<String, Double>> idsAndScores(Map<String, Object> response) {
        List<Map.Entry<String, Object>> raw = new ArrayList<>();
        for (Map<String, Object> hit : getNestedHits(response)) {
            raw.add(Map.entry((String) hit.get("_id"), hit.get("_score")));
        }
        List<Map.Entry<String, Double>> hits = new ArrayList<>();
        raw.forEach(e -> hits.add(Map.entry(e.getKey(), ((Number) e.getValue()).doubleValue())));
        return hits;
    }

    /** Fail loudly if a leg ties two documents on score, which would make exact per-document parity ill-defined. */
    @SneakyThrows
    private void assertLegsAreTieFree(String index) {
        for (QueryBuilder leg : PARITY_LEGS) {
            List<Double> scores = idsAndScores(search(index, leg, WINDOW)).stream().map(Map.Entry::getValue).toList();
            assertEquals("leg " + leg + " must not tie two documents on score", scores.size(), Set.copyOf(scores).size());
        }
    }

    /**
     * The core regression guard: for the same index and the same two sub-queries, classic shard-side RRF (a
     * {@code score-ranker-processor} pipeline) and coordinator-side fused RRF must return the same documents with the
     * same scores. Asserted three ways per configuration — classic against fused-with-inline-config, and classic
     * against fused reading the very same pipeline.
     *
     * <p>Scores are compared per document at exact equality, and the score <em>sequence</em> is compared separately so
     * the assertion stays agnostic about which document wins a tie on the fused score.
     */
    @SneakyThrows
    private void assertRrfParity(String index, int rankConstant, List<Double> weights) {
        String pipeline = "fused-mode-rrf-parity-k" + rankConstant + (weights.isEmpty() ? "" : "-weighted");
        createRRFSearchPipeline(pipeline, weights, rankConstant, false);

        Map<String, Object> combination = new HashMap<>(Map.of("technique", "rrf", "rank_constant", rankConstant));
        if (weights.isEmpty() == false) {
            combination.put("parameters", Map.of("weights", weights));
        }

        // pagination_depth / window_size are both well above the match count, so the two paths' candidate pools are the
        // same set and parity is well defined. (They can legitimately differ once a truncating window meets more than
        // one shard: classic ranks the union of each shard's top-depth, fused the merged global top-window.)
        HybridQueryBuilder classicQuery = withParityLegs(new HybridQueryBuilder()).paginationDepth(WINDOW);
        HybridQueryBuilder fusedInline = withParityLegs(
            new HybridQueryBuilder().fusion(Map.of("combination", combination, "window_size", WINDOW))
        );
        HybridQueryBuilder fusedFromPipeline = withParityLegs(
            new HybridQueryBuilder().fusion(Map.of("source", "pipeline", "window_size", WINDOW))
        );

        Map<String, String> usePipeline = Map.of("search_pipeline", pipeline);
        List<Map.Entry<String, Double>> classic = idsAndScores(search(index, classicQuery, null, WINDOW, usePipeline, null));
        String what = "rank_constant=" + rankConstant + ", weights=" + weights;
        for (Map.Entry<String, List<Map.Entry<String, Double>>> fused : Map.of(
            "fused inline config",
            idsAndScores(search(index, fusedInline, WINDOW)),
            "fused reading the same pipeline",
            idsAndScores(search(index, fusedFromPipeline, null, WINDOW, usePipeline, null))
        ).entrySet()) {
            assertEquals(
                "classic vs " + fused.getKey() + " score per document (" + what + ")",
                classic.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
                fused.getValue().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );
            assertEquals(
                "classic vs " + fused.getKey() + " score sequence (" + what + ")",
                classic.stream().map(Map.Entry::getValue).toList(),
                fused.getValue().stream().map(Map.Entry::getValue).toList()
            );
        }
    }

    @SneakyThrows
    public void testFusedMode_whenRrf_thenScoresMatchClassicScoreRankerPipeline() {
        // Single shard so the candidate pools coincide unconditionally, rather than only because this corpus happens to
        // fit inside the window on every shard.
        if (indexExists(INDEX_RRF_PARITY) == false) {
            createIndex(INDEX_RRF_PARITY, indexConfigWithoutPipeline(1));
            addParityDocs(INDEX_RRF_PARITY);
        }
        assertLegsAreTieFree(INDEX_RRF_PARITY);

        // Both ends of the supported rank_constant range plus the default: the rank scores differ by orders of
        // magnitude across these, so a discrepancy in the shared arithmetic cannot hide in any one of them.
        assertRrfParity(INDEX_RRF_PARITY, RRFScoreNormalizer.MIN_RANK_CONSTANT, List.of());
        assertRrfParity(INDEX_RRF_PARITY, RRFScoreNormalizer.DEFAULT_RANK_CONSTANT, List.of());
        assertRrfParity(INDEX_RRF_PARITY, RRFScoreNormalizer.MAX_RANK_CONSTANT, List.of());
        // Weights are applied by the shared combination technique on both paths, so parity must survive them too.
        assertRrfParity(INDEX_RRF_PARITY, RRFScoreNormalizer.DEFAULT_RANK_CONSTANT, List.of(0.3, 0.7));
    }

    @SneakyThrows
    public void testFusedMode_whenInlineRrfWithNormalizationTechnique_thenRejected() {
        // rrf is rank based; pairing it with a normalization technique is contradictory and must be rejected rather than
        // silently dropping the normalization clause.
        if (indexExists(INDEX_NO_PIPELINE) == false) {
            createIndex(INDEX_NO_PIPELINE, indexConfigWithoutPipeline());
            addFourDocs(INDEX_NO_PIPELINE);
        }
        HybridQueryBuilder contradictory = new HybridQueryBuilder().fusion(
            Map.of("normalization", Map.of("technique", "min_max"), "combination", Map.of("technique", "rrf"))
        );
        contradictory.add(new MatchQueryBuilder(TEXT_FIELD, "hello"));
        contradictory.add(new TermQueryBuilder(TEXT_FIELD, "place"));

        ResponseException e = expectThrows(ResponseException.class, () -> search(INDEX_NO_PIPELINE, contradictory, 10));
        // Rejected by the classic compatibility matrix, not by a fused-mode-specific rule: classic maps min_max to the three
        // means, never to rrf. Only rrf + rrf short circuits ahead of that matrix.
        assertTrue(e.getMessage(), e.getMessage().contains("does not support combination [rrf] with normalization [min_max]"));
    }
}
