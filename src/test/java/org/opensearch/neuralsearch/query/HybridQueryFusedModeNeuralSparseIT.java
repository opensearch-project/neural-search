/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.opensearch.neuralsearch.util.TestUtils.createRandomTokenWeightMap;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import lombok.SneakyThrows;

/**
 * A fused hybrid query with a {@code neural_sparse} leg: the leg is resolved once (one model call per request, counted through
 * the ML plugin's own predict counter for the model), it fuses correctly beside a lexical leg, and the
 * {@code neural_sparse_two_phase_processor} leaves a hybrid request unchanged, so attaching its pipeline changes nothing.
 *
 * <p>Two windows are covered. With the default window the sparse leg returns its whole match set; with a window of two it fills
 * the window. Either way the Tail carries it as the query it resolved to, since a leg written as {@code neural_sparse} is not
 * materialized, and the resolved leg carries its tokens, so round 2's rewrite has nothing left to infer.
 */
public class HybridQueryFusedModeNeuralSparseIT extends BaseNeuralSearchIT {

    private static final String INDEX = "test-fused-neural-sparse";
    private static final String SPARSE_FIELD = "sparse_embedding";
    private static final String TEXT_FIELD = "text";
    private static final String QUERY_TEXT = "Hello world a b";
    private static final List<String> TOKENS = List.of("hello", "world", "a", "b", "c");
    private static final String TWO_PHASE_PIPELINE = "fused-neural-sparse-two-phase";
    /** Documents 1..3 carry this word, so the lexical leg matches exactly them; every document matches the sparse leg. */
    private static final String LEXICAL_WORD = "alpha";
    private static final Set<String> LEXICAL_IDS = Set.of("1", "2", "3");
    private static final int DOCS = 6;

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public void testFusedNeuralSparseLeg_whenShortOrFillingTheWindow_thenOneInferenceAndCorrectFusion() {
        String modelId = prepareSparseEncodingModel();
        try {
            prepareDataset();
            // A plain sparse search first, so that model loading and first-call effects are attributed to no arm.
            search(plainSparse(modelId), null);

            // Arm 1: default window -- the sparse leg returns all six documents, fewer than the window, and the Tail carries
            // it as the resolved query.
            Map<String, Object>[] shortLeg = new Map[1];
            int inferences = predictsDuring(modelId, () -> shortLeg[0] = search(fused(modelId, 10, 100), null));
            assertEquals("short sparse leg carried by the Tail: one inference", 1, inferences);
            assertFusedUnion(shortLeg[0], DOCS);

            // Arm 2: a window of two -- the sparse leg fills it and is kept in the Tail as a query, the shape that re-inferred.
            Map<String, Object>[] carried = new Map[1];
            inferences = predictsDuring(modelId, () -> carried[0] = search(fused(modelId, 2, 2), null));
            assertEquals("window-filling sparse leg carried by the Tail: still one inference", 1, inferences);
            assertEquals("the page is the window", 2, hits(carried[0]).size());
            assertEquals("the Tail counted the whole union", DOCS, totalHits(carried[0]));
            assertTrue("fused scores are real", maxScore(carried[0]) > 0.0f);

            // Arm 3: the two-phase processor only rewrites top-level or bool-nested neural_sparse queries; a hybrid is left as
            // it is, so the same request through its pipeline returns the same documents, and still infers once.
            createNeuralSparseTwoPhaseSearchProcessor(TWO_PHASE_PIPELINE);
            Map<String, Object>[] twoPhase = new Map[1];
            inferences = predictsDuring(modelId, () -> twoPhase[0] = search(fused(modelId, 10, 100), TWO_PHASE_PIPELINE));
            assertEquals("through the two-phase pipeline: one inference", 1, inferences);
            assertFusedUnion(twoPhase[0], DOCS);
            assertEquals("same documents in the same order as without the pipeline", ids(shortLeg[0]), ids(twoPhase[0]));

            // Control: a plain neural_sparse query infers once.
            inferences = predictsDuring(modelId, () -> search(plainSparse(modelId), null));
            assertEquals("a plain neural_sparse query infers once", 1, inferences);
        } finally {
            wipeOfTestResources(INDEX, null, modelId, TWO_PHASE_PIPELINE);
        }
    }

    // ------------------------------------------------------------------------------------------------------------------

    @SneakyThrows
    private void prepareDataset() {
        prepareSparseEncodingIndex(INDEX, List.of(SPARSE_FIELD), 1);
        for (int id = 1; id <= DOCS; id++) {
            addSparseEncodingDoc(
                INDEX,
                String.valueOf(id),
                List.of(SPARSE_FIELD),
                List.of(createRandomTokenWeightMap(TOKENS)),
                List.of(TEXT_FIELD),
                List.of(LEXICAL_IDS.contains(String.valueOf(id)) ? LEXICAL_WORD : "beta")
            );
        }
        assertEquals(DOCS, getDocCount(INDEX));
    }

    /**
     * Every document is in the union (the sparse leg matches all of them), the documents the lexical leg also matched fuse
     * above the rest -- min_max puts a leg's single score, or its maximum, at 1.0 and a missing leg contributes 0 to the mean --
     * and every score is real.
     */
    @SuppressWarnings("unchecked")
    private void assertFusedUnion(final Map<String, Object> response, final int expectedTotal) {
        List<Map<String, Object>> hits = hits(response);
        assertEquals("the page holds the whole union", expectedTotal, hits.size());
        assertEquals(expectedTotal, totalHits(response));
        assertTrue(maxScore(response) > 0.0f);
        float lowestLexical = Float.MAX_VALUE;
        float highestSparseOnly = 0.0f;
        for (Map<String, Object> hit : hits) {
            float score = ((Number) hit.get("_score")).floatValue();
            assertTrue("every fused score is real: " + hit, score > 0.0f);
            if (LEXICAL_IDS.contains(hit.get("_id"))) {
                lowestLexical = Math.min(lowestLexical, score);
            } else {
                highestSparseOnly = Math.max(highestSparseOnly, score);
            }
        }
        assertTrue(
            "documents matched by both legs fuse at or above those matched by the sparse leg alone: " + ids(response),
            lowestLexical >= highestSparseOnly
        );
    }

    private String fused(final String modelId, final int size, final int windowSize) {
        return String.format(
            Locale.ROOT,
            "{\"size\":%d,\"query\":{\"hybrid\":{\"fusion\":{\"window_size\":%d,\"normalization\":{\"technique\":\"min_max\"},"
                + "\"combination\":{\"technique\":\"arithmetic_mean\"}},\"queries\":[{\"match\":{\"%s\":\"%s\"}},%s]}}}",
            size,
            windowSize,
            TEXT_FIELD,
            LEXICAL_WORD,
            sparseLeg(modelId)
        );
    }

    private String plainSparse(final String modelId) {
        return "{\"size\":10,\"query\":" + sparseLeg(modelId) + "}";
    }

    private String sparseLeg(final String modelId) {
        return String.format(
            Locale.ROOT,
            "{\"neural_sparse\":{\"%s\":{\"query_text\":\"%s\",\"model_id\":\"%s\"}}}",
            SPARSE_FIELD,
            QUERY_TEXT,
            modelId
        );
    }

    @SneakyThrows
    private Map<String, Object> search(final String body, final String searchPipeline) {
        Request request = new Request("POST", "/" + INDEX + "/_search");
        if (searchPipeline != null) {
            request.addParameter("search_pipeline", searchPipeline);
        }
        request.setJsonEntity(body);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    /**
     * How many predict calls the ML plugin recorded for {@code modelId} while {@code action} ran -- model-agnostic, and read
     * right after the search returns: a local model's predict count is incremented before the inference listener is called,
     * so a response in hand means every call the request made is counted.
     */
    private int predictsDuring(final String modelId, final Runnable action) {
        int before = predictCount(modelId);
        action.run();
        return predictCount(modelId) - before;
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    private int predictCount(final String modelId) {
        Response response = client().performRequest(new Request("GET", "/_plugins/_ml/stats"));
        Map<String, Object> stats = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(response.getEntity()),
            false
        );
        int count = 0;
        Map<String, Object> nodes = (Map<String, Object>) stats.get("nodes");
        for (Object nodeStats : nodes.values()) {
            Map<String, Object> models = (Map<String, Object>) ((Map<String, Object>) nodeStats).get("models");
            if (models == null || models.get(modelId) == null) {
                continue;
            }
            Map<String, Object> predict = (Map<String, Object>) ((Map<String, Object>) models.get(modelId)).get("predict");
            if (predict != null && predict.get("ml_action_request_count") != null) {
                count += ((Number) predict.get("ml_action_request_count")).intValue();
            }
        }
        return count;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> hits(final Map<String, Object> response) {
        return (List<Map<String, Object>>) ((Map<String, Object>) response.get("hits")).get("hits");
    }

    private List<String> ids(final Map<String, Object> response) {
        return hits(response).stream().map(hit -> (String) hit.get("_id")).collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private int totalHits(final Map<String, Object> response) {
        Map<String, Object> total = (Map<String, Object>) ((Map<String, Object>) response.get("hits")).get("total");
        assertEquals("an exact union", "eq", total.get("relation"));
        return ((Number) total.get("value")).intValue();
    }

    @SuppressWarnings("unchecked")
    private float maxScore(final Map<String, Object> response) {
        Object maxScore = ((Map<String, Object>) response.get("hits")).get("max_score");
        return maxScore == null ? 0.0f : ((Number) maxScore).floatValue();
    }

}
