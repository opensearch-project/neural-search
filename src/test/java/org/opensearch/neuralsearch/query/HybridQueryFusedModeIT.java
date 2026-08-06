/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getNestedHits;

import java.util.List;
import java.util.Map;

import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import lombok.SneakyThrows;

/**
 * End-to-end integration test for the resolver (fused) mode of the {@code hybrid} query — the first working slice
 * (min_max normalization + arithmetic_mean combination, top-level). Exercises the full coordinator flow: parse the
 * {@code fusion} parameter, fan the legs out as a MultiSearch, fuse on the coordinator via the shared fusion core, and
 * self-erase into a standard query that returns fused results.
 *
 * <p>Happy path only for this PR; broader coverage (nested, RRF, aggregations, explain/profiler, min_score, more
 * technique pairs) is scoped to later PRs.
 */
public class HybridQueryFusedModeIT extends BaseNeuralSearchIT {

    private static final String TEXT_FIELD = "text";
    private static final String INDEX_WITH_DEFAULT_NORM = "test-hybrid-fused-default-norm";
    private static final String INDEX_NO_PIPELINE = "test-hybrid-fused-inline-config";
    private static final String NORM_PIPELINE = "fused-mode-norm-pipeline";

    private String indexConfigWithDefaultPipeline(String pipelineId) {
        return "{\"settings\":{\"number_of_shards\":3,\"number_of_replicas\":0,\"index.search.default_pipeline\":\""
            + pipelineId
            + "\"},\"mappings\":{\"properties\":{\""
            + TEXT_FIELD
            + "\":{\"type\":\"text\"}}}}";
    }

    private String indexConfigWithoutPipeline() {
        return "{\"settings\":{\"number_of_shards\":3,\"number_of_replicas\":0},\"mappings\":{\"properties\":{\""
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
}
