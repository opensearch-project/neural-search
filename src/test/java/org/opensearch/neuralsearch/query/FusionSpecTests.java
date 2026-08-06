/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.List;
import java.util.Map;

import org.opensearch.test.OpenSearchTestCase;

public class FusionSpecTests extends OpenSearchTestCase {

    public void testFromInlineFusion_whenNormalizationCombination_thenParsed() {
        Map<String, Object> inline = Map.of(
            "normalization",
            Map.of("technique", "l2"),
            "combination",
            Map.of("technique", "arithmetic_mean", "parameters", Map.of("weights", List.of(0.3, 0.7)))
        );
        FusionSpec spec = FusionSpec.fromInlineFusion(inline);
        assertNotNull(spec);
        assertEquals("l2", spec.normalizationTechnique());
        assertEquals(FusionSpec.TECHNIQUE_ARITHMETIC_MEAN, spec.combinationTechnique());
        assertArrayEquals(new float[] { 0.3f, 0.7f }, spec.weights(), 0.0001f);
    }

    public void testFromInlineFusion_whenRrf_thenRankConstantAndNoNormalization() {
        Map<String, Object> inline = Map.of("combination", Map.of("technique", "rrf", "parameters", Map.of("rank_constant", 42)));
        FusionSpec spec = FusionSpec.fromInlineFusion(inline);
        assertNotNull(spec);
        assertEquals(FusionSpec.TECHNIQUE_RRF, spec.combinationTechnique());
        assertEquals(FusionSpec.NORMALIZATION_NONE, spec.normalizationTechnique());
        assertEquals(42, spec.rankConstant());
    }

    public void testFromInlineFusion_whenDefaults_thenMinMaxArithmeticMean() {
        // An empty inline block resolves to the min_max + arithmetic_mean defaults.
        FusionSpec spec = FusionSpec.fromInlineFusion(Map.of());
        assertNotNull(spec);
        assertEquals(FusionSpec.NORMALIZATION_MIN_MAX, spec.normalizationTechnique());
        assertEquals(FusionSpec.TECHNIQUE_ARITHMETIC_MEAN, spec.combinationTechnique());
        assertEquals(0, spec.weights().length);
        assertEquals(FusionSpec.DEFAULT_RANK_CONSTANT, spec.rankConstant());
    }

    public void testFromInlineFusion_whenNull_thenNull() {
        assertNull(FusionSpec.fromInlineFusion(null));
    }

    public void testFromPipelineConfig_whenNormalizationProcessor_thenParsed() {
        Map<String, Object> pipelineConfig = Map.of(
            "phase_results_processors",
            List.of(
                Map.of(
                    "normalization-processor",
                    Map.of("normalization", Map.of("technique", "z_score"), "combination", Map.of("technique", "arithmetic_mean"))
                )
            )
        );
        FusionSpec spec = FusionSpec.fromPipelineConfig(pipelineConfig);
        assertNotNull(spec);
        assertEquals("z_score", spec.normalizationTechnique());
        assertEquals(FusionSpec.TECHNIQUE_ARITHMETIC_MEAN, spec.combinationTechnique());
    }

    public void testFromPipelineConfig_whenScoreRankerProcessor_thenRrf() {
        Map<String, Object> pipelineConfig = Map.of(
            "phase_results_processors",
            List.of(
                Map.of(
                    "score-ranker-processor",
                    Map.of("combination", Map.of("technique", "rrf", "parameters", Map.of("rank_constant", 10)))
                )
            )
        );
        FusionSpec spec = FusionSpec.fromPipelineConfig(pipelineConfig);
        assertNotNull(spec);
        assertEquals(FusionSpec.TECHNIQUE_RRF, spec.combinationTechnique());
        assertEquals(10, spec.rankConstant());
    }

    public void testFromPipelineConfig_whenNoFusionProcessor_thenNull() {
        // A pipeline with no normalization/score-ranker processor yields null → caller fails fast.
        assertNull(FusionSpec.fromPipelineConfig(Map.of("phase_results_processors", List.of())));
        assertNull(FusionSpec.fromPipelineConfig(Map.of()));
        assertNull(FusionSpec.fromPipelineConfig(null));
    }
}
