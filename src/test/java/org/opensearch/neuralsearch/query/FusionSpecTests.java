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
        Map<String, Object> inline = Map.of("combination", Map.of("technique", "rrf", "rank_constant", 42));
        FusionSpec spec = FusionSpec.fromInlineFusion(inline);
        assertNotNull(spec);
        assertEquals(FusionSpec.TECHNIQUE_RRF, spec.combinationTechnique());
        assertEquals(FusionSpec.NORMALIZATION_NONE, spec.normalizationTechnique());
        assertEquals(42, spec.rankConstant());
    }

    public void testFromInlineFusion_whenRrfWithNormalizationClause_thenNormalizationReported() {
        // RRF takes no normalization technique, but an inline block can still carry one. It is reported rather than
        // dropped so the caller's technique check can reject the contradictory pairing.
        Map<String, Object> inline = Map.of("normalization", Map.of("technique", "min_max"), "combination", Map.of("technique", "rrf"));
        FusionSpec spec = FusionSpec.fromInlineFusion(inline);
        assertNotNull(spec);
        assertEquals(FusionSpec.TECHNIQUE_RRF, spec.combinationTechnique());
        assertEquals(FusionSpec.NORMALIZATION_MIN_MAX, spec.normalizationTechnique());
    }

    public void testFromInlineFusion_whenRrfRankConstantInvalid_thenRejected() {
        // Resolved through the shared validator, so fused mode rejects exactly what the score-ranker-processor rejects.
        assertThrows(
            IllegalArgumentException.class,
            () -> FusionSpec.fromInlineFusion(Map.of("combination", Map.of("technique", "rrf", "rank_constant", 0)))
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> FusionSpec.fromInlineFusion(Map.of("combination", Map.of("technique", "rrf", "rank_constant", 10001)))
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> FusionSpec.fromInlineFusion(Map.of("combination", Map.of("technique", "rrf", "rank_constant", "not-a-number")))
        );
    }

    public void testFromInlineFusion_whenRankConstantUnderParameters_thenRejected() {
        // The score-ranker-processor reads rank_constant off the combination clause and rejects it under `parameters`
        // ("supported parameters are [weights]"). Fused mode must reject it the same way rather than silently defaulting
        // to 60 and mis-ranking every query.
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> FusionSpec.fromInlineFusion(Map.of("combination", Map.of("technique", "rrf", "parameters", Map.of("rank_constant", 42))))
        );
        assertTrue(e.getMessage().contains("must be set on the [combination] clause"));
    }

    public void testFromInlineFusion_whenRrfWithoutRankConstant_thenDefault() {
        FusionSpec spec = FusionSpec.fromInlineFusion(Map.of("combination", Map.of("technique", "rrf")));
        assertNotNull(spec);
        assertEquals(FusionSpec.TECHNIQUE_RRF, spec.combinationTechnique());
        assertEquals(FusionSpec.DEFAULT_RANK_CONSTANT, spec.rankConstant());
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
            List.of(Map.of("score-ranker-processor", Map.of("combination", Map.of("technique", "rrf", "rank_constant", 10))))
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
