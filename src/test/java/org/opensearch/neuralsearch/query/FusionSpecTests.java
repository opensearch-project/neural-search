/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.ArrayList;
import java.util.HashMap;
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

    public void testFromInlineFusion_whenRrf_thenRankConstantAndRrfNormalization() {
        // An rrf block carries no normalization clause, and the technique it defaults to is rrf rather than none: rank
        // scoring IS the normalization step, which is how the coordinator resolves it through the same lookup as min_max.
        Map<String, Object> inline = Map.of("combination", Map.of("technique", "rrf", "rank_constant", 42));
        FusionSpec spec = FusionSpec.fromInlineFusion(inline);
        assertNotNull(spec);
        assertEquals(FusionSpec.TECHNIQUE_RRF, spec.combinationTechnique());
        assertEquals(FusionSpec.NORMALIZATION_RRF, spec.normalizationTechnique());
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

    public void testFromPipelineConfig_whenNormalizationProcessorWithRrfNormalization_thenRankConstantHonored() {
        // rrf is also a normalization-processor normalization technique, and there the rank constant lives under
        // `normalization.parameters` — that is where NormalizationProcessorFactory hands params to RRFNormalizationTechnique.
        // Reading it only off the combination clause would silently rank this config at the default 60.
        Map<String, Object> pipelineConfig = Map.of(
            "phase_results_processors",
            List.of(
                Map.of(
                    "normalization-processor",
                    Map.of(
                        "normalization",
                        Map.of("technique", "rrf", "parameters", Map.of("rank_constant", 100)),
                        "combination",
                        Map.of("technique", "arithmetic_mean")
                    )
                )
            )
        );
        FusionSpec spec = FusionSpec.fromPipelineConfig(pipelineConfig);
        assertNotNull(spec);
        assertEquals(FusionSpec.NORMALIZATION_RRF, spec.normalizationTechnique());
        assertEquals(FusionSpec.TECHNIQUE_ARITHMETIC_MEAN, spec.combinationTechnique());
        assertEquals(100, spec.rankConstant());
    }

    public void testFromPipelineConfig_whenNormalizationProcessorRankConstantInvalid_thenRejected() {
        // Same shared validator as the score-ranker shape, so an out-of-range value is a 400 here too rather than a
        // silent fallback to 60.
        assertThrows(IllegalArgumentException.class, () -> FusionSpec.fromPipelineConfig(normalizationProcessorWithRankConstant(0)));
        assertThrows(IllegalArgumentException.class, () -> FusionSpec.fromPipelineConfig(normalizationProcessorWithRankConstant(10001)));
    }

    public void testShape_distinguishesRrfPairingsThatResolveIdentically() {
        // Both shapes can resolve to normalization=rrf + combination=rrf, and only the score-ranker one is a pairing
        // classic allows — a normalization-processor asked to combine rrf-normalized scores by rrf is rejected by classic's
        // compatibility matrix. The technique names cannot tell them apart, so the shape is what the fused-mode gate keys
        // its exemption from that matrix on.
        Map<String, Object> asNormalizationProcessor = Map.of(
            "phase_results_processors",
            List.of(
                Map.of(
                    "normalization-processor",
                    Map.of("normalization", Map.of("technique", "rrf"), "combination", Map.of("technique", "rrf"))
                )
            )
        );
        FusionSpec normalizationShaped = FusionSpec.fromPipelineConfig(asNormalizationProcessor);
        assertNotNull(normalizationShaped);
        assertEquals(FusionSpec.Shape.NORMALIZATION_PROCESSOR, normalizationShaped.shape());
        assertEquals(FusionSpec.NORMALIZATION_RRF, normalizationShaped.normalizationTechnique());
        assertEquals(FusionSpec.TECHNIQUE_RRF, normalizationShaped.combinationTechnique());

        Map<String, Object> asScoreRanker = Map.of(
            "phase_results_processors",
            List.of(Map.of("score-ranker-processor", Map.of("combination", Map.of("technique", "rrf"))))
        );
        FusionSpec rankFusionShaped = FusionSpec.fromPipelineConfig(asScoreRanker);
        assertNotNull(rankFusionShaped);
        assertEquals(FusionSpec.Shape.SCORE_RANKER_PROCESSOR, rankFusionShaped.shape());
        // Same two technique names as above, which is the whole point.
        assertEquals(normalizationShaped.normalizationTechnique(), rankFusionShaped.normalizationTechnique());
        assertEquals(normalizationShaped.combinationTechnique(), rankFusionShaped.combinationTechnique());
    }

    public void testFromInlineFusion_whenRrf_thenScoreRankerShape() {
        // An inline block naming rrf as the combination is the score-ranker shape, wherever it came from.
        assertEquals(
            FusionSpec.Shape.SCORE_RANKER_PROCESSOR,
            FusionSpec.fromInlineFusion(Map.of("combination", Map.of("technique", "rrf"))).shape()
        );
        assertEquals(FusionSpec.Shape.NORMALIZATION_PROCESSOR, FusionSpec.fromInlineFusion(Map.of()).shape());
    }

    public void testFromInlineFusion_whenMinMaxBounds_thenRejected() {
        // lower_bounds/upper_bounds are the documented way to bound min_max in classic, and fused mode has no
        // coordinator-side implementation of them. Refused, because accepting them and normalizing without them answers
        // the request with a different ranking at HTTP 200 — silent divergence on the zero-migration path.
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> FusionSpec.fromInlineFusion(
                Map.of(
                    "normalization",
                    Map.of("technique", "min_max", "parameters", Map.of("lower_bounds", List.of(Map.of("mode", "apply", "min_score", 0.1))))
                )
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[lower_bounds] not supported with normalization [min_max] in fused mode"));
    }

    public void testFromPipelineConfig_whenMinMaxBounds_thenRejected() {
        // Same refusal through the other entry point: a classic normalization-processor pipeline read at rewrite. This is
        // the migration path a user takes without editing their query at all, so it must not fuse on unbounded scores.
        Map<String, Object> pipelineConfig = Map.of(
            "phase_results_processors",
            List.of(
                Map.of(
                    "normalization-processor",
                    Map.of(
                        "normalization",
                        Map.of(
                            "technique",
                            "min_max",
                            "parameters",
                            Map.of(
                                "lower_bounds",
                                List.of(Map.of("mode", "apply", "min_score", 0.1)),
                                "upper_bounds",
                                List.of(Map.of("mode", "apply", "max_score", 0.9))
                            )
                        ),
                        "combination",
                        Map.of("technique", "arithmetic_mean")
                    )
                )
            )
        );
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> FusionSpec.fromPipelineConfig(pipelineConfig));
        // Both offenders named, in a stable order, and the honored set reported as empty for a score-based technique.
        assertTrue(e.getMessage(), e.getMessage().contains("[normalization.parameters] [lower_bounds, upper_bounds] not supported"));
        assertTrue(e.getMessage(), e.getMessage().contains("supported parameters are []"));
    }

    public void testFromInlineFusion_whenScoreNormalizationHasAnyParameter_thenRejected() {
        // l2 and z_score take no parameters at all in classic either, so anything under the clause is a config error
        // rather than something fused mode is behind on.
        for (String technique : List.of("l2", "z_score")) {
            IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> FusionSpec.fromInlineFusion(Map.of("normalization", Map.of("technique", technique, "parameters", Map.of("bogus", 1))))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("[bogus] not supported with normalization [" + technique + "]"));
        }
    }

    public void testFromInlineFusion_whenRrfNormalizationUnknownParameter_thenRejected() {
        // rrf-as-normalization honors rank_constant and nothing else — the unknown key is named, the honored one is not.
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> FusionSpec.fromInlineFusion(
                Map.of(
                    "normalization",
                    Map.of("technique", "rrf", "parameters", Map.of("rank_constant", 100, "bogus", 1)),
                    "combination",
                    Map.of("technique", "arithmetic_mean")
                )
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[bogus] not supported"));
        assertTrue(e.getMessage(), e.getMessage().contains("supported parameters are [rank_constant]"));
    }

    public void testFromInlineFusion_whenRrfNormalizationWithArithmeticMean_thenRankConstantHonored() {
        // The honored case, inline: rrf as the normalization step with a mean doing the combining. Guards the refusal
        // above from over-reaching, and is the only unit coverage of this pairing through the inline entry point.
        FusionSpec spec = FusionSpec.fromInlineFusion(
            Map.of(
                "normalization",
                Map.of("technique", "rrf", "parameters", Map.of("rank_constant", 100)),
                "combination",
                Map.of("technique", "arithmetic_mean")
            )
        );
        assertNotNull(spec);
        assertEquals(FusionSpec.Shape.NORMALIZATION_PROCESSOR, spec.shape());
        assertEquals(FusionSpec.NORMALIZATION_RRF, spec.normalizationTechnique());
        assertEquals(FusionSpec.TECHNIQUE_ARITHMETIC_MEAN, spec.combinationTechnique());
        assertEquals(100, spec.rankConstant());
    }

    public void testFromInlineFusion_whenScoreRankerRankConstantUnderNormalizationParameters_thenHonored() {
        // The other shape's spelling of the same rrf parameter. Honored rather than refused: it names one rank constant
        // for one rrf normalization, so there is nothing ambiguous to resolve, and 100 must reach the spec — resolving to
        // the default 60 here is exactly the silent mis-ranking the refusal was written to avoid.
        FusionSpec spec = FusionSpec.fromInlineFusion(
            Map.of(
                "normalization",
                Map.of("technique", "rrf", "parameters", Map.of("rank_constant", 100)),
                "combination",
                Map.of("technique", "rrf")
            )
        );
        assertEquals(FusionSpec.Shape.SCORE_RANKER_PROCESSOR, spec.shape());
        assertEquals(FusionSpec.NORMALIZATION_RRF, spec.normalizationTechnique());
        assertEquals(FusionSpec.TECHNIQUE_RRF, spec.combinationTechnique());
        assertEquals(100, spec.rankConstant());
    }

    public void testFromInlineFusion_whenScoreRankerRankConstantInBothPlacesAndAgree_thenHonored() {
        FusionSpec spec = FusionSpec.fromInlineFusion(
            Map.of(
                "normalization",
                Map.of("technique", "rrf", "parameters", Map.of("rank_constant", 100)),
                "combination",
                Map.of("technique", "rrf", "rank_constant", 100)
            )
        );
        assertEquals(100, spec.rankConstant());
    }

    public void testFromInlineFusion_whenScoreRankerRankConstantInBothPlacesAndConflict_thenRejected() {
        // A contradiction, not a precedence question: picking one silently would rank by a constant the user did not ask
        // for, whichever side the rule favored.
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> FusionSpec.fromInlineFusion(
                Map.of(
                    "normalization",
                    Map.of("technique", "rrf", "parameters", Map.of("rank_constant", 100)),
                    "combination",
                    Map.of("technique", "rrf", "rank_constant", 10)
                )
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[rank_constant] is set to [10] on the [combination] clause"));
        assertTrue(e.getMessage(), e.getMessage().contains("to [100] under [normalization.parameters]"));
        assertTrue(e.getMessage(), e.getMessage().contains("set it in one place only"));
    }

    public void testFromInlineFusion_whenScoreRankerRankConstantUnderNormalizationParametersInvalid_thenRejected() {
        // Range and type checks have to hold at the alternate spelling too, or it becomes the lenient way in.
        for (Object invalid : List.of(0, 10001, "not-a-number")) {
            assertThrows(
                IllegalArgumentException.class,
                () -> FusionSpec.fromInlineFusion(
                    Map.of(
                        "normalization",
                        Map.of("technique", "rrf", "parameters", Map.of("rank_constant", invalid)),
                        "combination",
                        Map.of("technique", "rrf")
                    )
                )
            );
        }
    }

    public void testFromInlineFusion_whenWeights_thenParsedForEveryTechnique() {
        // weights live on the combination clause, which both shapes read the same way, so the normalization technique is
        // irrelevant to whether they parse. Pinned across all four so a future per-technique parameter check cannot start
        // dropping them for one of them.
        for (String normalization : List.of("min_max", "z_score", "l2", "rrf")) {
            FusionSpec spec = FusionSpec.fromInlineFusion(
                Map.of(
                    "normalization",
                    Map.of("technique", normalization),
                    "combination",
                    Map.of("technique", "arithmetic_mean", "parameters", Map.of("weights", List.of(0.4, 0.6)))
                )
            );
            assertEquals(normalization, FusionSpec.Shape.NORMALIZATION_PROCESSOR, spec.shape());
            assertArrayEquals(normalization, new float[] { 0.4f, 0.6f }, spec.weights(), 0.0001f);
        }
        // ...and on the score-ranker shape, alongside a rank_constant in either place.
        FusionSpec scoreRanker = FusionSpec.fromInlineFusion(
            Map.of("combination", Map.of("technique", "rrf", "rank_constant", 10, "parameters", Map.of("weights", List.of(0.4, 0.6))))
        );
        assertEquals(FusionSpec.Shape.SCORE_RANKER_PROCESSOR, scoreRanker.shape());
        assertEquals(10, scoreRanker.rankConstant());
        assertArrayEquals(new float[] { 0.4f, 0.6f }, scoreRanker.weights(), 0.0001f);
    }

    public void testFromInlineFusion_whenWeightsIsNotAList_thenRejected() {
        // Classic answers this with a 400 from ScoreCombinationUtil.validateParams. Fused mode never let that check see the
        // value — it rebuilds the list from the parsed array — so a scalar or string weights fused unweighted at HTTP 200.
        for (Object malformed : List.of(0.5, "0.3,0.7", Map.of("0", 0.3))) {
            IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> FusionSpec.fromInlineFusion(
                    Map.of(
                        "normalization",
                        Map.of("technique", "min_max"),
                        "combination",
                        Map.of("technique", "arithmetic_mean", "parameters", Map.of("weights", malformed))
                    )
                )
            );
            assertTrue(e.getMessage(), e.getMessage().contains("parameter [weights] must be a collection of numbers"));
        }
        // Absent is still absent, not malformed.
        assertEquals(
            0,
            FusionSpec.fromInlineFusion(
                Map.of("normalization", Map.of("technique", "min_max"), "combination", Map.of("technique", "arithmetic_mean"))
            ).weights().length
        );
    }

    public void testFromInlineFusion_whenWeightsHasNonNumericElement_thenRejected() {
        // The list check one level up cannot see this, and the element read is an unchecked cast — so before this guard a
        // non-numeric element was a ClassCastException and a null one a NullPointerException, both HTTP 500. Stricter than
        // classic, which casts the elements just as unguardedly, rather than parity with it.
        List<Object> withNull = new ArrayList<>();
        withNull.add(0.5);
        withNull.add(null);
        for (Object malformed : List.<Object>of(List.of("a"), List.of("a", 0.5), List.of(0.5, "b"), List.of(0.5, Map.of()), withNull)) {
            // Both shapes read weights through the same method, so both are pinned: rrf routes to the score-ranker shape.
            for (String combination : List.of("arithmetic_mean", "rrf")) {
                IllegalArgumentException e = assertThrows(
                    combination + " " + malformed,
                    IllegalArgumentException.class,
                    () -> FusionSpec.fromInlineFusion(
                        Map.of("combination", Map.of("technique", combination, "parameters", Map.of("weights", malformed)))
                    )
                );
                assertTrue(e.getMessage(), e.getMessage().contains("parameter [weights] must be a collection of numbers"));
            }
        }
        // An integer is a Number too — the guard must not reject a weights list a user wrote without decimal points.
        assertArrayEquals(
            new float[] { 1.0f, 0.0f },
            FusionSpec.fromInlineFusion(
                Map.of("combination", Map.of("technique", "arithmetic_mean", "parameters", Map.of("weights", List.of(1, 0))))
            ).weights(),
            0.0001f
        );
    }

    public void testFromInlineFusion_whenClauseIsNotAnObject_thenRejected() {
        // `"combination": "rrf"` copies the shape of the supported one-level-up shorthand `"fusion": "pipeline"`. Every
        // reader gates on instanceof Map, so without this it resolved to min_max + arithmetic_mean at HTTP 200.
        IllegalArgumentException combination = assertThrows(
            IllegalArgumentException.class,
            () -> FusionSpec.fromInlineFusion(Map.of("combination", "rrf"))
        );
        assertTrue(combination.getMessage(), combination.getMessage().contains("[combination] must be an object"));

        IllegalArgumentException normalization = assertThrows(
            IllegalArgumentException.class,
            () -> FusionSpec.fromInlineFusion(Map.of("normalization", "min_max"))
        );
        assertTrue(normalization.getMessage(), normalization.getMessage().contains("[normalization] must be an object"));
    }

    public void testFromInlineFusion_whenParametersIsNotAnObject_thenRejected() {
        // The same guard one level down, and the mistake is the container rather than the parameter:
        // `"parameters": [{"lower_bounds": [...]}]` is one stray bracket from the honest form. Every reader gated on
        // instanceof Map and fell back to an empty one, so rejectUnhonoredNormalizationParameters was handed no keys to
        // refuse and the request fused with min_max's defaults at HTTP 200 — bounds accepted and silently ignored, which is
        // the exact outcome that guard exists to prevent.
        for (Object malformed : List.<Object>of(List.of(Map.of("lower_bounds", List.of())), "lower_bounds", 0.5)) {
            IllegalArgumentException e = assertThrows(
                String.valueOf(malformed),
                IllegalArgumentException.class,
                () -> FusionSpec.fromInlineFusion(Map.of("normalization", Map.of("technique", "min_max", "parameters", malformed)))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("[normalization.parameters] must be an object"));
        }
        // And on the other clause, reached through a different reader on each shape: readWeights for the
        // normalization-processor one, rejectRankConstantUnderParameters for the score-ranker one that rrf routes to.
        for (String combination : List.of("arithmetic_mean", "rrf")) {
            IllegalArgumentException e = assertThrows(
                combination,
                IllegalArgumentException.class,
                () -> FusionSpec.fromInlineFusion(
                    Map.of("combination", Map.of("technique", combination, "parameters", List.of(Map.of("weights", List.of(0.3, 0.7)))))
                )
            );
            assertTrue(e.getMessage(), e.getMessage().contains("[combination.parameters] must be an object"));
        }
    }

    public void testFromInlineFusion_whenRrfParametersIsNotAnObject_thenTypeErrorNotRankConstantError() {
        // rrf reads rank_constant out of this very map, so which of the two guards runs first decides what the user is told.
        // The type error is the actionable one: nothing true can be said about a rank constant in a map that is not a map.
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> FusionSpec.fromInlineFusion(Map.of("normalization", Map.of("technique", "rrf", "parameters", List.of(60))))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[normalization.parameters] must be an object"));
        assertFalse(e.getMessage(), e.getMessage().contains("rank constant must be in the interval"));
    }

    public void testFromInlineFusion_whenParametersIsExplicitlyNull_thenTreatedAsAbsent() {
        // An explicit null asks for nothing, which is servable — the same tolerance requireObjectClause gives a null clause.
        // Written through HashMap because Map.of rejects null values.
        Map<String, Object> normalization = new HashMap<>();
        normalization.put("technique", "min_max");
        normalization.put("parameters", null);
        Map<String, Object> combination = new HashMap<>();
        combination.put("technique", "arithmetic_mean");
        combination.put("parameters", null);

        FusionSpec spec = FusionSpec.fromInlineFusion(Map.of("normalization", normalization, "combination", combination));
        assertEquals("min_max", spec.normalizationTechnique());
        assertEquals(FusionSpec.TECHNIQUE_ARITHMETIC_MEAN, spec.combinationTechnique());
        assertEquals(0, spec.weights().length);
    }

    public void testFromPipelineConfig_whenParametersIsNotAnObject_thenRejected() {
        // A stored pipeline cannot carry this — classic's factory reads the clause through core's readOptionalMap and
        // refuses it at pipeline creation — but the same processor shape is also how an inline fusion block is spelled, and
        // that one is only ever parsed here. Pinned on this entry point too so the guard cannot be lost to a refactor that
        // only keeps fromInlineFusion covered.
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> FusionSpec.fromPipelineConfig(
                Map.of(
                    "phase_results_processors",
                    List.of(
                        Map.of("normalization-processor", Map.of("normalization", Map.of("technique", "rrf", "parameters", List.of(60))))
                    )
                )
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[normalization.parameters] must be an object"));
    }

    private static Map<String, Object> normalizationProcessorWithRankConstant(int rankConstant) {
        return Map.of(
            "phase_results_processors",
            List.of(
                Map.of(
                    "normalization-processor",
                    Map.of("normalization", Map.of("technique", "rrf", "parameters", Map.of("rank_constant", rankConstant)))
                )
            )
        );
    }

    public void testFromPipelineConfig_whenNoFusionProcessor_thenNull() {
        // A pipeline with no normalization/score-ranker processor yields null → caller fails fast.
        assertNull(FusionSpec.fromPipelineConfig(Map.of("phase_results_processors", List.of())));
        assertNull(FusionSpec.fromPipelineConfig(Map.of()));
        assertNull(FusionSpec.fromPipelineConfig(null));
    }
}
