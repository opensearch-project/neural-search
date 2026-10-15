/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion.math;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Every combination core must produce, bit for bit, what the classic processor technique of the same name produces for the
 * same weights, and must describe itself with the same words. The classic techniques are the test oracle only; they
 * delegate to the cores in production, so this also pins the delegation.
 */
public class ScoreCombinationCoresTests extends OpenSearchTestCase {

    private static final ScoreCombinationFactory CLASSIC = new ScoreCombinationFactory();

    public void testSupportedTechniques() {
        assertEquals(
            List.of("arithmetic_mean", "geometric_mean", "harmonic_mean", "rrf"),
            List.copyOf(ScoreCombinationCores.supportedTechniques())
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ScoreCombinationCores.forTechnique("median"));
        assertTrue(e.getMessage(), e.getMessage().contains("[median] is not supported"));
        expectThrows(IllegalArgumentException.class, () -> ScoreCombinationCores.forTechnique(null));
    }

    public void testEveryCore_bitExactAgainstClassicTechnique_unweighted() {
        for (String technique : ScoreCombinationCores.supportedTechniques()) {
            ScoreCombinationCore core = ScoreCombinationCores.forTechnique(technique);
            ScoreCombinationTechnique classic = CLASSIC.createCombination(technique);
            assertEquals(technique, core.techniqueName());
            assertEquals(classic.describe(), core.describe());
            assertTrue(core.weights().isEmpty());
            for (int round = 0; round < 200; round++) {
                float[] scores = randomScores(randomIntBetween(1, 5));
                assertEquals(technique, Float.floatToIntBits(classic.combine(scores)), Float.floatToIntBits(core.combine(scores)));
            }
        }
    }

    public void testEveryCore_bitExactAgainstClassicTechnique_weighted() {
        for (String technique : ScoreCombinationCores.supportedTechniques()) {
            int legs = randomIntBetween(2, 5);
            List<Float> weights = randomWeights(legs);
            ScoreCombinationCore core = ScoreCombinationCores.forTechnique(technique, CombinationWeights.of(weights));
            List<Double> asDoubles = new ArrayList<>();
            weights.forEach(weight -> asDoubles.add((double) weight));
            ScoreCombinationTechnique classic = CLASSIC.createCombination(technique, Map.of(FusionTechniqueNames.PARAM_WEIGHTS, asDoubles));
            assertEquals(classic.describe(), core.describe());
            assertEquals(weights, core.weights().asList());
            for (int round = 0; round < 200; round++) {
                float[] scores = randomScores(legs);
                assertEquals(technique, Float.floatToIntBits(classic.combine(scores)), Float.floatToIntBits(core.combine(scores)));
            }
        }
    }

    public void testClassicAdaptersExposeSharedWeights() {
        List<Float> expectedWeights = List.of(0.25f, 0.75f);
        Map<String, Object> parameters = Map.of(FusionTechniqueNames.PARAM_WEIGHTS, List.of(0.25d, 0.75d));
        for (String technique : ScoreCombinationCores.supportedTechniques()) {
            ScoreCombinationTechnique classic = CLASSIC.createCombination(technique, parameters);
            assertEquals(technique, expectedWeights, classic.weights().asList());
        }
    }

    public void testWeightCountMismatch_failsWithClassicMessage() {
        ScoreCombinationCore core = ScoreCombinationCores.forTechnique(
            FusionTechniqueNames.ARITHMETIC_MEAN,
            CombinationWeights.of(List.of(0.3f, 0.7f))
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> core.combine(new float[] { 0.5f, 0.5f, 0.5f }));
        assertEquals("number of weights [2] must match number of sub-queries [3] in hybrid query", e.getMessage());
    }

    public void testKnownValues() {
        float[] scores = new float[] { 0.5f, 0.0f, 1.0f };
        ScoreCombinationCore arithmeticMean = ScoreCombinationCores.forTechnique(FusionTechniqueNames.ARITHMETIC_MEAN);
        assertEquals(0.5f, arithmeticMean.combine(scores), 1e-6f);
        assertEquals(0.0f, arithmeticMean.combine(new float[0]), 0.0f);
        assertEquals(0.0f, arithmeticMean.combine(new float[] { -1.0f }), 0.0f);
        // zero scores are skipped by the geometric and harmonic means
        assertEquals(
            (float) Math.sqrt(0.5d),
            ScoreCombinationCores.forTechnique(FusionTechniqueNames.GEOMETRIC_MEAN).combine(scores),
            1e-6f
        );
        assertEquals(2.0f / 3.0f, ScoreCombinationCores.forTechnique(FusionTechniqueNames.HARMONIC_MEAN).combine(scores), 1e-6f);
        // rrf sums without dividing by the weights
        assertEquals(1.5f, ScoreCombinationCores.forTechnique(FusionTechniqueNames.RRF_COMBINATION).combine(scores), 1e-6f);
        // nothing matched anywhere
        for (String technique : ScoreCombinationCores.supportedTechniques()) {
            assertEquals(technique, 0.0f, ScoreCombinationCores.forTechnique(technique).combine(new float[] { 0.0f, 0.0f }), 0.0f);
        }
    }

    public void testDescribe() {
        assertEquals("arithmetic_mean", ScoreCombinationCores.forTechnique(FusionTechniqueNames.ARITHMETIC_MEAN).describe());
        assertEquals(
            "harmonic_mean, weights [0.3, 0.7]",
            ScoreCombinationCores.forTechnique(FusionTechniqueNames.HARMONIC_MEAN, CombinationWeights.of(List.of(0.3f, 0.7f))).describe()
        );
        assertEquals("rrf", FusionTechniqueDescriptions.describeCombination("rrf", null));
        expectThrows(IllegalArgumentException.class, () -> FusionTechniqueDescriptions.describeCombination(null, List.of()));
    }

    private float[] randomScores(final int legs) {
        float[] scores = new float[legs];
        for (int i = 0; i < legs; i++) {
            // 0.0f stands for "absent from this leg", which every technique must handle
            scores[i] = randomBoolean() ? 0.0f : (float) randomDoubleBetween(0.0f, 1.0f, true);
        }
        return scores;
    }

    private List<Float> randomWeights(final int legs) {
        // hundredths that add up to exactly 100, so the float sum is 1.0 within the classic tolerance
        int[] hundredths = new int[legs];
        int remaining = 100;
        for (int i = 0; i < legs - 1; i++) {
            hundredths[i] = randomIntBetween(0, remaining);
            remaining -= hundredths[i];
        }
        hundredths[legs - 1] = remaining;
        List<Float> weights = new ArrayList<>(legs);
        for (int hundredth : hundredths) {
            weights.add(hundredth / 100f);
        }
        return weights;
    }
}
