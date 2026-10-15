/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion.math;

import java.util.Set;

import org.opensearch.neuralsearch.processor.combination.ArithmeticMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.GeometricMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.HarmonicMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.RRFScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.normalization.L2ScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.MinMaxScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.RRFNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.processor.normalization.ZScoreNormalizationTechnique;
import org.opensearch.test.OpenSearchTestCase;

public class FusionTechniquePairingTests extends OpenSearchTestCase {

    private static final Set<String> MEANS = Set.of("arithmetic_mean", "geometric_mean", "harmonic_mean");

    public void testNames_areTheRequestStrings() {
        // the literal strings are the public contract; the classic techniques read them from here
        assertEquals("min_max", FusionTechniqueNames.MIN_MAX);
        assertEquals("l2", FusionTechniqueNames.L2);
        assertEquals("z_score", FusionTechniqueNames.Z_SCORE);
        assertEquals("rrf", FusionTechniqueNames.RRF);
        assertEquals("arithmetic_mean", FusionTechniqueNames.ARITHMETIC_MEAN);
        assertEquals("geometric_mean", FusionTechniqueNames.GEOMETRIC_MEAN);
        assertEquals("harmonic_mean", FusionTechniqueNames.HARMONIC_MEAN);
        assertEquals("rrf", FusionTechniqueNames.RRF_COMBINATION);
        assertEquals("weights", FusionTechniqueNames.PARAM_WEIGHTS);
        assertEquals("rank_constant", FusionTechniqueNames.PARAM_RANK_CONSTANT);
        assertEquals("lower_bounds", FusionTechniqueNames.PARAM_LOWER_BOUNDS);
        assertEquals("upper_bounds", FusionTechniqueNames.PARAM_UPPER_BOUNDS);
        assertEquals(FusionTechniqueNames.MIN_MAX, MinMaxScoreNormalizationTechnique.TECHNIQUE_NAME);
        assertEquals(FusionTechniqueNames.L2, L2ScoreNormalizationTechnique.TECHNIQUE_NAME);
        assertEquals(FusionTechniqueNames.Z_SCORE, ZScoreNormalizationTechnique.TECHNIQUE_NAME);
        assertEquals(FusionTechniqueNames.RRF, RRFNormalizationTechnique.TECHNIQUE_NAME);
        assertEquals(FusionTechniqueNames.ARITHMETIC_MEAN, ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME);
        assertEquals(FusionTechniqueNames.GEOMETRIC_MEAN, GeometricMeanScoreCombinationTechnique.TECHNIQUE_NAME);
        assertEquals(FusionTechniqueNames.HARMONIC_MEAN, HarmonicMeanScoreCombinationTechnique.TECHNIQUE_NAME);
        assertEquals(FusionTechniqueNames.RRF_COMBINATION, RRFScoreCombinationTechnique.TECHNIQUE_NAME);
    }

    public void testMatrix_isTheClassicMatrix() {
        assertEquals(MEANS, FusionTechniquePairing.combinationTechniquesFor("min_max"));
        assertEquals(MEANS, FusionTechniquePairing.combinationTechniquesFor("l2"));
        assertEquals(MEANS, FusionTechniquePairing.combinationTechniquesFor("rrf"));
        assertEquals(Set.of("arithmetic_mean"), FusionTechniquePairing.combinationTechniquesFor("z_score"));
        assertEquals(Set.of(), FusionTechniquePairing.combinationTechniquesFor("no_such_technique"));
        assertEquals(Set.of(), FusionTechniquePairing.combinationTechniquesFor(null));
        for (String normalization : Set.of("min_max", "l2", "z_score", "rrf")) {
            assertEquals(
                normalization,
                FusionTechniquePairing.combinationTechniquesFor(normalization),
                ScoreNormalizationFactory.supportedCombinationTechniques(normalization)
            );
        }
    }

    public void testIsCompatible() {
        assertTrue(FusionTechniquePairing.isCompatible("min_max", "harmonic_mean"));
        assertFalse(FusionTechniquePairing.isCompatible("z_score", "harmonic_mean"));
        // the score-ranker-processor's rrf + rrf is not in this matrix; callers exempt it before asking
        assertFalse(FusionTechniquePairing.isCompatible("rrf", "rrf"));
        FusionTechniquePairing.requireCompatible("l2", "geometric_mean");
    }

    public void testRequireCompatible_message() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FusionTechniquePairing.requireCompatible("z_score", "geometric_mean")
        );
        assertEquals(
            "provided combination technique geometric_mean is not supported for normalization technique z_score. "
                + "Supported techniques are: arithmetic_mean",
            e.getMessage()
        );
    }
}
