/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.neuralsearch.processor.combination;

import static org.hamcrest.Matchers.containsString;

import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

public class ScoreCombinationFactoryTests extends OpenSearchQueryTestCase {

    public void testArithmeticWeightedMean_whenCreatingByName_thenReturnCorrectInstance() {
        ScoreCombinationFactory scoreCombinationFactory = new ScoreCombinationFactory();
        ScoreCombinationTechnique scoreCombinationTechnique = scoreCombinationFactory.createCombination("arithmetic_mean");

        assertNotNull(scoreCombinationTechnique);
        assertTrue(scoreCombinationTechnique instanceof ArithmeticMeanScoreCombinationTechnique);
    }

    public void testHarmonicWeightedMean_whenCreatingByName_thenReturnCorrectInstance() {
        ScoreCombinationFactory scoreCombinationFactory = new ScoreCombinationFactory();
        ScoreCombinationTechnique scoreCombinationTechnique = scoreCombinationFactory.createCombination("harmonic_mean");

        assertNotNull(scoreCombinationTechnique);
        assertTrue(scoreCombinationTechnique instanceof HarmonicMeanScoreCombinationTechnique);
    }

    public void testGeometricWeightedMean_whenCreatingByName_thenReturnCorrectInstance() {
        ScoreCombinationFactory scoreCombinationFactory = new ScoreCombinationFactory();
        ScoreCombinationTechnique scoreCombinationTechnique = scoreCombinationFactory.createCombination("geometric_mean");

        assertNotNull(scoreCombinationTechnique);
        assertTrue(scoreCombinationTechnique instanceof GeometricMeanScoreCombinationTechnique);
    }

    public void testUnsupportedTechnique_whenPassingInvalidName_thenFail() {
        ScoreCombinationFactory scoreCombinationFactory = new ScoreCombinationFactory();
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> scoreCombinationFactory.createCombination("randomname")
        );
        org.hamcrest.MatcherAssert.assertThat(
            illegalArgumentException.getMessage(),
            containsString("provided combination technique is not supported")
        );
    }
}
