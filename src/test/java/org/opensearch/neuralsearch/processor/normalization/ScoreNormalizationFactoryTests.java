/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.neuralsearch.util.TestUtils.PARAM_NAME_LOWER_BOUNDS;

import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScoreNormalizationFactoryTests extends OpenSearchQueryTestCase {

    public void testMinMaxNorm_whenCreatingByName_thenReturnCorrectInstance() {
        ScoreNormalizationFactory scoreNormalizationFactory = new ScoreNormalizationFactory();
        ScoreNormalizationTechnique scoreNormalizationTechnique = scoreNormalizationFactory.createNormalization("min_max");

        assertNotNull(scoreNormalizationTechnique);
        assertTrue(scoreNormalizationTechnique instanceof MinMaxScoreNormalizationTechnique);
    }

    public void testL2Norm_whenCreatingByName_thenReturnCorrectInstance() {
        ScoreNormalizationFactory scoreNormalizationFactory = new ScoreNormalizationFactory();
        ScoreNormalizationTechnique scoreNormalizationTechnique = scoreNormalizationFactory.createNormalization("l2");

        assertNotNull(scoreNormalizationTechnique);
        assertTrue(scoreNormalizationTechnique instanceof L2ScoreNormalizationTechnique);
    }

    public void testRRFNorm_whenCreatingByName_thenReturnCorrectInstance() {
        ScoreNormalizationFactory scoreNormalizationFactory = new ScoreNormalizationFactory();
        ScoreNormalizationTechnique scoreNormalizationTechnique = scoreNormalizationFactory.createNormalization("rrf");

        assertNotNull(scoreNormalizationTechnique);
        assertTrue(scoreNormalizationTechnique instanceof RRFNormalizationTechnique);
    }

    public void testZScoreNorm_whenCreatingByName_thenReturnCorrectInstance() {
        ScoreNormalizationFactory scoreNormalizationFactory = new ScoreNormalizationFactory();
        ScoreNormalizationTechnique scoreNormalizationTechnique = scoreNormalizationFactory.createNormalization("z_score");

        assertNotNull(scoreNormalizationTechnique);
        assertTrue(scoreNormalizationTechnique instanceof ZScoreNormalizationTechnique);
    }

    public void testUnsupportedTechnique_whenPassingInvalidName_thenFail() {
        ScoreNormalizationFactory scoreNormalizationFactory = new ScoreNormalizationFactory();
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> scoreNormalizationFactory.createNormalization("randomname")
        );
        assertThat(illegalArgumentException.getMessage(), containsString("provided normalization technique is not supported"));
    }

    public void testCreateMinMaxNormalizationWithParameters() {
        Map<String, Object> parameters = new HashMap<>();

        List<Map<String, Object>> lowerBounds = Arrays.asList(Map.of("mode", "clip", "min_score", 0.1));
        parameters.put(PARAM_NAME_LOWER_BOUNDS, lowerBounds);

        ScoreNormalizationFactory scoreNormalizationFactory = new ScoreNormalizationFactory();
        ScoreNormalizationTechnique normalizationTechnique = scoreNormalizationFactory.createNormalization("min_max", parameters);

        assertNotNull(normalizationTechnique);
        assertTrue(normalizationTechnique instanceof MinMaxScoreNormalizationTechnique);
    }

    public void testThrowsExceptionForInvalidTechniqueWithParameters() {
        Map<String, Object> parameters = new HashMap<>();

        List<Map<String, Object>> lowerBounds = Arrays.asList(Map.of("mode", "clip", "min_score", 0.1));
        parameters.put(PARAM_NAME_LOWER_BOUNDS, lowerBounds);

        ScoreNormalizationFactory scoreNormalizationFactory = new ScoreNormalizationFactory();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> scoreNormalizationFactory.createNormalization(L2ScoreNormalizationTechnique.TECHNIQUE_NAME, parameters)
        );
        assertEquals("unrecognized parameters in normalization technique", exception.getMessage());
    }

}
