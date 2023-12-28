/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import static org.hamcrest.Matchers.containsString;

import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

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

    public void testUnsupportedTechnique_whenPassingInvalidName_thenFail() {
        ScoreNormalizationFactory scoreNormalizationFactory = new ScoreNormalizationFactory();
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> scoreNormalizationFactory.createNormalization("randomname")
        );
        assertThat(illegalArgumentException.getMessage(), containsString("provided normalization technique is not supported"));
    }
}
