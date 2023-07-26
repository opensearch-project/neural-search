/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.normalization;

import java.util.Map;
import java.util.Optional;

import org.opensearch.OpenSearchParseException;

/**
 * Abstracts creation of exact score normalization method based on technique name
 */
public class ScoreNormalizationFactory {

    public static final ScoreNormalizationTechnique DEFAULT_METHOD = new MinMaxScoreNormalizationTechnique();

    private final Map<String, ScoreNormalizationTechnique> scoreNormalizationMethodsMap = Map.of(
        MinMaxScoreNormalizationTechnique.TECHNIQUE_NAME,
        new MinMaxScoreNormalizationTechnique()
    );

    /**
     * Get score normalization method by technique name
     * @param technique name of technique
     * @return instance of ScoreNormalizationMethod for technique name
     */
    public ScoreNormalizationTechnique createNormalization(final String technique) {
        return Optional.ofNullable(scoreNormalizationMethodsMap.get(technique))
            .orElseThrow(() -> new OpenSearchParseException("provided normalization technique is not supported"));
    }
}
