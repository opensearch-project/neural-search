/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Abstracts creation of exact score normalization method based on technique name
 */
public class ScoreNormalizationFactory {

    private static final ScoreNormalizationUtil scoreNormalizationUtil = new ScoreNormalizationUtil();

    public static final ScoreNormalizationTechnique DEFAULT_METHOD = new MinMaxScoreNormalizationTechnique();

    private final Map<String, Function<Map<String, Object>, ScoreNormalizationTechnique>> scoreNormalizationMethodsMap = Map.of(
        MinMaxScoreNormalizationTechnique.TECHNIQUE_NAME,
        params -> new MinMaxScoreNormalizationTechnique(params, scoreNormalizationUtil),
        L2ScoreNormalizationTechnique.TECHNIQUE_NAME,
        params -> new L2ScoreNormalizationTechnique(params, scoreNormalizationUtil),
        RRFNormalizationTechnique.TECHNIQUE_NAME,
        params -> new RRFNormalizationTechnique(params, scoreNormalizationUtil),
        ZScoreNormalizationTechnique.TECHNIQUE_NAME,
        params -> new ZScoreNormalizationTechnique()
    );

    /**
     * Get score normalization method by technique name
     * @param technique name of technique
     * @return instance of ScoreNormalizationMethod for technique name
     */
    public ScoreNormalizationTechnique createNormalization(final String technique) {
        return createNormalization(technique, Map.of());
    }

    public ScoreNormalizationTechnique createNormalization(final String technique, final Map<String, Object> params) {
        return Optional.ofNullable(scoreNormalizationMethodsMap.get(technique))
            .orElseThrow(() -> new IllegalArgumentException("provided normalization technique is not supported"))
            .apply(params);
    }
}
