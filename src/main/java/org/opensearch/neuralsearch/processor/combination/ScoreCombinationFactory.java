/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Abstracts creation of exact score combination method based on technique name
 */
public class ScoreCombinationFactory {
    private static final ScoreCombinationUtil scoreCombinationUtil = new ScoreCombinationUtil();

    public static final ScoreCombinationTechnique DEFAULT_METHOD = new ArithmeticMeanScoreCombinationTechnique(
        Map.of(),
        scoreCombinationUtil
    );

    private final Map<String, Function<Map<String, Object>, ScoreCombinationTechnique>> scoreCombinationMethodsMap = Map.of(
        ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME,
        params -> new ArithmeticMeanScoreCombinationTechnique(params, scoreCombinationUtil),
        HarmonicMeanScoreCombinationTechnique.TECHNIQUE_NAME,
        params -> new HarmonicMeanScoreCombinationTechnique(params, scoreCombinationUtil),
        GeometricMeanScoreCombinationTechnique.TECHNIQUE_NAME,
        params -> new GeometricMeanScoreCombinationTechnique(params, scoreCombinationUtil),
        RRFScoreCombinationTechnique.TECHNIQUE_NAME,
        params -> new RRFScoreCombinationTechnique()
    );

    /**
     * Get score combination method by technique name
     * @param technique name of technique
     * @return instance of ScoreCombinationTechnique for technique name
     */
    public ScoreCombinationTechnique createCombination(final String technique) {
        return createCombination(technique, Map.of());
    }

    /**
     * Get score combination method by technique name
     * @param technique name of technique
     * @param params parameters that combination technique may use
     * @return instance of ScoreCombinationTechnique for technique name
     */
    public ScoreCombinationTechnique createCombination(final String technique, final Map<String, Object> params) {
        return Optional.ofNullable(scoreCombinationMethodsMap.get(technique))
            .orElseThrow(() -> new IllegalArgumentException("provided combination technique is not supported"))
            .apply(params);
    }
}
