/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.combination;

import java.util.Map;
import java.util.Optional;

import org.opensearch.OpenSearchParseException;

/**
 * Abstracts creation of exact score combination method based on technique name
 */
public class ScoreCombinationFactory {

    private static final ScoreCombinationTechnique DEFAULT_COMBINATION_METHOD = ArithmeticMeanScoreCombinationTechnique.getInstance();

    private final Map<String, ScoreCombinationTechnique> scoreCombinationMethodsMap = Map.of(
        ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME,
        ArithmeticMeanScoreCombinationTechnique.getInstance()
    );

    /**
     * Get score combination method by technique name
     * @param technique name of technique
     * @return
     */
    public ScoreCombinationTechnique createCombination(final String technique) {
        return Optional.ofNullable(scoreCombinationMethodsMap.get(technique))
            .orElseThrow(() -> new OpenSearchParseException("provided combination technique is not supported"));
    }

    /**
     * Default combination method
     * @return combination method that is used in case user did not provide combination technique name
     */
    public ScoreCombinationTechnique defaultCombination() {
        return DEFAULT_COMBINATION_METHOD;
    }
}
