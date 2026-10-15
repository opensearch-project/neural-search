/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import org.opensearch.neuralsearch.processor.dto.TechniqueCompatibilityCheckDTO;

import java.util.Map;
import org.opensearch.neuralsearch.fusion.math.FusionTechniquePairing;
import java.util.Set;
import java.util.Optional;
import java.util.function.Function;

/**
 * Abstracts creation of exact score normalization method based on technique name
 */
public class ScoreNormalizationFactory {

    private static final ScoreNormalizationUtil scoreNormalizationUtil = new ScoreNormalizationUtil();

    public static final ScoreNormalizationTechnique DEFAULT_METHOD = new MinMaxScoreNormalizationTechnique();

    private static final Map<String, Function<Map<String, Object>, ScoreNormalizationTechnique>> SCORE_NORMALIZATION_METHODS = Map.of(
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
        return Optional.ofNullable(SCORE_NORMALIZATION_METHODS.get(technique))
            .orElseThrow(() -> new IllegalArgumentException("provided normalization technique is not supported"))
            .apply(params);
    }

    /**
     * Combination techniques a normalization technique may be paired with, by name. Exposed so callers that validate before
     * any technique instance exists — notably the fused-mode gate, which validates names at query rewrite — read the same
     * matrix {@link #isTechniquesCompatible} enforces, rather than keeping a second copy of it.
     *
     * @param normalizationTechniqueName name of the normalization technique
     * @return the combination technique names supported for it, empty for an unrecognized normalization technique
     */
    public static Set<String> supportedCombinationTechniques(final String normalizationTechniqueName) {
        return FusionTechniquePairing.combinationTechniquesFor(normalizationTechniqueName);
    }

    /**
     * Validate normalization technique based on combination technique and other params that needs to be validated
     * @param techniqueCompatibilityCheckDTO data transfer object that contains combination technique and other params that needs to be validated
     */
    public void isTechniquesCompatible(TechniqueCompatibilityCheckDTO techniqueCompatibilityCheckDTO) {
        FusionTechniquePairing.requireCompatible(
            techniqueCompatibilityCheckDTO.getScoreNormalizationTechnique().techniqueName(),
            techniqueCompatibilityCheckDTO.getScoreCombinationTechnique().techniqueName()
        );
    }

}
