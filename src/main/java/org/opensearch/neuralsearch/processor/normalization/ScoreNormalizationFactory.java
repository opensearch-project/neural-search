/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import org.opensearch.neuralsearch.processor.ValidateNormalizationDTO;
import org.opensearch.neuralsearch.processor.combination.ArithmeticMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.GeometricMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.HarmonicMeanScoreCombinationTechnique;

import java.util.List;
import java.util.Locale;
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

    private final Map<String, List<String>> combinationTechniqueForNormalizationTechniqueMap = Map.of(
        MinMaxScoreNormalizationTechnique.TECHNIQUE_NAME,
        List.of(
            ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME,
            GeometricMeanScoreCombinationTechnique.TECHNIQUE_NAME,
            HarmonicMeanScoreCombinationTechnique.TECHNIQUE_NAME
        ),
        L2ScoreNormalizationTechnique.TECHNIQUE_NAME,
        List.of(
            ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME,
            GeometricMeanScoreCombinationTechnique.TECHNIQUE_NAME,
            HarmonicMeanScoreCombinationTechnique.TECHNIQUE_NAME
        ),
        RRFNormalizationTechnique.TECHNIQUE_NAME,
        List.of(
            ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME,
            GeometricMeanScoreCombinationTechnique.TECHNIQUE_NAME,
            HarmonicMeanScoreCombinationTechnique.TECHNIQUE_NAME
        ),
        ZScoreNormalizationTechnique.TECHNIQUE_NAME,
        List.of(ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME)
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

    /**
     * Validate normalization technique based on combination technique and other params that needs to be validated
     * @param normalizationTechnique normalization technique to be validated
     * @param validateNormalizationDTO data transfer object that contains combination technique and other params that needs to be validated
     */
    public void validateNormalizationTechnique(
        ScoreNormalizationTechnique normalizationTechnique,
        ValidateNormalizationDTO validateNormalizationDTO
    ) {
        List<String> supportedTechniques = combinationTechniqueForNormalizationTechniqueMap.get(normalizationTechnique.techniqueName());

        if (!supportedTechniques.contains(validateNormalizationDTO.getScoreCombinationTechnique().techniqueName())) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "provided combination technique %s is not supported for normalization technique %s. Supported techniques are: %s",
                    validateNormalizationDTO.getScoreCombinationTechnique().techniqueName(),
                    normalizationTechnique.techniqueName(),
                    String.join(", ", supportedTechniques)
                )
            );
        }
    }

}
