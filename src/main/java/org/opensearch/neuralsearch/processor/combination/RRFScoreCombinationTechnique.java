/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

import lombok.ToString;
import lombok.extern.log4j.Log4j2;

@Log4j2
/**
 * Abstracts combination of scores based on geometrical mean method
 */
@ToString(onlyExplicitlyIncluded = true)

public class RRFScoreCombinationTechnique implements ScoreCombinationTechnique {
    @ToString.Include
    public static final String TECHNIQUE_NAME = "rrf";
    public static final String RANK_CONSTANT = "rank_constant";
    // private static final String SUPPORTED_PARAMS = RANK_CONSTANT;
    // private final ScoreCombinationUtil scoreCombinationUtil;
    // public static int rankConstant;

    public RRFScoreCombinationTechnique(int i) {
        log.debug(i);
    }

    @Override
    public float combine(final float[] scores) {
        float sumScores = 0.0f;
        for (float score : scores) {
            sumScores += score;
        }
        sumScores *= 10;
        return sumScores;
    }
    // scoreCombinationUtil.validateIfWeightsMatchScores(scores, weights);
    // float weightedLnSum = 0;
    // float sumOfWeights = 0;
    // for (int indexOfSubQuery = 0; indexOfSubQuery < scores.length; indexOfSubQuery++) {
    // float score = scores[indexOfSubQuery];
    // if (score <= 0) {
    // // scores 0.0 need to be skipped, ln() of 0 is not defined
    // continue;
    // }
    // float weight = scoreCombinationUtil.getWeightForSubQuery(weights, indexOfSubQuery);
    // sumOfWeights += weight;
    // weightedLnSum += weight * Math.log(score);
    // }
    // return sumOfWeights == 0 ? ZERO_SCORE : (float) Math.exp(weightedLnSum / sumOfWeights);
    // }

}
