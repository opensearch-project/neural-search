/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

import lombok.ToString;
import lombok.extern.log4j.Log4j2;

import java.util.Map;

@Log4j2
/**
 * Abstracts combination of scores based on reciprocal rank fusion algorithm
 */
@ToString(onlyExplicitlyIncluded = true)
public class RRFScoreCombinationTechnique implements ScoreCombinationTechnique {
    @ToString.Include
    public static final String TECHNIQUE_NAME = "rrf";
    private final ScoreCombinationUtil scoreCombinationUtil;

    // Not currently using weights for RRF, no need to modify or verify these params
    public RRFScoreCombinationTechnique(final Map<String, Object> params, final ScoreCombinationUtil combinationUtil) {
        this.scoreCombinationUtil = combinationUtil;
    }

    @Override
    public float combine(final float[] scores) {
        float sumScores = 0.0f;
        for (float score : scores) {
            sumScores += score;
        }
        return sumScores;
    }
}
