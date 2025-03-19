/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;

import java.util.List;
import java.util.Objects;

import static org.opensearch.neuralsearch.processor.explain.ExplanationUtils.describeCombinationTechnique;

/**
 * Abstracts combination of scores based on reciprocal rank fusion algorithm
 */
@Log4j2
@ToString(onlyExplicitlyIncluded = true)
public class RRFScoreCombinationTechnique implements ScoreCombinationTechnique, ExplainableTechnique {
    @ToString.Include
    public static final String TECHNIQUE_NAME = "rrf";

    // Not currently using weights for RRF, no need to modify or verify these params
    public RRFScoreCombinationTechnique() {}

    @Override
    public float combine(final float[] scores) {
        if (Objects.isNull(scores)) {
            throw new IllegalArgumentException("scores array cannot be null");
        }
        float sumScores = 0.0f;
        for (float score : scores) {
            sumScores += score;
        }
        return sumScores;
    }

    @Override
    public String techniqueName() {
        return TECHNIQUE_NAME;
    }

    @Override
    public String describe() {
        return describeCombinationTechnique(TECHNIQUE_NAME, List.of());
    }
}
