/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import lombok.Builder;
import lombok.Data;

import com.google.common.primitives.Floats;

/**
 * Collection of techniques for score normalization
 */
public enum ScoreNormalizationTechnique {

    /**
     * Min-max normalization method.
     * nscore = (score - min_score)/(max_score - min_score)
     */
    MIN_MAX {
        @Override
        float normalize(ScoreNormalizationRequest request) {
            // edge case when there is only one score and min and max scores are same
            if (Floats.compare(request.getMaxScore(), request.getMinScore()) == 0
                && Floats.compare(request.getMaxScore(), request.getScore()) == 0) {
                return SINGLE_RESULT_SCORE;
            }
            float normalizedScore = (request.getScore() - request.getMinScore()) / (request.getMaxScore() - request.getMinScore());
            return normalizedScore == 0.0f ? MIN_SCORE : normalizedScore;
        }
    };

    public static final ScoreNormalizationTechnique DEFAULT = MIN_MAX;

    static final float MIN_SCORE = 0.001f;
    static final float SINGLE_RESULT_SCORE = 1.0f;

    /**
     * Defines normalization function specific to this technique
     * @param request complex request DTO that defines parameters like min/max scores etc.
     * @return normalized score
     */
    abstract float normalize(ScoreNormalizationRequest request);

    /**
     * DTO for normalize method request
     */
    @Data
    @Builder
    static class ScoreNormalizationRequest {
        private final float score;
        private final float minScore;
        private final float maxScore;
    }
}
