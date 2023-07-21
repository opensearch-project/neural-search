/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.normalization;

import java.util.List;

import lombok.AllArgsConstructor;

import org.opensearch.neuralsearch.search.CompoundTopDocs;

/**
 * Collection of techniques for score normalization
 */
@AllArgsConstructor
public enum ScoreNormalizationTechnique {

    /**
     * Min-max normalization method.
     * nscore = (score - min_score)/(max_score - min_score)
     */
    MIN_MAX(MinMaxScoreNormalizationMethod.getInstance());

    public static final ScoreNormalizationTechnique DEFAULT = MIN_MAX;
    private final ScoreNormalizationMethod method;

    public void normalize(final List<CompoundTopDocs> queryTopDocs) {
        method.normalize(queryTopDocs);
    }
}
