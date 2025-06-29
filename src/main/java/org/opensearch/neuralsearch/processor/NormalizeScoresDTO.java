/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;

import java.util.List;

/**
 * DTO object to hold data required for score normalization.
 */
@AllArgsConstructor
@Builder
@Getter
public class NormalizeScoresDTO {
    @NonNull
    private List<CompoundTopDocs> queryTopDocs;
    @NonNull
    private ScoreNormalizationTechnique normalizationTechnique;
    @NonNull
    private boolean subQueryScores;
}
