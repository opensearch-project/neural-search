/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;

/**
 * DTO object to hold data required for validation.
 */
@AllArgsConstructor
@Builder
@Getter
public class TechniqueCompatibilityCheckDTO {
    @NonNull
    private ScoreCombinationTechnique scoreCombinationTechnique;
    @NonNull
    private ScoreNormalizationTechnique scoreNormalizationTechnique;
}
