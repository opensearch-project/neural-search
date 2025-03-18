/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;

/**
 * DTO object to hold data required for validation.
 */
@AllArgsConstructor
@Builder
@Getter
public class ValidateNormalizationDTO {
    @NonNull
    private ScoreCombinationTechnique scoreCombinationTechnique;
}
