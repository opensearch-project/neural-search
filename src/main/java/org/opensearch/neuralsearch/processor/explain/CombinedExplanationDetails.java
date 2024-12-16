/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.explain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * DTO class to hold explain details for normalization and combination
 */
@AllArgsConstructor
@Builder
@Getter
public class CombinedExplanationDetails {
    private ExplanationDetails normalizationExplanations;
    private ExplanationDetails combinationExplanations;
}
