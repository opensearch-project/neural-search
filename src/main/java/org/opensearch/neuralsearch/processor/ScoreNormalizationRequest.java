/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import lombok.Builder;
import lombok.Data;

/**
 * DTO for normalize method request
 */
@Data
@Builder
public class ScoreNormalizationRequest {
    private final float score;
    private final float minScore;
    private final float maxScore;
}
