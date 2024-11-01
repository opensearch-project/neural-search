/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.explain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.lucene.search.Explanation;

import java.util.Map;

/**
 * DTO class to hold explain details for normalization and combination
 */
@AllArgsConstructor
@Builder
@Getter
public class ExplanationResponse {
    Explanation explanation;
    Map<ExplanationType, Object> explainPayload;

    public enum ExplanationType {
        NORMALIZATION_PROCESSOR
    }
}