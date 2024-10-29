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

@AllArgsConstructor
@Builder
@Getter
public class ProcessorExplainDto {
    Explanation explanation;
    Map<ExplanationType, Object> explainPayload;

    public enum ExplanationType {
        NORMALIZATION_PROCESSOR
    }
}
