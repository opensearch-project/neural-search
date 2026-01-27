/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;

import java.util.List;

/**
 * DTO object to hold data required for explain.
 */
@AllArgsConstructor
@Builder
@Getter
public class ExplainDTO {
    @NonNull
    private List<CompoundTopDocs> queryTopDocs;
    @NonNull
    private ExplainableTechnique explainableTechnique;
    private boolean singleShard;
}
