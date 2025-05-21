/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper.dto;

import lombok.Builder;
import lombok.Getter;

/**
 * A DTO to hold all the semantic parameters.
 */
@Getter
@Builder
public class SemanticParameters {
    private final String modelId;
    private final String searchModelId;
    private final String rawFieldType;
    private final String semanticInfoFieldName;
    private final Boolean chunkingEnabled;
}
