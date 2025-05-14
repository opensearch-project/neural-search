/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class NeuralQueryTargetFieldConfig {
    private final Boolean isSemanticField;
    private final Boolean isUnmappedField;
    private final String searchModelId;
    private final String embeddingFieldType;
    private final String embeddingFieldPath;
    private final String chunksPath;
}
