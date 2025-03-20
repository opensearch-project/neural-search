/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper.semanticfieldtypes;

import lombok.Getter;

/**
 * A DTO to hold all the semantic parameters.
 */
@Getter
public class SemanticParameters {
    private final String modelId;
    private final String searchModelId;
    private final String rawFieldType;
    private final String semanticInfoFieldName;

    public SemanticParameters(String modelId, String searchModelId, String rawFieldType, String semanticInfoFieldName) {
        this.modelId = modelId;
        this.searchModelId = searchModelId;
        this.semanticInfoFieldName = semanticInfoFieldName;
        this.rawFieldType = rawFieldType;
    }
}
