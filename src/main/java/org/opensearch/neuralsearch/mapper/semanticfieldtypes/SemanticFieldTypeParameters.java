/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper.semanticfieldtypes;

import lombok.Getter;
import org.opensearch.neuralsearch.mapper.SemanticFieldMapper;

/**
 * An object to hold all the parameters we can have in a semantic field type.
 */
@Getter
public class SemanticFieldTypeParameters {
    private final String modelId;
    private final String searchModelId;
    private final String semanticInfoFieldName;
    private final String rawFieldType;

    public SemanticFieldTypeParameters(SemanticFieldMapper.Builder builder) {
        modelId = builder.getModelId().getValue();
        searchModelId = builder.getSearchModelId().getValue();
        semanticInfoFieldName = builder.getSemanticInfoFieldName().getValue();
        rawFieldType = builder.getRawFieldType().getValue();
    }
}
