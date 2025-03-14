/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper.semanticfieldtypes;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.opensearch.index.mapper.BinaryFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MatchOnlyTextFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.StringFieldType;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.mapper.TokenCountFieldMapper;
import org.opensearch.index.mapper.WildcardFieldMapper;
import org.opensearch.neuralsearch.mapper.SemanticFieldMapper;

import java.util.Set;

/**
 * A factory to create the semantic field type based on the raw field type.
 */
@NoArgsConstructor
public class SemanticFieldTypeFactory {
    private static final Set<String> STRING_FIELD_TYPES = Set.of(
        TextFieldMapper.CONTENT_TYPE,
        MatchOnlyTextFieldMapper.CONTENT_TYPE,
        KeywordFieldMapper.CONTENT_TYPE,
        WildcardFieldMapper.CONTENT_TYPE
    );

    /**
     * Create the semantic field type based on the raw field type.
     * @param delegateMapper delegate field mapper
     * @param rawFieldType field type for the raw data
     * @param builder semantic field mapper builder
     * @return semantic field type
     */
    public MappedFieldType createSemanticFieldType(
        @NonNull final ParametrizedFieldMapper delegateMapper,
        @NonNull final String rawFieldType,
        SemanticFieldMapper.Builder builder
    ) {
        if (BinaryFieldMapper.CONTENT_TYPE.equals(rawFieldType)) {
            return new SemanticFieldType(delegateMapper.fieldType(), builder);
        } else if (STRING_FIELD_TYPES.contains(rawFieldType)) {
            return new SemanticStringFieldType((StringFieldType) delegateMapper.fieldType(), builder);
        } else if (TokenCountFieldMapper.CONTENT_TYPE.equals(rawFieldType)) {
            return new SemanticNumberFieldType((NumberFieldMapper.NumberFieldType) delegateMapper.fieldType(), builder);
        } else {
            throw new IllegalArgumentException(
                "Failed to create delegate field type for semantic field ["
                    + delegateMapper.name()
                    + "]. Unsupported raw field type: "
                    + rawFieldType
            );
        }
    }
}
