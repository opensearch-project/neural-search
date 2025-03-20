/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper.semanticfieldtypes;

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

import java.util.Set;

/**
 * A factory to create the semantic field type based on the raw field type.
 */
public class SemanticFieldTypeFactory {
    private static final Set<String> STRING_FIELD_TYPES = Set.of(
        TextFieldMapper.CONTENT_TYPE,
        MatchOnlyTextFieldMapper.CONTENT_TYPE,
        KeywordFieldMapper.CONTENT_TYPE,
        WildcardFieldMapper.CONTENT_TYPE
    );

    private SemanticFieldTypeFactory() {
        // Private constructor to prevent instantiation
    }

    private static class Holder {
        private static final SemanticFieldTypeFactory INSTANCE = new SemanticFieldTypeFactory();
    }

    public static SemanticFieldTypeFactory getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * Create the semantic field type based on the raw field type.
     * @param delegateMapper delegate field mapper
     * @param rawFieldType field type for the raw data
     * @param semanticParameters semantic parameters
     * @return semantic field type
     */
    public MappedFieldType createSemanticFieldType(
        @NonNull final ParametrizedFieldMapper delegateMapper,
        @NonNull final String rawFieldType,
        @NonNull final SemanticParameters semanticParameters
    ) {
        if (BinaryFieldMapper.CONTENT_TYPE.equals(rawFieldType)) {
            return new SemanticFieldType(delegateMapper.fieldType(), semanticParameters);
        } else if (STRING_FIELD_TYPES.contains(rawFieldType)) {
            return new SemanticStringFieldType((StringFieldType) delegateMapper.fieldType(), semanticParameters);
        } else if (TokenCountFieldMapper.CONTENT_TYPE.equals(rawFieldType)) {
            return new SemanticNumberFieldType((NumberFieldMapper.NumberFieldType) delegateMapper.fieldType(), semanticParameters);
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
