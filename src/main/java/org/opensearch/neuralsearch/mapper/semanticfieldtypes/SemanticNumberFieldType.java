/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper.semanticfieldtypes;

import lombok.Getter;
import lombok.NonNull;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.neuralsearch.mapper.SemanticFieldMapper;
import org.opensearch.search.lookup.SearchLookup;

/**
 * A semantic field type that delegate the work to a number field type.
 */
public class SemanticNumberFieldType extends NumberFieldMapper.NumberFieldType {
    @Getter
    private SemanticParameters semanticParameters;
    protected NumberFieldMapper.NumberFieldType delegateFieldType;

    public SemanticNumberFieldType(
        @NonNull final NumberFieldMapper.NumberFieldType delegateFieldType,
        @NonNull final SemanticParameters semanticParameters
    ) {
        super(
            delegateFieldType.name(),
            delegateFieldType.numberType(),
            delegateFieldType.isSearchable(),
            delegateFieldType.isStored(),
            delegateFieldType.hasDocValues(),
            delegateFieldType.coerce(),
            delegateFieldType.nullValue(),
            delegateFieldType.meta()
        );
        this.delegateFieldType = delegateFieldType;
        this.semanticParameters = semanticParameters;
    }

    @Override
    public String typeName() {
        return SemanticFieldMapper.CONTENT_TYPE;
    }

    @Override
    public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
        return delegateFieldType.valueFetcher(context, searchLookup, format);
    }
}
