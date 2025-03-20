/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper.semanticfieldtypes;

import lombok.Getter;
import lombok.NonNull;
import org.apache.lucene.search.Query;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.neuralsearch.mapper.SemanticFieldMapper;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.lookup.SearchLookup;

import java.time.ZoneId;
import java.util.function.Supplier;

/**
 * A semantic field type that delegate the work to a mapped field type.
 */
public class SemanticFieldType extends MappedFieldType {
    @Getter
    private SemanticParameters semanticParameters;
    protected MappedFieldType delegateFieldType;

    public SemanticFieldType(@NonNull final MappedFieldType delegateFieldType, @NonNull final SemanticParameters semanticParameters) {
        super(
            delegateFieldType.name(),
            delegateFieldType.isSearchable(),
            delegateFieldType.isStored(),
            delegateFieldType.hasDocValues(),
            delegateFieldType.getTextSearchInfo(),
            delegateFieldType.meta()
        );
        this.delegateFieldType = delegateFieldType;
        this.semanticParameters = semanticParameters;
    }

    @Override
    public ValueFetcher valueFetcher(QueryShardContext queryShardContext, SearchLookup searchLookup, String format) {
        return delegateFieldType.valueFetcher(queryShardContext, searchLookup, format);
    }

    @Override
    public String typeName() {
        return SemanticFieldMapper.CONTENT_TYPE;
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        return delegateFieldType.termQuery(value, context);
    }

    @Override
    public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
        return delegateFieldType.docValueFormat(format, timeZone);
    }

    @Override
    public BytesReference valueForDisplay(Object value) {
        return (BytesReference) delegateFieldType.valueForDisplay(value);
    }

    @Override
    public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        return delegateFieldType.fielddataBuilder(fullyQualifiedIndexName, searchLookup);
    }
}
