/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.mapper;

import lombok.Getter;
import org.apache.lucene.search.Query;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.SourceValueFetcher;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

import java.util.Map;
import java.util.function.Supplier;

/**
 * SparseTokensFieldType is responsible for handling sparse tokens field type
 */
@Getter
public class SparseTokensFieldType extends MappedFieldType {
    private final SparseMethodContext sparseMethodContext;
    protected boolean stored;
    protected boolean hasDocValues;

    public SparseTokensFieldType(String name, SparseMethodContext sparseMethodContext, boolean stored, boolean hasDocValues) {
        super(name, false, stored, hasDocValues, TextSearchInfo.NONE, Map.of());
        this.sparseMethodContext = sparseMethodContext;
        this.stored = stored;
        this.hasDocValues = hasDocValues;
    }

    @Override
    public String typeName() {
        return SparseTokensFieldMapper.CONTENT_TYPE;
    }

    @Override
    public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
        return SourceValueFetcher.identity(name(), context, format);
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        throw new IllegalArgumentException("Queries on [" + SparseTokensFieldMapper.CONTENT_TYPE + "] fields are not supported");
    }

    @Override
    public Query existsQuery(QueryShardContext context) {
        throw new IllegalArgumentException("[" + SparseTokensFieldMapper.CONTENT_TYPE + "] fields do not support [exists] queries");
    }

    @Override
    public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        throw new IllegalArgumentException(
            "[" + SparseTokensFieldMapper.CONTENT_TYPE + "] fields do not support sorting, scripting or aggregating"
        );
    }
}
