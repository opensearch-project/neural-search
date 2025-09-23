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
 * Field type for sparse vector fields that handles sparse vector representations.
 * Extends MappedFieldType to provide specialized behavior for neural search sparse tokens.
 */
@Getter
public class SparseVectorFieldType extends MappedFieldType {
    private final SparseMethodContext sparseMethodContext;

    /**
     * Creates a new SparseVectorFieldType.
     *
     * @param name field name
     * @param sparseMethodContext context for sparse encoding method
     */
    public SparseVectorFieldType(String name, SparseMethodContext sparseMethodContext) {
        super(name, false, false, false, TextSearchInfo.NONE, Map.of());
        this.sparseMethodContext = sparseMethodContext;
    }

    @Override
    public String typeName() {
        return SparseVectorFieldMapper.CONTENT_TYPE;
    }

    @Override
    public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
        return SourceValueFetcher.identity(name(), context, format);
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        throw new IllegalArgumentException("Queries on [" + SparseVectorFieldMapper.CONTENT_TYPE + "] fields are not supported");
    }

    @Override
    public Query existsQuery(QueryShardContext context) {
        throw new IllegalArgumentException("[" + SparseVectorFieldMapper.CONTENT_TYPE + "] fields do not support [exists] queries");
    }

    @Override
    public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        throw new IllegalArgumentException(
            "[" + SparseVectorFieldMapper.CONTENT_TYPE + "] fields do not support sorting, scripting or aggregating"
        );
    }

    public static boolean isSparseVectorType(String type) {
        return SparseVectorFieldMapper.CONTENT_TYPE.equals(type);
    }
}
