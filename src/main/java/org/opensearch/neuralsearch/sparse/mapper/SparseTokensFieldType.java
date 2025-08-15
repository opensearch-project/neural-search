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
 * Field type implementation for sparse tokens in neural search mappings.
 *
 * <p>This class extends OpenSearch's MappedFieldType to provide specialized
 * handling for sparse token fields. It defines the behavior and capabilities
 * of sparse token fields within the OpenSearch mapping system.
 *
 * <p>Key characteristics:
 * <ul>
 *   <li>Disables direct querying on sparse token fields</li>
 *   <li>Prevents sorting, scripting, and aggregation operations</li>
 *   <li>Supports source value fetching for field retrieval</li>
 *   <li>Integrates with sparse method configurations</li>
 * </ul>
 *
 * <p>The field type is designed to work with neural search queries rather
 * than traditional term-based queries, ensuring proper usage patterns.
 *
 * @see MappedFieldType
 * @see SparseMethodContext
 * @see SparseTokensFieldMapper
 */
@Getter
public class SparseTokensFieldType extends MappedFieldType {
    private final SparseMethodContext sparseMethodContext;
    protected boolean stored;
    protected boolean hasDocValues;

    /**
     * Constructs a SparseTokensFieldType with the specified configuration.
     *
     * @param name the field name
     * @param sparseMethodContext the sparse method configuration
     * @param stored whether the field should be stored
     * @param hasDocValues whether the field has doc values
     */
    public SparseTokensFieldType(String name, SparseMethodContext sparseMethodContext, boolean stored, boolean hasDocValues) {
        super(name, false, stored, hasDocValues, TextSearchInfo.NONE, Map.of());
        this.sparseMethodContext = sparseMethodContext;
        this.stored = stored;
        this.hasDocValues = hasDocValues;
    }

    /**
     * Returns the type name for sparse tokens fields.
     *
     * @return the content type identifier
     */
    @Override
    public String typeName() {
        return SparseTokensFieldMapper.CONTENT_TYPE;
    }

    /**
     * Creates a value fetcher for retrieving field values from source.
     *
     * @param context the query shard context
     * @param searchLookup the search lookup
     * @param format the format for value fetching
     * @return a source value fetcher for this field
     */
    @Override
    public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
        return SourceValueFetcher.identity(name(), context, format);
    }

    /**
     * Throws an exception as term queries are not supported on sparse token fields.
     *
     * @param value the query value
     * @param context the query shard context
     * @throws IllegalArgumentException always, as term queries are not supported
     */
    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        throw new IllegalArgumentException("Queries on [" + SparseTokensFieldMapper.CONTENT_TYPE + "] fields are not supported");
    }

    /**
     * Throws an exception as exists queries are not supported on sparse token fields.
     *
     * @param context the query shard context
     * @throws IllegalArgumentException always, as exists queries are not supported
     */
    @Override
    public Query existsQuery(QueryShardContext context) {
        throw new IllegalArgumentException("[" + SparseTokensFieldMapper.CONTENT_TYPE + "] fields do not support [exists] queries");
    }

    /**
     * Throws an exception as field data operations are not supported.
     *
     * @param fullyQualifiedIndexName the index name
     * @param searchLookup the search lookup supplier
     * @throws IllegalArgumentException always, as field data operations are not supported
     */
    @Override
    public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        throw new IllegalArgumentException(
            "[" + SparseTokensFieldMapper.CONTENT_TYPE + "] fields do not support sorting, scripting or aggregating"
        );
    }
}
