/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper.semanticfieldtypes;

import lombok.Getter;
import lombok.NonNull;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.mapper.StringFieldType;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.IntervalMode;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.neuralsearch.mapper.SemanticFieldMapper;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/**
 * A semantic field type that delegate the work to a string field type.
 */
public class SemanticStringFieldType extends StringFieldType {
    @Getter
    private SemanticParameters semanticParameters;
    protected StringFieldType delegateFieldType;

    public SemanticStringFieldType(@NonNull final StringFieldType delegateFieldType, @NonNull final SemanticParameters semanticParameters) {
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
    public ValueFetcher valueFetcher(QueryShardContext queryShardContext, SearchLookup searchLookup, String s) {
        return delegateFieldType.valueFetcher(queryShardContext, searchLookup, s);
    }

    @Override
    public String typeName() {
        return SemanticFieldMapper.CONTENT_TYPE;
    }

    @Override
    public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, QueryShardContext context) {
        return delegateFieldType.prefixQuery(value, method, caseInsensitive, context);
    }

    @Override
    public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, QueryShardContext context) {
        return delegateFieldType.spanPrefixQuery(value, method, context);
    }

    @Override
    public IntervalsSource intervals(String query, int max_gaps, IntervalMode mode, NamedAnalyzer analyzer, boolean prefix)
        throws IOException {
        return delegateFieldType.intervals(query, max_gaps, mode, analyzer, prefix);
    }

    @Override
    public Query phraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
        return delegateFieldType.phraseQuery(stream, slop, enablePositionIncrements);
    }

    @Override
    public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
        return delegateFieldType.multiPhraseQuery(stream, slop, enablePositionIncrements);
    }

    @Override
    public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions) throws IOException {
        return delegateFieldType.phrasePrefixQuery(stream, slop, maxExpansions);
    }

    @Override
    public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        return delegateFieldType.fielddataBuilder(fullyQualifiedIndexName, searchLookup);
    }

    @Override
    public Query phraseQuery(TokenStream stream, int slop, boolean enablePosIncrements, QueryShardContext context) throws IOException {
        return delegateFieldType.phraseQuery(stream, slop, enablePosIncrements, context);
    }

    @Override
    public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, QueryShardContext context)
        throws IOException {
        return delegateFieldType.multiPhraseQuery(stream, slop, enablePositionIncrements, context);
    }

    @Override
    public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions, QueryShardContext context) throws IOException {
        return delegateFieldType.phrasePrefixQuery(stream, slop, maxExpansions, context);
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        return delegateFieldType.termQuery(value, context);
    }

    @Override
    public Query termQueryCaseInsensitive(Object value, QueryShardContext context) {
        return delegateFieldType.termQueryCaseInsensitive(value, context);
    }

    @Override
    public Query fuzzyQuery(
        Object value,
        Fuzziness fuzziness,
        int prefixLength,
        int maxExpansions,
        boolean transpositions,
        MultiTermQuery.RewriteMethod method,
        QueryShardContext context
    ) {
        return delegateFieldType.fuzzyQuery(value, fuzziness, prefixLength, maxExpansions, transpositions, method, context);
    }

    @Override
    public Query wildcardQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, QueryShardContext context) {
        return delegateFieldType.wildcardQuery(value, method, caseInsensitive, context);
    }

    @Override
    public Query regexpQuery(
        String value,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates,
        MultiTermQuery.RewriteMethod method,
        QueryShardContext context
    ) {
        return delegateFieldType.regexpQuery(value, syntaxFlags, matchFlags, maxDeterminizedStates, method, context);
    }

    @Override
    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
        return delegateFieldType.rangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, context);
    }

    @Override
    public Query termsQuery(List<?> values, QueryShardContext context) {
        return delegateFieldType.termsQuery(values, context);
    }

    @Override
    public Object valueForDisplay(Object value) {
        return delegateFieldType.valueForDisplay(value);
    }

    @Override
    public BytesRef indexedValueForSearch(Object value) {
        return delegateFieldType.indexedValueForSearch(value);
    }
}
