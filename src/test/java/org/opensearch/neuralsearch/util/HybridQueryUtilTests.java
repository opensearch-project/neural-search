/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import lombok.SneakyThrows;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchType;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryContext;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.slice.TermsSliceQuery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.index.remote.RemoteStoreEnums.PathType.HASHED_PREFIX;
import static org.opensearch.neuralsearch.util.HybridQueryUtil.transformHybridQueryWrappedInBooleanMustQuery;

public class HybridQueryUtilTests extends OpenSearchQueryTestCase {

    private static final String TERM_QUERY_TEXT = "keyword";
    private static final String RANGE_FIELD = "date_range";
    private static final String FROM_TEXT = "123";
    private static final String TO_TEXT = "456";
    private static final String TEXT_FIELD_NAME = "field";

    @SneakyThrows
    public void testValidateHybridQuerySearchType_whenDfsQueryThenFetch_thenFail() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> HybridQueryUtil.validateHybridQuerySearchType(SearchType.DFS_QUERY_THEN_FETCH)
        );
        assertThat(exception.getMessage(), containsString("hybrid query does not support search_type [dfs_query_then_fetch]"));
    }

    public void testValidateHybridQuerySearchType_whenQueryThenFetch_thenSuccess() {
        HybridQueryUtil.validateHybridQuerySearchType(SearchType.QUERY_THEN_FETCH);
    }

    public void testValidateHybridQuerySearchType_whenDefaultSearchType_thenSuccess() {
        HybridQueryUtil.validateHybridQuerySearchType(SearchType.DEFAULT);
    }

    @SneakyThrows
    public void testIsHybridQueryCheck_whenQueryIsHybridQueryInstance_thenSuccess() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        when(mockQueryShardContext.convertToShardContext()).thenReturn(mockQueryShardContext);
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().paginationDepth(10).build();

        HybridQuery query = new HybridQuery(
            List.of(
                QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext),
                QueryBuilders.rangeQuery(RANGE_FIELD)
                    .from(FROM_TEXT)
                    .to(TO_TEXT)
                    .rewrite(mockQueryShardContext)
                    .rewrite(mockQueryShardContext)
                    .toQuery(mockQueryShardContext),
                QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext)
            ),
            hybridQueryContext
        );
        SearchContext searchContext = mock(SearchContext.class);

        assertTrue(HybridQueryUtil.isHybridQuery(query, searchContext));
    }

    @SneakyThrows
    public void testIsHybridQueryCheck_whenHybridWrappedIntoBoolAndNoNested_thenSuccess() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        MapperService mapperService = createMapperService();
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) mapperService.fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        IndexMetadata indexMetadata = getIndexMetadata();
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, Integer.toString(1)).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        when(mockQueryShardContext.getIndexSettings()).thenReturn(indexSettings);
        when(mockQueryShardContext.convertToShardContext()).thenReturn(mockQueryShardContext);

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT));
        hybridQueryBuilder.add(
            QueryBuilders.rangeQuery(RANGE_FIELD).from(FROM_TEXT).to(TO_TEXT).rewrite(mockQueryShardContext).rewrite(mockQueryShardContext)
        );
        hybridQueryBuilder.paginationDepth(10);
        Query booleanQuery = QueryBuilders.boolQuery()
            .should(hybridQueryBuilder)
            .should(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT))
            .toQuery(mockQueryShardContext);
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.mapperService()).thenReturn(mapperService);

        assertFalse(HybridQueryUtil.isHybridQuery(booleanQuery, searchContext));
    }

    @SneakyThrows
    public void testIsHybridQueryCheck_whenNoHybridQuery_thenSuccess() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        MapperService mapperService = createMapperService();
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) mapperService.fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        when(mockQueryShardContext.convertToShardContext()).thenReturn(mockQueryShardContext);

        Query booleanQuery = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT))
            .should(
                QueryBuilders.rangeQuery(RANGE_FIELD)
                    .from(FROM_TEXT)
                    .to(TO_TEXT)
                    .rewrite(mockQueryShardContext)
                    .rewrite(mockQueryShardContext)
            )
            .toQuery(mockQueryShardContext);
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.mapperService()).thenReturn(mapperService);

        assertFalse(HybridQueryUtil.isHybridQuery(booleanQuery, searchContext));
    }

    @SneakyThrows
    public void testIsHybridQueryCheck_whenHybridWrappedIntoBoolWithDlsRules_thenSuccess() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        MapperService mapperService = createMapperService();
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) mapperService.fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        IndexMetadata indexMetadata = getIndexMetadata();
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, Integer.toString(1)).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        when(mockQueryShardContext.getIndexSettings()).thenReturn(indexSettings);
        when(mockQueryShardContext.convertToShardContext()).thenReturn(mockQueryShardContext);

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT));
        hybridQueryBuilder.add(
            QueryBuilders.rangeQuery(RANGE_FIELD).from(FROM_TEXT).to(TO_TEXT).rewrite(mockQueryShardContext).rewrite(mockQueryShardContext)
        );
        hybridQueryBuilder.paginationDepth(10);

        Query booleanQuery = new BooleanQuery.Builder().add(
            QueryBuilders.constantScoreQuery(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT)).toQuery(mockQueryShardContext),
            BooleanClause.Occur.SHOULD
        ).add(hybridQueryBuilder.toQuery(mockQueryShardContext), BooleanClause.Occur.MUST).build();
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.mapperService()).thenReturn(mapperService);

        assertTrue(HybridQueryUtil.isHybridQuery(booleanQuery, searchContext));
    }

    @SneakyThrows
    public void testIsHybridQueryWrappedInBooleanMustQueryWithFilters_whenSingleHybridMustClause_thenSuccessful() {
        List<BooleanClause> clauses = new ArrayList<>();
        assertNull(transformHybridQueryWrappedInBooleanMustQuery(clauses));
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        when(mockQueryShardContext.convertToShardContext()).thenReturn(mockQueryShardContext);
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder().add(new TermQueryBuilder("field", "text"));
        BooleanClause hybridMustClause = new BooleanClause(hybridQueryBuilder.toQuery(mockQueryShardContext), BooleanClause.Occur.MUST);
        clauses.add(hybridMustClause);
        BooleanClause filterClause = new BooleanClause(new TermQuery(new Term("field", "value")), BooleanClause.Occur.FILTER);
        clauses.add(filterClause);
        Query query = transformHybridQueryWrappedInBooleanMustQuery(clauses);
        assertNotNull(query);
        assert query instanceof BooleanQuery;
    }

    @SneakyThrows
    public void testIsHybridQueryWrappedInBooleanMustQueryWithMustNot_whenSingleHybridMustClause_thenSuccessful() {
        List<BooleanClause> clauses = new ArrayList<>();
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        when(mockQueryShardContext.convertToShardContext()).thenReturn(mockQueryShardContext);
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder().add(new TermQueryBuilder("field", "text"));
        BooleanClause hybridMustClause = new BooleanClause(hybridQueryBuilder.toQuery(mockQueryShardContext), BooleanClause.Occur.MUST);
        clauses.add(hybridMustClause);
        BooleanClause filterClause = new BooleanClause(new TermQuery(new Term("field", "value")), BooleanClause.Occur.MUST_NOT);
        clauses.add(filterClause);
        Query query = transformHybridQueryWrappedInBooleanMustQuery(clauses);
        assertNotNull(query);
        assert query instanceof BooleanQuery;
    }

    @SneakyThrows
    public void testIsHybridQueryWrappedInBooleanMustQuery_whenNoFiltersOrMustClause_thenSuccessful() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        when(mockQueryShardContext.convertToShardContext()).thenReturn(mockQueryShardContext);
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder().add(new TermQueryBuilder("field", "text"));

        BooleanClause hybridMustClause = new BooleanClause(hybridQueryBuilder.toQuery(mockQueryShardContext), BooleanClause.Occur.MUST);
        List<BooleanClause> clauses = new ArrayList<>();
        clauses.add(hybridMustClause);
        Query query = transformHybridQueryWrappedInBooleanMustQuery(clauses);
        assertNull(query);
    }

    @SneakyThrows
    public void testExtractHybridQuery_whenShardIsNotPartOfSlice_thenEmptyHybridQueryWithSameNumberOfSubQueries() {
        HybridQuery hybridQuery = createHybridQuery();
        MatchNoDocsQuery shardQuery = new MatchNoDocsQuery("this shard is not part of the slice");
        SearchContext searchContext = mockSearchContext(shardQuery, hybridQuery);

        assertTrue(HybridQueryUtil.isHybridQuery(shardQuery, searchContext));
        Query extractedQuery = HybridQueryUtil.extractHybridQuery(searchContext, shardQuery);

        assertTrue(extractedQuery instanceof HybridQuery);
        HybridQuery extractedHybridQuery = (HybridQuery) extractedQuery;
        assertEquals(hybridQuery.getSubQueries().size(), extractedHybridQuery.getSubQueries().size());
        extractedHybridQuery.getSubQueries().forEach(subQuery -> assertSame(shardQuery, subQuery));
        assertSame(hybridQuery.getQueryContext(), extractedHybridQuery.getQueryContext());
    }

    @SneakyThrows
    public void testExtractHybridQuery_whenShardIsNotPartOfSliceAndDlsRules_thenEmptyHybridQueryWithSameNumberOfSubQueries() {
        HybridQuery hybridQuery = createHybridQuery();
        MatchNoDocsQuery shardQuery = new MatchNoDocsQuery("this shard is not part of the slice");
        SearchContext searchContext = mockSearchContext(shardQuery, extendWithDlsRules(hybridQuery));

        assertTrue(HybridQueryUtil.isHybridQuery(shardQuery, searchContext));
        Query extractedQuery = HybridQueryUtil.extractHybridQuery(searchContext, shardQuery);

        assertTrue(extractedQuery instanceof HybridQuery);
        HybridQuery extractedHybridQuery = (HybridQuery) extractedQuery;
        assertEquals(hybridQuery.getSubQueries().size(), extractedHybridQuery.getSubQueries().size());
        extractedHybridQuery.getSubQueries().forEach(subQuery -> assertSame(shardQuery, subQuery));
    }

    @SneakyThrows
    public void testExtractHybridQuery_whenWrappedWithSliceFilter_thenFiltersAddedToEachSubQuery() {
        HybridQuery hybridQuery = createHybridQuery();
        Query sliceFilter = new TermsSliceQuery("_id", 0, 2);
        Query otherFilter = new TermQuery(new Term(TEXT_FIELD_NAME, "value"));
        BooleanQuery shardQuery = new BooleanQuery.Builder().add(hybridQuery, BooleanClause.Occur.MUST)
            .add(sliceFilter, BooleanClause.Occur.FILTER)
            .add(otherFilter, BooleanClause.Occur.FILTER)
            .build();
        SearchContext searchContext = mockSearchContext(shardQuery, hybridQuery);

        assertTrue(HybridQueryUtil.isHybridQueryWrappedInBooleanQuery(searchContext, shardQuery));
        Query extractedQuery = HybridQueryUtil.extractHybridQuery(searchContext, shardQuery);

        assertTrue(extractedQuery instanceof HybridQuery);
        Collection<Query> subQueries = ((HybridQuery) extractedQuery).getSubQueries();
        assertEquals(hybridQuery.getSubQueries().size(), subQueries.size());
        for (Query subQuery : subQueries) {
            assertTrue(subQuery instanceof BooleanQuery);
            assertEquals(Set.of(sliceFilter, otherFilter), clausesOf((BooleanQuery) subQuery, BooleanClause.Occur.FILTER));
        }
    }

    @SneakyThrows
    public void testExtractHybridQuery_whenDlsRulesWrappedWithSliceFilter_thenDlsRulesAndFilterAddedToEachSubQuery() {
        HybridQuery hybridQuery = createHybridQuery();
        BooleanQuery queryWithDlsRules = extendWithDlsRules(hybridQuery);
        Query sliceFilter = new TermsSliceQuery("_id", 0, 2);
        BooleanQuery shardQuery = new BooleanQuery.Builder().add(queryWithDlsRules, BooleanClause.Occur.MUST)
            .add(sliceFilter, BooleanClause.Occur.FILTER)
            .build();
        SearchContext searchContext = mockSearchContext(shardQuery, queryWithDlsRules);

        assertTrue(HybridQueryUtil.isHybridQueryExtendedWithDlsRulesAndWrappedInBoolQuery(searchContext, shardQuery));
        Query extractedQuery = HybridQueryUtil.extractHybridQuery(searchContext, shardQuery);

        assertTrue(extractedQuery instanceof HybridQuery);
        Collection<Query> subQueries = ((HybridQuery) extractedQuery).getSubQueries();
        assertEquals(hybridQuery.getSubQueries().size(), subQueries.size());
        Set<Query> dlsRules = clausesOf(queryWithDlsRules, BooleanClause.Occur.SHOULD);
        for (Query subQuery : subQueries) {
            assertTrue(subQuery instanceof BooleanQuery);
            assertEquals(dlsRules, clausesOf((BooleanQuery) subQuery, BooleanClause.Occur.SHOULD));
            assertEquals(queryWithDlsRules.getMinimumNumberShouldMatch(), ((BooleanQuery) subQuery).getMinimumNumberShouldMatch());
            assertEquals(Set.of(sliceFilter), clausesOf((BooleanQuery) subQuery, BooleanClause.Occur.FILTER));
        }
    }

    @SneakyThrows
    public void testIsHybridQueryWrappedInBooleanQuery_whenParsedQueryIsNotHybridQuery_thenFalse() {
        HybridQuery hybridQuery = createHybridQuery();
        BooleanQuery booleanQuery = new BooleanQuery.Builder().add(hybridQuery, BooleanClause.Occur.MUST)
            .add(new TermQuery(new Term(TEXT_FIELD_NAME, "value")), BooleanClause.Occur.FILTER)
            .build();
        SearchContext searchContext = mockSearchContext(booleanQuery, booleanQuery);

        assertFalse(HybridQueryUtil.isHybridQueryWrappedInBooleanQuery(searchContext, booleanQuery));
    }

    @SneakyThrows
    public void testIsHybridQueryWrappedInBooleanQuery_whenClauseOtherThanFilterAfterHybridQuery_thenFalse() {
        HybridQuery hybridQuery = createHybridQuery();
        BooleanQuery shardQuery = new BooleanQuery.Builder().add(hybridQuery, BooleanClause.Occur.MUST)
            .add(new TermQuery(new Term(TEXT_FIELD_NAME, "value")), BooleanClause.Occur.MUST_NOT)
            .build();
        SearchContext searchContext = mockSearchContext(shardQuery, hybridQuery);

        assertFalse(HybridQueryUtil.isHybridQueryWrappedInBooleanQuery(searchContext, shardQuery));
    }

    @SneakyThrows
    public void testExtractHybridQuery_whenShardQueryDoesNotContainHybridQueryInSupportedForm_thenFail() {
        HybridQuery hybridQuery = createHybridQuery();
        BooleanQuery shardQuery = new BooleanQuery.Builder().add(hybridQuery, BooleanClause.Occur.MUST)
            .add(new TermQuery(new Term(TEXT_FIELD_NAME, "value")), BooleanClause.Occur.SHOULD)
            .build();
        SearchContext searchContext = mockSearchContext(shardQuery, hybridQuery);

        assertTrue(HybridQueryUtil.isHybridQuery(shardQuery, searchContext));
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> HybridQueryUtil.extractHybridQuery(searchContext, shardQuery)
        );
        assertEquals(
            "hybrid query cannot be executed, the query on this shard is a [BooleanQuery] that wraps the hybrid query "
                + "in a form that is not supported",
            exception.getMessage()
        );
    }

    @SneakyThrows
    public void testIsHybridQueryWrappedInBooleanQuery_whenNoParsedQuery_thenFalse() {
        HybridQuery hybridQuery = createHybridQuery();
        BooleanQuery shardQuery = new BooleanQuery.Builder().add(hybridQuery, BooleanClause.Occur.MUST)
            .add(new TermsSliceQuery("_id", 0, 2), BooleanClause.Occur.FILTER)
            .build();
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.query()).thenReturn(shardQuery);
        MapperService mapperService = createMapperService();
        when(searchContext.mapperService()).thenReturn(mapperService);

        assertFalse(HybridQueryUtil.isHybridQueryWrappedInBooleanQuery(searchContext, shardQuery));
    }

    @SneakyThrows
    public void testIsHybridQueryWrappedInBooleanQuery_whenHybridQueryIsNotFirstClause_thenFalse() {
        HybridQuery hybridQuery = createHybridQuery();
        BooleanQuery shardQuery = new BooleanQuery.Builder().add(new TermsSliceQuery("_id", 0, 2), BooleanClause.Occur.FILTER)
            .add(hybridQuery, BooleanClause.Occur.MUST)
            .build();
        SearchContext searchContext = mockSearchContext(shardQuery, hybridQuery);

        assertFalse(HybridQueryUtil.isHybridQueryWrappedInBooleanQuery(searchContext, shardQuery));
    }

    @SneakyThrows
    public void testIsHybridQueryWrappedInBooleanQuery_whenSingleClause_thenFalse() {
        HybridQuery hybridQuery = createHybridQuery();
        BooleanQuery shardQuery = new BooleanQuery.Builder().add(hybridQuery, BooleanClause.Occur.MUST).build();
        SearchContext searchContext = mockSearchContext(shardQuery, hybridQuery);

        assertFalse(HybridQueryUtil.isHybridQueryWrappedInBooleanQuery(searchContext, shardQuery));
    }

    @SneakyThrows
    public void testExtractHybridQuery_whenSearchContextHasNoQuery_thenGivenQueryReturned() {
        HybridQuery hybridQuery = createHybridQuery();
        SearchContext searchContext = mockSearchContext(null, hybridQuery);

        assertSame(hybridQuery, HybridQueryUtil.extractHybridQuery(searchContext, hybridQuery));
    }

    /**
     * The phase-results processors read top-level {@code hybrid.weights} through this accessor alone: present only for a
     * classic (no {@code fusion}) hybrid at the top of the request, empty for a fused one, a wrapped one, or no query.
     */
    public void testClassicRequestWeights_whenTopLevelClassicHybrid_thenPresentOtherwiseEmpty() {
        HybridQueryBuilder classic = new HybridQueryBuilder().weights(List.of(0.3f, 0.7f));
        classic.add(new TermQueryBuilder(TEXT_FIELD_NAME, TERM_QUERY_TEXT));
        classic.add(new TermQueryBuilder(TEXT_FIELD_NAME, "another"));
        assertEquals(
            Optional.of(List.of(0.3f, 0.7f)),
            HybridQueryUtil.classicRequestWeights(new SearchRequest("idx").source(new SearchSourceBuilder().query(classic)))
        );

        HybridQueryBuilder unweighted = new HybridQueryBuilder();
        unweighted.add(new TermQueryBuilder(TEXT_FIELD_NAME, TERM_QUERY_TEXT));
        assertEquals(
            Optional.empty(),
            HybridQueryUtil.classicRequestWeights(new SearchRequest("idx").source(new SearchSourceBuilder().query(unweighted)))
        );

        HybridQueryBuilder fused = new HybridQueryBuilder().weights(List.of(0.3f, 0.7f)).fusion(new HashMap<>());
        fused.add(new TermQueryBuilder(TEXT_FIELD_NAME, TERM_QUERY_TEXT));
        fused.add(new TermQueryBuilder(TEXT_FIELD_NAME, "another"));
        assertEquals(
            "a fused hybrid keeps its weights in its own FusionSpec",
            Optional.empty(),
            HybridQueryUtil.classicRequestWeights(new SearchRequest("idx").source(new SearchSourceBuilder().query(fused)))
        );

        assertEquals(
            Optional.empty(),
            HybridQueryUtil.classicRequestWeights(
                new SearchRequest("idx").source(new SearchSourceBuilder().query(QueryBuilders.boolQuery().must(classic)))
            )
        );
        assertEquals(Optional.empty(), HybridQueryUtil.classicRequestWeights(new SearchRequest("idx").source(new SearchSourceBuilder())));
        assertEquals(Optional.empty(), HybridQueryUtil.classicRequestWeights(new SearchRequest("idx")));
        assertEquals(Optional.empty(), HybridQueryUtil.classicRequestWeights(null));
    }

    private static HybridQuery createHybridQuery() {
        return new HybridQuery(
            List.of(new TermQuery(new Term(TEXT_FIELD_NAME, TERM_QUERY_TEXT)), new TermQuery(new Term(TEXT_FIELD_NAME, "another"))),
            HybridQueryContext.builder().paginationDepth(10).build()
        );
    }

    private static BooleanQuery extendWithDlsRules(final HybridQuery hybridQuery) {
        return new BooleanQuery.Builder().add(
            new ConstantScoreQuery(new TermQuery(new Term(TEXT_FIELD_NAME, "allowed"))),
            BooleanClause.Occur.SHOULD
        )
            .add(new ConstantScoreQuery(new TermQuery(new Term(TEXT_FIELD_NAME, "also_allowed"))), BooleanClause.Occur.SHOULD)
            .add(hybridQuery, BooleanClause.Occur.MUST)
            .setMinimumNumberShouldMatch(1)
            .build();
    }

    @SneakyThrows
    private SearchContext mockSearchContext(final Query shardQuery, final Query parsedQuery) {
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.query()).thenReturn(shardQuery);
        when(searchContext.parsedQuery()).thenReturn(new ParsedQuery(parsedQuery));
        MapperService mapperService = createMapperService();
        when(searchContext.mapperService()).thenReturn(mapperService);
        return searchContext;
    }

    private static Set<Query> clausesOf(final BooleanQuery booleanQuery, final BooleanClause.Occur occur) {
        return booleanQuery.clauses()
            .stream()
            .filter(clause -> clause.occur() == occur)
            .map(BooleanClause::query)
            .collect(Collectors.toSet());
    }

    private static IndexMetadata getIndexMetadata() {
        Map<String, String> remoteCustomData = Map.of(
            RemoteStoreEnums.PathType.NAME,
            HASHED_PREFIX.name(),
            RemoteStoreEnums.PathHashAlgorithm.NAME,
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64.name(),
            IndexMetadata.TRANSLOG_METADATA_KEY,
            "false"
        );
        Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder("test").settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putCustom(IndexMetadata.REMOTE_STORE_CUSTOM_KEY, remoteCustomData)
            .build();
        return indexMetadata;
    }
}
