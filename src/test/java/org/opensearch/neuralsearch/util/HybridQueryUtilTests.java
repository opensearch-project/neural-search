/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import lombok.SneakyThrows;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryContext;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.index.remote.RemoteStoreEnums.PathType.HASHED_PREFIX;
import static org.opensearch.neuralsearch.util.HybridQueryUtil.isHybridQueryWrappedInBooleanMustQueryWithFilters;
import static org.opensearch.neuralsearch.util.HybridQueryUtil.isHybridQueryWrappedInBooleanQuery;

public class HybridQueryUtilTests extends OpenSearchQueryTestCase {

    private static final String TERM_QUERY_TEXT = "keyword";
    private static final String RANGE_FIELD = "date _range";
    private static final String FROM_TEXT = "123";
    private static final String TO_TEXT = "456";
    private static final String TEXT_FIELD_NAME = "field";

    @SneakyThrows
    public void testIsHybridQueryCheck_whenQueryIsHybridQueryInstance_thenSuccess() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
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
    public void testIsHybridQueryWrappedInBooleanMustQueryWithFilters_whenEmptyClauses_thenSuccessful() {
        List<BooleanClause> clauses = new ArrayList<>();
        assertFalse(isHybridQueryWrappedInBooleanMustQueryWithFilters(clauses));
    }

    @SneakyThrows
    public void testIsHybridQueryWrappedInBooleanMustQueryWithFilters_whenSingleHybridMustClause_thenSuccessful() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder().add(new TermQueryBuilder("field", "text"));
        BooleanClause hybridMustClause = new BooleanClause(hybridQueryBuilder.toQuery(mockQueryShardContext), BooleanClause.Occur.MUST);
        List<BooleanClause> clauses = Arrays.asList(hybridMustClause);

        assertTrue(isHybridQueryWrappedInBooleanMustQueryWithFilters(clauses));
    }

    @SneakyThrows
    public void testIsHybridQueryWrappedInBooleanMustQueryWithFilters_whenHybridMustWithValidFilters_thenSuccessful() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder().add(new TermQueryBuilder("field", "text"));

        BooleanClause hybridMustClause = new BooleanClause(hybridQueryBuilder.toQuery(mockQueryShardContext), BooleanClause.Occur.MUST);
        BooleanClause filterClause1 = new BooleanClause(new TermQuery(new Term("field", "value")), BooleanClause.Occur.FILTER);
        BooleanClause filterClause2 = new BooleanClause(new TermQuery(new Term("field2", "value2")), BooleanClause.Occur.FILTER);

        List<BooleanClause> clauses = Arrays.asList(hybridMustClause, filterClause1, filterClause2);

        assertTrue(isHybridQueryWrappedInBooleanMustQueryWithFilters(clauses));
    }

    @SneakyThrows
    public void testIsHybridQueryWrappedInBooleanMustQueryWithFilters_whenFirstClauseNotMust_thenSuccessful() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder().add(new TermQueryBuilder("field", "text"));

        BooleanClause hybridShouldClause = new BooleanClause(hybridQueryBuilder.toQuery(mockQueryShardContext), BooleanClause.Occur.SHOULD);
        BooleanClause filterClause = new BooleanClause(new TermQuery(new Term("field", "value")), BooleanClause.Occur.FILTER);

        List<BooleanClause> clauses = Arrays.asList(hybridShouldClause, filterClause);

        assertFalse(isHybridQueryWrappedInBooleanMustQueryWithFilters(clauses));
    }

    @SneakyThrows
    public void testIsHybridQueryWrappedInBooleanMustQueryWithFilters_whenFirstClauseNotHybrid_thenSuccessful() {
        BooleanClause termMustClause = new BooleanClause(new TermQuery(new Term("field", "value")), BooleanClause.Occur.MUST);
        BooleanClause filterClause = new BooleanClause(new TermQuery(new Term("field2", "value2")), BooleanClause.Occur.FILTER);

        List<BooleanClause> clauses = Arrays.asList(termMustClause, filterClause);

        assertFalse(isHybridQueryWrappedInBooleanMustQueryWithFilters(clauses));
    }

    @SneakyThrows
    public void testIsHybridQueryWrappedInBooleanMustQueryWithFilters_NonFilterSecondClause_thenSuccessful() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder().add(new TermQueryBuilder("field", "text"));

        BooleanClause hybridMustClause = new BooleanClause(hybridQueryBuilder.toQuery(mockQueryShardContext), BooleanClause.Occur.MUST);
        BooleanClause mustClause = new BooleanClause(new TermQuery(new Term("field", "value")), BooleanClause.Occur.MUST);

        List<BooleanClause> clauses = Arrays.asList(hybridMustClause, mustClause);

        assertFalse(isHybridQueryWrappedInBooleanMustQueryWithFilters(clauses));
    }

    @SneakyThrows
    public void testIsHybridQueryWrappedInBooleanQuery_whenHybridMustClauseAndFilterClauses_thenSuccessful() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder().add(new TermQueryBuilder("field", "text"));

        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
        booleanQueryBuilder.add(hybridQueryBuilder.toQuery(mockQueryShardContext), BooleanClause.Occur.MUST);
        booleanQueryBuilder.add(new TermQuery(new Term("filter_field", "filter_value")), BooleanClause.Occur.FILTER);
        BooleanQuery booleanQuery = booleanQueryBuilder.build();
        SearchContext mockSearchContext = mock(SearchContext.class);

        assertTrue(isHybridQueryWrappedInBooleanQuery(mockSearchContext, booleanQuery));
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
