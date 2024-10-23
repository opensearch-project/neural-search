/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import lombok.SneakyThrows;
import org.apache.lucene.search.Query;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.search.internal.SearchContext;

import java.util.List;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
            0
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

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT));
        hybridQueryBuilder.add(
            QueryBuilders.rangeQuery(RANGE_FIELD).from(FROM_TEXT).to(TO_TEXT).rewrite(mockQueryShardContext).rewrite(mockQueryShardContext)
        );

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
}
