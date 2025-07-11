/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.query.HybridQueryBuilderTests.QUERY_TEXT;
import static org.opensearch.neuralsearch.query.HybridQueryBuilderTests.TEXT_FIELD_NAME;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.QueryUtils;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import lombok.SneakyThrows;

public class HybridQueryTests extends OpenSearchQueryTestCase {

    static final String TERM_QUERY_TEXT = "keyword";
    static final String TERM_ANOTHER_QUERY_TEXT = "anotherkeyword";
    @Mock
    protected ClusterService clusterService;
    private AutoCloseable openMocks;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        openMocks = MockitoAnnotations.openMocks(this);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        openMocks.close();
    }

    @SneakyThrows
    public void testQueryBasics_whenMultipleDifferentQueries_thenSuccessful() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        HybridQuery query1 = new HybridQuery(
            List.of(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext)),
            new HybridQueryContext(10)
        );
        HybridQuery query2 = new HybridQuery(
            List.of(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext)),
            new HybridQueryContext(10)
        );
        HybridQuery query3 = new HybridQuery(
            List.of(
                QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext),
                QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_ANOTHER_QUERY_TEXT).toQuery(mockQueryShardContext)
            ),
            new HybridQueryContext(10)
        );
        QueryUtils.check(query1);
        QueryUtils.checkEqual(query1, query2);
        QueryUtils.checkUnequal(query1, query3);

        Iterator<Query> queryIterator = query3.iterator();
        assertNotNull(queryIterator);
        int countOfQueries = 0;
        while (queryIterator.hasNext()) {
            Query query = queryIterator.next();
            assertNotNull(query);
            countOfQueries++;
        }
        assertEquals(2, countOfQueries);
        assertEquals(10, query3.getQueryContext().getPaginationDepth().intValue());
    }

    @SneakyThrows
    public void testRewrite_whenRewriteQuery_thenSuccessful() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        String field1Value = "text1";

        Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), RandomizedTest.randomAsciiAlphanumOfLength(8), ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), RandomizedTest.randomAsciiAlphanumOfLength(8), ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), RandomizedTest.randomAsciiAlphanumOfLength(8), ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);

        // Test with TermQuery
        HybridQuery hybridQueryWithTerm = new HybridQuery(
            List.of(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext)),
            new HybridQueryContext(10)
        );
        Query rewritten = hybridQueryWithTerm.rewrite(new IndexSearcher(reader));
        // term query is the same after we rewrite it
        assertSame(hybridQueryWithTerm, rewritten);

        // Test empty query list
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new HybridQuery(List.of(), new HybridQueryContext(10))
        );
        assertThat(exception.getMessage(), containsString("collection of queries must not be empty"));

        w.close();
        reader.close();
        directory.close();
    }

    @SneakyThrows
    public void testWithRandomDocuments_whenMultipleTermSubQueriesWithMatch_thenReturnSuccessfully() {
        int docId1 = RandomizedTest.randomInt();
        int docId2 = RandomizedTest.randomInt();
        int docId3 = RandomizedTest.randomInt();
        String field1Value = "text1";
        String field2Value = "text2";
        String field3Value = "text3";

        final Directory dir = newDirectory();
        final IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, docId1, field1Value, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId2, field2Value, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId3, field3Value, ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = new IndexSearcher(reader);

        HybridQuery query = new HybridQuery(
            List.of(new TermQuery(new Term(TEXT_FIELD_NAME, field1Value)), new TermQuery(new Term(TEXT_FIELD_NAME, field2Value))),
            new HybridQueryContext(10)
        );
        // executing search query, getting up to 3 docs in result
        TopDocs hybridQueryResult = searcher.search(query, 3);

        assertNotNull(hybridQueryResult);
        assertEquals(2, hybridQueryResult.scoreDocs.length);
        List<Integer> expectedDocIds = List.of(docId1, docId2);
        List<Integer> actualDocIds = Arrays.stream(hybridQueryResult.scoreDocs).map(scoreDoc -> {
            try {
                return reader.storedFields().document(scoreDoc.doc).getField("id").stringValue();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).map(Integer::parseInt).collect(Collectors.toList());
        assertEquals(actualDocIds, expectedDocIds);
        assertFalse(actualDocIds.contains(docId3));
        w.close();
        reader.close();
        dir.close();
    }

    @SneakyThrows
    public void testWithRandomDocuments_whenOneTermSubQueryWithoutMatch_thenReturnSuccessfully() {
        int docId1 = RandomizedTest.randomInt();
        int docId2 = RandomizedTest.randomInt();
        int docId3 = RandomizedTest.randomInt();
        String field1Value = "text1";
        String field2Value = "text2";
        String field3Value = "text3";

        final Directory dir = newDirectory();
        final IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, docId1, field1Value, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId2, field2Value, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId3, field3Value, ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = new IndexSearcher(reader);

        HybridQuery query = new HybridQuery(List.of(new TermQuery(new Term(TEXT_FIELD_NAME, QUERY_TEXT))), new HybridQueryContext(10));
        // executing search query, getting up to 3 docs in result
        TopDocs hybridQueryResult = searcher.search(query, 3);

        assertNotNull(hybridQueryResult);
        assertEquals(0, hybridQueryResult.scoreDocs.length);
        w.close();
        reader.close();
        dir.close();
    }

    @SneakyThrows
    public void testWithRandomDocuments_whenMultipleTermSubQueriesWithoutMatch_thenReturnSuccessfully() {
        int docId1 = RandomizedTest.randomInt();
        int docId2 = RandomizedTest.randomInt();
        int docId3 = RandomizedTest.randomInt();
        String field1Value = "text1";
        String field2Value = "text2";
        String field3Value = "text3";

        final Directory dir = newDirectory();
        final IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, docId1, field1Value, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId2, field2Value, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId3, field3Value, ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = new IndexSearcher(reader);

        HybridQuery query = new HybridQuery(
            List.of(new TermQuery(new Term(TEXT_FIELD_NAME, QUERY_TEXT)), new TermQuery(new Term(TEXT_FIELD_NAME, QUERY_TEXT))),
            new HybridQueryContext(10)
        );
        // executing search query, getting up to 3 docs in result
        TopDocs hybridQueryResult = searcher.search(query, 3);

        assertNotNull(hybridQueryResult);
        assertEquals(0, hybridQueryResult.scoreDocs.length);
        w.close();
        reader.close();
        dir.close();
    }

    @SneakyThrows
    public void testWithRandomDocuments_whenNoSubQueries_thenFail() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new HybridQuery(List.of(), new HybridQueryContext(10))
        );
        assertThat(exception.getMessage(), containsString("collection of queries must not be empty"));
    }

    @SneakyThrows
    public void testWithRandomDocuments_whenPaginationDepthIsZero_thenFail() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new HybridQuery(
                List.of(new TermQuery(new Term(TEXT_FIELD_NAME, QUERY_TEXT)), new TermQuery(new Term(TEXT_FIELD_NAME, QUERY_TEXT))),
                new HybridQueryContext(0)
            )
        );
        assertThat(exception.getMessage(), containsString("pagination_depth must not be zero"));
    }

    @SneakyThrows
    public void testToString_whenCallQueryToString_thenSuccessful() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        HybridQuery query = new HybridQuery(
            List.of(
                QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext),
                QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_ANOTHER_QUERY_TEXT).toQuery(mockQueryShardContext),
                new BoolQueryBuilder().should(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT))
                    .should(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_ANOTHER_QUERY_TEXT))
                    .toQuery(mockQueryShardContext)
            ),
            new HybridQueryContext(10)
        );

        String queryString = query.toString(TEXT_FIELD_NAME);
        assertEquals("(keyword | anotherkeyword | (keyword anotherkeyword))", queryString);
    }

    @SneakyThrows
    public void testFilter_whenSubQueriesWithFilterPassed_thenSuccessful() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Query filter = QueryBuilders.boolQuery().mustNot(QueryBuilders.matchQuery(TERM_QUERY_TEXT, "test")).toQuery(mockQueryShardContext);

        HybridQuery hybridQuery = new HybridQuery(
            List.of(
                QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext),
                QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_ANOTHER_QUERY_TEXT).toQuery(mockQueryShardContext)
            ),
            List.of(filter),
            new HybridQueryContext(10)
        );
        QueryUtils.check(hybridQuery);

        Iterator<Query> queryIterator = hybridQuery.iterator();
        assertNotNull(queryIterator);
        int countOfQueries = 0;
        while (queryIterator.hasNext()) {
            Query query = queryIterator.next();
            assertNotNull(query);
            assertTrue(query instanceof BooleanQuery);
            BooleanQuery booleanQuery = (BooleanQuery) query;
            assertEquals(2, booleanQuery.clauses().size());
            Query subQuery = booleanQuery.clauses().get(0).query();
            assertTrue(subQuery instanceof TermQuery);
            Query filterQuery = booleanQuery.clauses().get(1).query();
            assertTrue(filterQuery instanceof BooleanQuery);
            assertTrue(((BooleanQuery) filterQuery).clauses().get(0).query() instanceof MatchNoDocsQuery);
            countOfQueries++;
        }
        assertEquals(2, countOfQueries);
    }

    @SneakyThrows
    public void testFromQueryExtendedWithDlsRulesBySecurityPlugin_whenNoFilters_thenSuccessful() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        List<Query> originHybridSubQueries = List.of(
            QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext),
            QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_ANOTHER_QUERY_TEXT).toQuery(mockQueryShardContext)
        );
        HybridQuery originHybridQuery = new HybridQuery(originHybridSubQueries, List.of(), new HybridQueryContext(10));

        Query dlsQueryNotTest = QueryBuilders.constantScoreQuery(
            QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(TEXT_FIELD_NAME, "test"))
        ).toQuery(mockQueryShardContext);

        Query dlsQueryNotSomething = QueryBuilders.constantScoreQuery(
            QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(TEXT_FIELD_NAME, "something"))
        ).toQuery(mockQueryShardContext);

        BooleanQuery hybridWrappedInDlsRules = new BooleanQuery.Builder().add(dlsQueryNotTest, BooleanClause.Occur.SHOULD)
            .add(dlsQueryNotSomething, BooleanClause.Occur.SHOULD)
            .add(originHybridQuery, BooleanClause.Occur.MUST)
            .setMinimumNumberShouldMatch(1)
            .build();

        HybridQuery hybridFromExtendedWithDlsRules = HybridQuery.fromQueryExtendedWithDlsRules(hybridWrappedInDlsRules, List.of());

        List<Query> subqueriesWithDlsRules = hybridFromExtendedWithDlsRules.getSubQueries().stream().toList();
        assertEquals(originHybridSubQueries.size(), subqueriesWithDlsRules.size());
        assertEquals(
            originHybridQuery.getQueryContext().getPaginationDepth(),
            hybridFromExtendedWithDlsRules.getQueryContext().getPaginationDepth()
        );

        for (int i = 0; i < originHybridSubQueries.size(); i++) {
            Query subqueryWithDls = subqueriesWithDlsRules.get(i);
            assertTrue(subqueryWithDls instanceof BooleanQuery);
            BooleanQuery booleanWithDls = (BooleanQuery) subqueryWithDls;
            assertEquals(hybridWrappedInDlsRules.clauses().size(), booleanWithDls.clauses().size());
            assertEquals(hybridWrappedInDlsRules.getMinimumNumberShouldMatch(), booleanWithDls.getMinimumNumberShouldMatch());
            assertEquals(BooleanClause.Occur.MUST, booleanWithDls.clauses().get(0).occur());
            QueryUtils.checkEqual(booleanWithDls.clauses().get(0).query(), originHybridSubQueries.get(i));
            assertEquals(BooleanClause.Occur.SHOULD, booleanWithDls.clauses().get(1).occur());
            QueryUtils.checkEqual(booleanWithDls.clauses().get(1).query(), dlsQueryNotTest);
            assertEquals(BooleanClause.Occur.SHOULD, booleanWithDls.clauses().get(2).occur());
            QueryUtils.checkEqual(booleanWithDls.clauses().get(2).query(), dlsQueryNotSomething);
        }
    }

    @SneakyThrows
    public void testFromQueryExtendedWithDlsRulesBySecurityPlugin_whenFiltersPassed_thenSuccessful() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        List<Query> originHybridSubQueries = List.of(
            QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext),
            QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_ANOTHER_QUERY_TEXT).toQuery(mockQueryShardContext)
        );
        HybridQuery originHybridQuery = new HybridQuery(originHybridSubQueries, List.of(), new HybridQueryContext(10));

        Query dlsQueryNotTest = QueryBuilders.constantScoreQuery(
            QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(TEXT_FIELD_NAME, "test"))
        ).toQuery(mockQueryShardContext);

        BooleanQuery hybridWrappedInDlsRules = new BooleanQuery.Builder().add(dlsQueryNotTest, BooleanClause.Occur.SHOULD)
            .add(originHybridQuery, BooleanClause.Occur.MUST)
            .setMinimumNumberShouldMatch(1)
            .build();

        BooleanClause filterNotSomething = new BooleanClause(
            QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(TEXT_FIELD_NAME, "something")).toQuery(mockQueryShardContext),
            BooleanClause.Occur.FILTER
        );

        HybridQuery hybridFromExtendedWithDlsRules = HybridQuery.fromQueryExtendedWithDlsRules(
            hybridWrappedInDlsRules,
            List.of(filterNotSomething)
        );

        List<Query> subqueriesWithDlsRules = hybridFromExtendedWithDlsRules.getSubQueries().stream().toList();
        assertEquals(originHybridSubQueries.size(), subqueriesWithDlsRules.size());
        assertEquals(
            originHybridQuery.getQueryContext().getPaginationDepth(),
            hybridFromExtendedWithDlsRules.getQueryContext().getPaginationDepth()
        );

        for (int i = 0; i < originHybridSubQueries.size(); i++) {
            Query subqueryWithDls = subqueriesWithDlsRules.get(i);
            assertTrue(subqueryWithDls instanceof BooleanQuery);
            BooleanQuery booleanWithDls = (BooleanQuery) subqueryWithDls;
            assertEquals(3, booleanWithDls.clauses().size());
            assertEquals(hybridWrappedInDlsRules.getMinimumNumberShouldMatch(), booleanWithDls.getMinimumNumberShouldMatch());
            assertEquals(BooleanClause.Occur.MUST, booleanWithDls.clauses().get(0).occur());
            QueryUtils.checkEqual(booleanWithDls.clauses().get(0).query(), originHybridSubQueries.get(i));
            assertEquals(BooleanClause.Occur.SHOULD, booleanWithDls.clauses().get(1).occur());
            QueryUtils.checkEqual(booleanWithDls.clauses().get(1).query(), dlsQueryNotTest);
            assertEquals(BooleanClause.Occur.FILTER, booleanWithDls.clauses().get(2).occur());
            QueryUtils.checkEqual(booleanWithDls.clauses().get(2).query(), filterNotSomething.query());
        }
    }

    @SneakyThrows
    public void testFromQueryExtendedWithDlsRulesBySecurityPlugin_whenBooleanQueryWithNoHybridClause_thenFail() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> HybridQuery.fromQueryExtendedWithDlsRules(
                new BooleanQuery.Builder().add(new TermQuery(new Term(TEXT_FIELD_NAME, QUERY_TEXT)), BooleanClause.Occur.SHOULD).build(),
                List.of()
            )
        );
        assertThat(exception.getMessage(), containsString("Given boolean query does not contain a HybridQuery clause"));
    }
}
