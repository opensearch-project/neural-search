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
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
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
            10
        );
        HybridQuery query2 = new HybridQuery(
            List.of(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext)),
            10
        );
        HybridQuery query3 = new HybridQuery(
            List.of(
                QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext),
                QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_ANOTHER_QUERY_TEXT).toQuery(mockQueryShardContext)
            ),
            10
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
        assertEquals(10, query3.getPaginationDepth());
    }

    @SneakyThrows
    public void testRewrite_whenRewriteQuery_thenSuccessful() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        String field1Value = "text1";

        Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), RandomizedTest.randomAsciiAlphanumOfLength(8), ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), RandomizedTest.randomAsciiAlphanumOfLength(8), ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), RandomizedTest.randomAsciiAlphanumOfLength(8), ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);

        // Test with TermQuery
        HybridQuery hybridQueryWithTerm = new HybridQuery(
            List.of(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext)),
            10
        );
        Query rewritten = hybridQueryWithTerm.rewrite(reader);
        // term query is the same after we rewrite it
        assertSame(hybridQueryWithTerm, rewritten);

        // Test empty query list
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new HybridQuery(List.of(), 10));
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
        final IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, docId1, field1Value, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId2, field2Value, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId3, field3Value, ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader);

        HybridQuery query = new HybridQuery(
            List.of(new TermQuery(new Term(TEXT_FIELD_NAME, field1Value)), new TermQuery(new Term(TEXT_FIELD_NAME, field2Value))),
            10
        );
        // executing search query, getting up to 3 docs in result
        TopDocs hybridQueryResult = searcher.search(query, 3);

        assertNotNull(hybridQueryResult);
        assertEquals(2, hybridQueryResult.scoreDocs.length);
        List<Integer> expectedDocIds = List.of(docId1, docId2);
        List<Integer> actualDocIds = Arrays.stream(hybridQueryResult.scoreDocs).map(scoreDoc -> {
            try {
                return reader.document(scoreDoc.doc).getField("id").stringValue();
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
        final IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, docId1, field1Value, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId2, field2Value, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId3, field3Value, ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader);

        HybridQuery query = new HybridQuery(List.of(new TermQuery(new Term(TEXT_FIELD_NAME, QUERY_TEXT))), 10);
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
        final IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, docId1, field1Value, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId2, field2Value, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId3, field3Value, ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader);

        HybridQuery query = new HybridQuery(
            List.of(new TermQuery(new Term(TEXT_FIELD_NAME, QUERY_TEXT)), new TermQuery(new Term(TEXT_FIELD_NAME, QUERY_TEXT))),
            10
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
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new HybridQuery(List.of(), 10));
        assertThat(exception.getMessage(), containsString("collection of queries must not be empty"));
    }

    @SneakyThrows
    public void testWithRandomDocuments_whenPaginationDepthIsZero_thenFail() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new HybridQuery(
                List.of(new TermQuery(new Term(TEXT_FIELD_NAME, QUERY_TEXT)), new TermQuery(new Term(TEXT_FIELD_NAME, QUERY_TEXT))),
                0
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
            10
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
            10
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
            Query subQuery = booleanQuery.clauses().get(0).getQuery();
            assertTrue(subQuery instanceof TermQuery);
            Query filterQuery = booleanQuery.clauses().get(1).getQuery();
            assertTrue(filterQuery instanceof BooleanQuery);
            assertTrue(((BooleanQuery) filterQuery).clauses().get(0).getQuery() instanceof MatchNoDocsQuery);
            countOfQueries++;
        }
        assertEquals(2, countOfQueries);
    }
}
