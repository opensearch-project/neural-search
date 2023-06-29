/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search.query;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import lombok.SneakyThrows;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.opensearch.action.OriginalIndices;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.index.Index;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.shard.ShardId;
import org.opensearch.knn.index.mapper.KNNVectorFieldMapper;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.search.CompoundTopDocs;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryCollectorContext;
import org.opensearch.search.query.QuerySearchResult;

import com.carrotsearch.randomizedtesting.RandomizedTest;

public class HybridQueryPhaseSearcherTests extends OpenSearchQueryTestCase {
    private static final String VECTOR_FIELD_NAME = "vectorField";
    private static final String TEXT_FIELD_NAME = "field";
    private static final String TEST_DOC_TEXT1 = "Hello world";
    private static final String TEST_DOC_TEXT2 = "Hi to this place";
    private static final String TEST_DOC_TEXT3 = "We would like to welcome everyone";
    private static final String TEST_DOC_TEXT4 = "This is really nice place to be";
    private static final String QUERY_TEXT1 = "hello";
    private static final String QUERY_TEXT2 = "randomkeyword";
    private static final String QUERY_TEXT3 = "place";
    private static final Index dummyIndex = new Index("dummy", "dummy");
    private static final String MODEL_ID = "mfgfgdsfgfdgsde";
    private static final int K = 10;
    private static final QueryBuilder TEST_FILTER = new MatchAllQueryBuilder();

    @SneakyThrows
    public void testQueryType_whenQueryIsHybrid_thenCallHybridDocCollector() {
        HybridQueryPhaseSearcher hybridQueryPhaseSearcher = spy(new HybridQueryPhaseSearcher());
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        KNNVectorFieldMapper.KNNVectorFieldType mockKNNVectorField = mock(KNNVectorFieldMapper.KNNVectorFieldType.class);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        when(mockKNNVectorField.getDimension()).thenReturn(4);
        when(mockQueryShardContext.fieldMapper(eq(VECTOR_FIELD_NAME))).thenReturn(mockKNNVectorField);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT1, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT2, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT3, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);

        ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null
        );

        SearchContext searchContext = mock(SearchContext.class);
        ShardId shardId = new ShardId(dummyIndex, 1);
        SearchShardTarget shardTarget = new SearchShardTarget(
            randomAlphaOfLength(10),
            shardId,
            randomAlphaOfLength(10),
            OriginalIndices.NONE
        );
        when(searchContext.shardTarget()).thenReturn(shardTarget);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);

        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
        boolean hasFilterCollector = randomBoolean();
        boolean hasTimeout = randomBoolean();

        HybridQueryBuilder queryBuilder = new HybridQueryBuilder();

        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT1);
        queryBuilder.add(termSubQuery);

        Query query = queryBuilder.toQuery(mockQueryShardContext);
        when(searchContext.query()).thenReturn(query);

        hybridQueryPhaseSearcher.searchWith(searchContext, contextIndexSearcher, query, collectors, hasFilterCollector, hasTimeout);

        releaseResources(directory, w, reader);

        verify(hybridQueryPhaseSearcher, atLeastOnce()).searchWithCollector(any(), any(), any(), any(), anyBoolean(), anyBoolean());
    }

    @SneakyThrows
    public void testQueryType_whenQueryIsNotHybrid_thenDoNotCallHybridDocCollector() {
        HybridQueryPhaseSearcher hybridQueryPhaseSearcher = spy(new HybridQueryPhaseSearcher());
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT1, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT2, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT3, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);

        ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null
        );

        SearchContext searchContext = mock(SearchContext.class);
        ShardId shardId = new ShardId(dummyIndex, 1);
        SearchShardTarget shardTarget = new SearchShardTarget(
            randomAlphaOfLength(10),
            shardId,
            randomAlphaOfLength(10),
            OriginalIndices.NONE
        );
        when(searchContext.shardTarget()).thenReturn(shardTarget);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        when(searchContext.queryResult()).thenReturn(new QuerySearchResult());

        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
        boolean hasFilterCollector = randomBoolean();
        boolean hasTimeout = randomBoolean();

        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT1);

        Query query = termSubQuery.toQuery(mockQueryShardContext);
        when(searchContext.query()).thenReturn(query);

        hybridQueryPhaseSearcher.searchWith(searchContext, contextIndexSearcher, query, collectors, hasFilterCollector, hasTimeout);

        releaseResources(directory, w, reader);

        verify(hybridQueryPhaseSearcher, never()).searchWithCollector(any(), any(), any(), any(), anyBoolean(), anyBoolean());
    }

    @SneakyThrows
    public void testQueryResult_whenOneSubQueryWithHits_thenHybridResultsAreSet() {
        HybridQueryPhaseSearcher hybridQueryPhaseSearcher = spy(new HybridQueryPhaseSearcher());
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();
        int docId1 = RandomizedTest.randomInt();
        int docId2 = RandomizedTest.randomInt();
        int docId3 = RandomizedTest.randomInt();
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId1, TEST_DOC_TEXT1, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId2, TEST_DOC_TEXT2, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId3, TEST_DOC_TEXT3, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);

        ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null
        );

        SearchContext searchContext = mock(SearchContext.class);
        ShardId shardId = new ShardId(dummyIndex, 1);
        SearchShardTarget shardTarget = new SearchShardTarget(
            randomAlphaOfLength(10),
            shardId,
            randomAlphaOfLength(10),
            OriginalIndices.NONE
        );
        when(searchContext.shardTarget()).thenReturn(shardTarget);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        when(searchContext.size()).thenReturn(3);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        when(searchContext.queryResult()).thenReturn(querySearchResult);

        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
        boolean hasFilterCollector = randomBoolean();
        boolean hasTimeout = randomBoolean();

        HybridQueryBuilder queryBuilder = new HybridQueryBuilder();

        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT1);
        queryBuilder.add(termSubQuery);

        Query query = queryBuilder.toQuery(mockQueryShardContext);
        when(searchContext.query()).thenReturn(query);

        hybridQueryPhaseSearcher.searchWith(searchContext, contextIndexSearcher, query, collectors, hasFilterCollector, hasTimeout);

        assertNotNull(querySearchResult.topDocs());
        TopDocsAndMaxScore topDocsAndMaxScore = querySearchResult.topDocs();
        TopDocs topDocs = topDocsAndMaxScore.topDocs;
        assertEquals(1, topDocs.totalHits.value);
        assertTrue(topDocs instanceof CompoundTopDocs);
        List<TopDocs> compoundTopDocs = ((CompoundTopDocs) topDocs).getCompoundTopDocs();
        assertNotNull(compoundTopDocs);
        assertEquals(1, compoundTopDocs.size());
        TopDocs subQueryTopDocs = compoundTopDocs.get(0);
        assertEquals(1, subQueryTopDocs.totalHits.value);
        assertNotNull(subQueryTopDocs.scoreDocs);
        assertEquals(1, subQueryTopDocs.scoreDocs.length);
        ScoreDoc scoreDoc = subQueryTopDocs.scoreDocs[0];
        assertNotNull(scoreDoc);
        int actualDocId = Integer.parseInt(reader.document(scoreDoc.doc).getField("id").stringValue());
        assertEquals(docId1, actualDocId);
        assertTrue(scoreDoc.score > 0.0f);

        releaseResources(directory, w, reader);
    }

    @SneakyThrows
    public void testQueryResult_whenMultipleTextSubQueriesWithSomeHits_thenHybridResultsAreSet() {
        HybridQueryPhaseSearcher hybridQueryPhaseSearcher = new HybridQueryPhaseSearcher();
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();
        int docId1 = RandomizedTest.randomInt();
        int docId2 = RandomizedTest.randomInt();
        int docId3 = RandomizedTest.randomInt();
        int docId4 = RandomizedTest.randomInt();
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId1, TEST_DOC_TEXT1, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId2, TEST_DOC_TEXT2, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId3, TEST_DOC_TEXT3, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId4, TEST_DOC_TEXT4, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);

        ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null
        );

        SearchContext searchContext = mock(SearchContext.class);
        ShardId shardId = new ShardId(dummyIndex, 1);
        SearchShardTarget shardTarget = new SearchShardTarget(
            randomAlphaOfLength(10),
            shardId,
            randomAlphaOfLength(10),
            OriginalIndices.NONE
        );
        when(searchContext.shardTarget()).thenReturn(shardTarget);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        when(searchContext.size()).thenReturn(4);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        when(searchContext.queryResult()).thenReturn(querySearchResult);

        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
        boolean hasFilterCollector = randomBoolean();
        boolean hasTimeout = randomBoolean();

        HybridQueryBuilder queryBuilder = new HybridQueryBuilder();

        queryBuilder.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT1));
        queryBuilder.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT2));
        queryBuilder.add(QueryBuilders.matchAllQuery());

        Query query = queryBuilder.toQuery(mockQueryShardContext);
        when(searchContext.query()).thenReturn(query);

        hybridQueryPhaseSearcher.searchWith(searchContext, contextIndexSearcher, query, collectors, hasFilterCollector, hasTimeout);

        assertNotNull(querySearchResult.topDocs());
        TopDocsAndMaxScore topDocsAndMaxScore = querySearchResult.topDocs();
        TopDocs topDocs = topDocsAndMaxScore.topDocs;
        assertEquals(4, topDocs.totalHits.value);
        assertTrue(topDocs instanceof CompoundTopDocs);
        List<TopDocs> compoundTopDocs = ((CompoundTopDocs) topDocs).getCompoundTopDocs();
        assertNotNull(compoundTopDocs);
        assertEquals(3, compoundTopDocs.size());

        TopDocs subQueryTopDocs1 = compoundTopDocs.get(0);
        List<Integer> expectedIds1 = List.of(docId1);
        assertQueryResults(subQueryTopDocs1, expectedIds1, reader);

        TopDocs subQueryTopDocs2 = compoundTopDocs.get(1);
        List<Integer> expectedIds2 = List.of();
        assertQueryResults(subQueryTopDocs2, expectedIds2, reader);

        TopDocs subQueryTopDocs3 = compoundTopDocs.get(2);
        List<Integer> expectedIds3 = List.of(docId1, docId2, docId3, docId4);
        assertQueryResults(subQueryTopDocs3, expectedIds3, reader);

        releaseResources(directory, w, reader);
    }

    @SneakyThrows
    private void assertQueryResults(TopDocs subQueryTopDocs, List<Integer> expectedDocIds, IndexReader reader) {
        assertEquals(expectedDocIds.size(), subQueryTopDocs.totalHits.value);
        assertNotNull(subQueryTopDocs.scoreDocs);
        assertEquals(expectedDocIds.size(), subQueryTopDocs.scoreDocs.length);
        assertEquals(TotalHits.Relation.EQUAL_TO, subQueryTopDocs.totalHits.relation);
        for (int i = 0; i < expectedDocIds.size(); i++) {
            int expectedDocId = expectedDocIds.get(i);
            ScoreDoc scoreDoc = subQueryTopDocs.scoreDocs[i];
            assertNotNull(scoreDoc);
            int actualDocId = Integer.parseInt(reader.document(scoreDoc.doc).getField("id").stringValue());
            assertEquals(expectedDocId, actualDocId);
            assertTrue(scoreDoc.score > 0.0f);
        }
    }

    private void releaseResources(Directory directory, IndexWriter w, IndexReader reader) throws IOException {
        w.close();
        reader.close();
        directory.close();
    }
}
