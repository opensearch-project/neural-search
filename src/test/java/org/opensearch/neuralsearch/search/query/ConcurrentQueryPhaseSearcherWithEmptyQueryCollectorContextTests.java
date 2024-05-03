/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import lombok.SneakyThrows;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.opensearch.action.OriginalIndices;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryCollectorContext;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;
import java.util.LinkedList;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ConcurrentQueryPhaseSearcherWithEmptyQueryCollectorContextTests extends OpenSearchQueryTestCase {
    private static final String TEXT_FIELD_NAME = "field";
    private static final String TEST_DOC_TEXT1 = "Hello world";
    private static final String QUERY_TEXT1 = "hello";
    private static final Index dummyIndex = new Index("dummy", "dummy");

    @SneakyThrows
    public void testQueryResult_whenOneSubQueryWithHits_thenHybridResultsAreSet() {
        ConcurrentQueryPhaseSearcherWithEmptyQueryCollectorContext queryPhaseSearcher = spy(
            new ConcurrentQueryPhaseSearcherWithEmptyQueryCollectorContext()
        );
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        MapperService mapperService = createMapperService();
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) mapperService.fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();
        int docId1 = RandomizedTest.randomInt();
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId1, TEST_DOC_TEXT1, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        SearchContext searchContext = mock(SearchContext.class);

        ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null,
            searchContext
        );

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
        when(searchContext.numberOfShards()).thenReturn(1);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(new ShardId("test", "test", 0));
        when(searchContext.indexShard()).thenReturn(indexShard);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        when(searchContext.queryResult()).thenReturn(querySearchResult);
        when(searchContext.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
        when(searchContext.mapperService()).thenReturn(mapperService);

        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
        boolean hasFilterCollector = randomBoolean();
        boolean hasTimeout = randomBoolean();

        HybridQueryBuilder queryBuilder = new HybridQueryBuilder();

        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT1);
        queryBuilder.add(termSubQuery);

        Query query = queryBuilder.toQuery(mockQueryShardContext);
        when(searchContext.query()).thenReturn(query);
        queryPhaseSearcher.aggregationProcessor(searchContext).preProcess(searchContext);
        queryPhaseSearcher.searchWith(searchContext, contextIndexSearcher, query, collectors, hasFilterCollector, hasTimeout);

        assertTrue(querySearchResult.hasConsumedTopDocs());

        releaseResources(directory, w, reader);
    }

    private void releaseResources(Directory directory, IndexWriter w, IndexReader reader) throws IOException {
        w.close();
        reader.close();
        directory.close();
    }
}
