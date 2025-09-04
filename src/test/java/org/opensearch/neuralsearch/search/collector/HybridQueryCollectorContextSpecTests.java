/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.neuralsearch.query.HybridQueryContext;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.search.query.HybridCollectorManager;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HybridQueryCollectorContextSpecTests extends OpenSearchQueryTestCase {
    static final String TEXT_FIELD_NAME = "field";
    static final String TERM_QUERY_TEXT = "keyword";
    private IndexReader indexReader;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        IndexObjects indexObjects = createIndexObjects(2);
        indexReader = indexObjects.indexReader();
    }

    public void testCreateManagerAndCreateCollector() throws IOException {
        SearchContext searchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT);
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().paginationDepth(10).build();
        HybridQuery hybridQuery = new HybridQuery(List.of(termSubQuery.toQuery(mockQueryShardContext)), hybridQueryContext);

        when(searchContext.query()).thenReturn(hybridQuery);
        MapperService mapperService = mock(MapperService.class);
        when(searchContext.mapperService()).thenReturn(mapperService);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);

        HybridQueryCollectorContextSpec hybridQueryCollectorContextSpec = new HybridQueryCollectorContextSpec(searchContext, hybridQuery);
        CollectorManager collectorManager = mock(CollectorManager.class);
        CollectorManager hybridCollectorManager = hybridQueryCollectorContextSpec.createManager(collectorManager);
        assertTrue(hybridCollectorManager instanceof HybridCollectorManager);

        Collector collector = mock(Collector.class);
        Collector hybridCollector = hybridQueryCollectorContextSpec.create(collector);
        assertTrue(hybridCollector instanceof HybridTopScoreDocCollector);

        assertEquals("search_top_hits", hybridQueryCollectorContextSpec.getContextName());
    }

    private record IndexObjects(IndexReader indexReader, Directory directory, IndexWriter writer) {
    }

    private IndexObjects createIndexObjects(int numDocs) throws IOException {
        Directory directory = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig());

        // Add the specified number of documents
        for (int i = 1; i <= numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("field", "value" + i, Field.Store.YES));
            writer.addDocument(doc);
        }

        writer.commit();
        IndexReader indexReader = DirectoryReader.open(writer);

        return new IndexObjects(indexReader, directory, writer);
    }
}
