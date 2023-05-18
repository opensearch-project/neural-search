/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.query.HybridQueryBuilderTests.QUERY_TEXT;
import static org.opensearch.neuralsearch.query.HybridQueryBuilderTests.TERM_QUERY_TEXT;
import static org.opensearch.neuralsearch.query.HybridQueryBuilderTests.TEXT_FIELD_NAME;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import lombok.SneakyThrows;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.search.QueryUtils;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;

import com.carrotsearch.randomizedtesting.RandomizedTest;

public class HybridQueryTests extends OpenSearchQueryTestCase {

    @SneakyThrows
    public void testBasics() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        MapperService mapperService = createMapperService(
            fieldMapping(
                b -> b.field("type", "text")
                    .field("fielddata", true)
                    .startObject("fielddata_frequency_filter")
                    .field("min", 2d)
                    .field("min_segment_size", 1000)
                    .endObject()
            )
        );
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) mapperService.fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        HybridQuery query1 = new HybridQuery(List.of());
        HybridQuery query2 = new HybridQuery(List.of());
        HybridQuery query3 = new HybridQuery(
            List.of(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext))
        );
        QueryUtils.check(query1);
        QueryUtils.checkEqual(query1, query2);
        QueryUtils.checkUnequal(query1, query3);
    }

    public void testRewrite() throws Exception {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        MapperService mapperService = createMapperService(
            fieldMapping(
                b -> b.field("type", "text")
                    .field("fielddata", true)
                    .startObject("fielddata_frequency_filter")
                    .field("min", 2d)
                    .field("min_segment_size", 1000)
                    .endObject()
            )
        );
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) mapperService.fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        IndexReader reader = mock(IndexReader.class);
        HybridQuery query = new HybridQuery(
            List.of(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext))
        );
        Query rewritten = query.rewrite(reader);
        QueryUtils.checkUnequal(query, rewritten);
        Query rewritten2 = rewritten.rewrite(reader);
        assertSame(rewritten, rewritten2);
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

        w.addDocument(getDocument(docId1, field1Value, ft));
        w.addDocument(getDocument(docId2, field2Value, ft));
        w.addDocument(getDocument(docId3, field3Value, ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader);

        HybridQuery query = new HybridQuery(
            List.of(new TermQuery(new Term(TEXT_FIELD_NAME, field1Value)), new TermQuery(new Term(TEXT_FIELD_NAME, field2Value)))
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

        w.addDocument(getDocument(docId1, field1Value, ft));
        w.addDocument(getDocument(docId2, field2Value, ft));
        w.addDocument(getDocument(docId3, field3Value, ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader);

        HybridQuery query = new HybridQuery(
            List.of(new TermQuery(new Term(TEXT_FIELD_NAME, QUERY_TEXT)), new TermQuery(new Term(TEXT_FIELD_NAME, QUERY_TEXT)))
        );
        // executing search query, getting up to 3 docs in result
        TopDocs hybridQueryResult = searcher.search(query, 3);

        assertNotNull(hybridQueryResult);
        assertEquals(0, hybridQueryResult.scoreDocs.length);
        w.close();
        reader.close();
        dir.close();
    }

    private static Document getDocument(int docId1, String field1Value, FieldType ft) {
        Document doc = new Document();
        doc.add(new TextField("id", Integer.toString(docId1), Field.Store.YES));
        doc.add(new Field(TEXT_FIELD_NAME, field1Value, ft));
        return doc;
    }
}
