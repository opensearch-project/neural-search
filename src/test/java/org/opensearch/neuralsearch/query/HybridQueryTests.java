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
import org.opensearch.index.Index;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.knn.index.mapper.KNNVectorFieldMapper;
import org.opensearch.knn.index.query.KNNQueryBuilder;

import com.carrotsearch.randomizedtesting.RandomizedTest;

public class HybridQueryTests extends OpenSearchQueryTestCase {

    static final String VECTOR_FIELD_NAME = "vectorField";
    static final String TERM_QUERY_TEXT = "keyword";
    static final String TERM_ANOTHER_QUERY_TEXT = "anotherkeyword";
    static final float[] VECTOR_QUERY = new float[] { 1.0f, 2.0f, 2.1f, 0.6f };
    static final int K = 2;

    @SneakyThrows
    public void testBasics() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        HybridQuery query1 = new HybridQuery(
            List.of(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext))
        );
        HybridQuery query2 = new HybridQuery(
            List.of(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext))
        );
        HybridQuery query3 = new HybridQuery(
            List.of(
                QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext),
                QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_ANOTHER_QUERY_TEXT).toQuery(mockQueryShardContext)
            )
        );
        QueryUtils.check(query1);
        QueryUtils.checkEqual(query1, query2);
        QueryUtils.checkUnequal(query1, query3);
    }

    public void testRewrite() throws Exception {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        w.addDocument(getDocument(RandomizedTest.randomInt(), RandomizedTest.randomAsciiAlphanumOfLength(8), ft));
        w.addDocument(getDocument(RandomizedTest.randomInt(), RandomizedTest.randomAsciiAlphanumOfLength(8), ft));
        w.addDocument(getDocument(RandomizedTest.randomInt(), RandomizedTest.randomAsciiAlphanumOfLength(8), ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        HybridQuery hybridQueryWithTerm = new HybridQuery(
            List.of(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext))
        );
        Query rewritten = hybridQueryWithTerm.rewrite(reader);
        // term query is the same after we rewrite it
        assertSame(hybridQueryWithTerm, rewritten);

        Index dummyIndex = new Index("dummy", "dummy");
        KNNVectorFieldMapper.KNNVectorFieldType mockKNNVectorField = mock(KNNVectorFieldMapper.KNNVectorFieldType.class);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        when(mockKNNVectorField.getDimension()).thenReturn(4);
        when(mockQueryShardContext.fieldMapper(eq(VECTOR_FIELD_NAME))).thenReturn(mockKNNVectorField);
        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(VECTOR_FIELD_NAME, VECTOR_QUERY, K);
        Query knnQuery = knnQueryBuilder.toQuery(mockQueryShardContext);

        HybridQuery hybridQueryWithKnn = new HybridQuery(List.of(knnQuery));
        rewritten = hybridQueryWithKnn.rewrite(reader);
        assertSame(hybridQueryWithKnn, rewritten);

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

        w.addDocument(getDocument(docId1, field1Value, ft));
        w.addDocument(getDocument(docId2, field2Value, ft));
        w.addDocument(getDocument(docId3, field3Value, ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader);

        HybridQuery query = new HybridQuery(List.of(new TermQuery(new Term(TEXT_FIELD_NAME, QUERY_TEXT))));
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

    @SneakyThrows
    public void testWithRandomDocuments_whenNoSubQueries_thenFail() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new HybridQuery(List.of()));
        assertThat(exception.getMessage(), containsString("collection of queries must not be empty"));
    }

    private static Document getDocument(int docId1, String field1Value, FieldType ft) {
        Document doc = new Document();
        doc.add(new TextField("id", Integer.toString(docId1), Field.Store.YES));
        doc.add(new Field(TEXT_FIELD_NAME, field1Value, ft));
        return doc;
    }
}
