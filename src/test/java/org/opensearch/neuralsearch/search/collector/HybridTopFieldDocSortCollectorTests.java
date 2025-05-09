/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.commons.lang3.RandomUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.neuralsearch.query.HybridSubQueryScorer;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;

public class HybridTopFieldDocSortCollectorTests extends HybridCollectorTestCase {
    static final String TEXT_FIELD_NAME = "field";
    static final String INT_FIELD_NAME = "integerField";
    static final String DOC_FIELD_NAME = "_doc";
    private static final int NUM_DOCS = 4;
    private static final int TOTAL_HITS_UP_TO = 1000;

    private static final int DOC_ID_1 = RandomUtils.nextInt(0, 100_000);
    private static final int DOC_ID_2 = RandomUtils.nextInt(0, 100_000);
    private static final int DOC_ID_3 = RandomUtils.nextInt(0, 100_000);
    private static final int DOC_ID_4 = RandomUtils.nextInt(0, 100_000);
    private static final String FIELD_1_VALUE = "text1";
    private static final String FIELD_2_VALUE = "text2";
    private static final String FIELD_3_VALUE = "text3";
    private static final String FIELD_4_VALUE = "text4";

    @SneakyThrows
    public void testSimpleFieldCollectorTopDocs_whenCreateNewAndGetTopDocs_thenSuccessful() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        List<Document> documents = new ArrayList<>();
        Document document1 = new Document();
        document1.add(new NumericDocValuesField("_id", DOC_ID_1));
        document1.add(new IntField(INT_FIELD_NAME, 100, Field.Store.YES));
        document1.add(new TextField(TEXT_FIELD_NAME, FIELD_1_VALUE, Field.Store.YES));
        documents.add(document1);
        Document document2 = new Document();
        document2.add(new NumericDocValuesField("_id", DOC_ID_2));
        document2.add(new IntField(INT_FIELD_NAME, 200, Field.Store.YES));
        document2.add(new TextField(TEXT_FIELD_NAME, FIELD_2_VALUE, Field.Store.YES));
        documents.add(document2);
        Document document3 = new Document();
        document3.add(new NumericDocValuesField("_id", DOC_ID_3));
        document3.add(new IntField(INT_FIELD_NAME, 300, Field.Store.YES));
        document3.add(new TextField(TEXT_FIELD_NAME, FIELD_3_VALUE, Field.Store.YES));
        documents.add(document3);
        Document document4 = new Document();
        document4.add(new NumericDocValuesField("_id", DOC_ID_4));
        document4.add(new IntField(INT_FIELD_NAME, 400, Field.Store.YES));
        document4.add(new TextField(TEXT_FIELD_NAME, FIELD_4_VALUE, Field.Store.YES));
        documents.add(document4);
        w.addDocuments(documents);
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);

        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);

        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        SortField sortField = new SortField(DOC_FIELD_NAME, SortField.Type.DOC);
        HybridTopFieldDocSortCollector hybridTopFieldDocSortCollector = new SimpleFieldCollector(
            NUM_DOCS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO),
            new Sort(sortField)
        );
        Weight weight = mock(Weight.class);
        hybridTopFieldDocSortCollector.setWeight(weight);
        LeafCollector leafCollector = hybridTopFieldDocSortCollector.getLeafCollector(leafReaderContext);
        assertNotNull(leafCollector);

        int[] docIdsForQuery = new int[] { DOC_ID_1, DOC_ID_2, DOC_ID_3, DOC_ID_4 };
        Arrays.sort(docIdsForQuery);
        final List<Float> scores = Stream.generate(() -> random().nextFloat()).limit(NUM_DOCS).collect(Collectors.toList());

        HybridSubQueryScorer mockHybridSubQueryScorer = mock(HybridSubQueryScorer.class);
        when(mockHybridSubQueryScorer.getNumOfSubQueries()).thenReturn(1);

        HybridSubQueryScorer scorer = new HybridSubQueryScorer(1);
        leafCollector.setScorer(scorer);

        collectDocsAndScores(scorer, scores, leafCollector, 0, docIdsForQuery);

        List<TopFieldDocs> topFieldDocs = hybridTopFieldDocSortCollector.topDocs();

        assertNotNull(topFieldDocs);
        assertEquals(1, topFieldDocs.size());
        for (TopFieldDocs topFieldDoc : topFieldDocs) {
            // assert results for each sub-query, there must be correct number of matches, all doc id are correct and scores must be desc
            // ordered
            assertEquals(4, topFieldDoc.totalHits.value());
            ScoreDoc[] scoreDocs = topFieldDoc.scoreDocs;
            assertNotNull(scoreDocs);
            assertEquals(4, scoreDocs.length);
            assertTrue(IntStream.range(0, scoreDocs.length - 1).noneMatch(idx -> scoreDocs[idx].doc > scoreDocs[idx + 1].doc));
            List<Integer> resultDocIds = Arrays.stream(scoreDocs).map(scoreDoc -> scoreDoc.doc).collect(Collectors.toList());
            assertTrue(Arrays.stream(docIdsForQuery).allMatch(resultDocIds::contains));
        }
        w.close();
        reader.close();
        directory.close();
    }

    @SneakyThrows
    public void testPagingFieldCollectorTopDocs_whenCreateNewAndGetTopDocs_thenSuccessful() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        List<Document> documents = new ArrayList<>();
        Document document1 = new Document();
        document1.add(new NumericDocValuesField("_id", DOC_ID_1));
        document1.add(new IntField(INT_FIELD_NAME, 100, Field.Store.YES));
        document1.add(new TextField(TEXT_FIELD_NAME, FIELD_1_VALUE, Field.Store.YES));
        documents.add(document1);
        Document document2 = new Document();
        document2.add(new NumericDocValuesField("_id", DOC_ID_2));
        document2.add(new IntField(INT_FIELD_NAME, 200, Field.Store.YES));
        document2.add(new TextField(TEXT_FIELD_NAME, FIELD_2_VALUE, Field.Store.YES));
        documents.add(document2);
        Document document3 = new Document();
        document3.add(new NumericDocValuesField("_id", DOC_ID_3));
        document3.add(new IntField(INT_FIELD_NAME, 300, Field.Store.YES));
        document3.add(new TextField(TEXT_FIELD_NAME, FIELD_3_VALUE, Field.Store.YES));
        documents.add(document3);
        Document document4 = new Document();
        document4.add(new NumericDocValuesField("_id", DOC_ID_4));
        document4.add(new IntField(INT_FIELD_NAME, 400, Field.Store.YES));
        document4.add(new TextField(TEXT_FIELD_NAME, FIELD_4_VALUE, Field.Store.YES));
        documents.add(document4);
        w.addDocuments(documents);
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);

        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);

        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        SortField sortField = new SortField(DOC_FIELD_NAME, SortField.Type.DOC);
        HybridTopFieldDocSortCollector hybridTopFieldDocSortCollector = new PagingFieldCollector(
            NUM_DOCS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO),
            new Sort(sortField),
            new FieldDoc(Integer.MAX_VALUE, 0.0f, new Object[] { DOC_ID_2 })
        );
        Weight weight = mock(Weight.class);
        hybridTopFieldDocSortCollector.setWeight(weight);
        LeafCollector leafCollector = hybridTopFieldDocSortCollector.getLeafCollector(leafReaderContext);
        assertNotNull(leafCollector);

        int[] docIdsForQuery = new int[] { DOC_ID_1, DOC_ID_2, DOC_ID_3, DOC_ID_4 };
        Arrays.sort(docIdsForQuery);
        int indexPositionOfDocId2 = Arrays.binarySearch(docIdsForQuery, DOC_ID_2);
        final List<Float> scores = Stream.generate(() -> random().nextFloat()).limit(NUM_DOCS).collect(Collectors.toList());

        HybridSubQueryScorer mockHybridSubQueryScorer = mock(HybridSubQueryScorer.class);
        when(mockHybridSubQueryScorer.getNumOfSubQueries()).thenReturn(1);
        HybridSubQueryScorer scorer = new HybridSubQueryScorer(1);
        leafCollector.setScorer(scorer);

        collectDocsAndScores(scorer, scores, leafCollector, 0, docIdsForQuery);

        List<TopFieldDocs> topFieldDocs = hybridTopFieldDocSortCollector.topDocs();

        assertNotNull(topFieldDocs);
        assertEquals(1, topFieldDocs.size());
        for (TopFieldDocs topFieldDoc : topFieldDocs) {
            // assert results for each sub-query, there must be correct number of matches, all doc id are correct and scores must be desc
            // ordered
            assertEquals(4 - (indexPositionOfDocId2 + 1), topFieldDoc.totalHits.value());
            ScoreDoc[] scoreDocs = topFieldDoc.scoreDocs;
            assertNotNull(scoreDocs);
            assertEquals(4 - (indexPositionOfDocId2 + 1), scoreDocs.length);
            assertTrue(IntStream.range(0, scoreDocs.length - 1).noneMatch(idx -> scoreDocs[idx].doc > scoreDocs[idx + 1].doc));
            List<Integer> resultDocIds = Arrays.stream(scoreDocs).map(scoreDoc -> scoreDoc.doc).collect(Collectors.toList());
            List<Integer> docIdsByQueryList = Arrays.stream(docIdsForQuery).boxed().collect(Collectors.toList());
            resultDocIds.stream().forEach(val -> assertTrue(docIdsByQueryList.contains(val)));
        }
        w.close();
        reader.close();
        directory.close();
    }
}
