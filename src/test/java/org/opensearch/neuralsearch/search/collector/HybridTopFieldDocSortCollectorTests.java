/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import java.io.IOException;
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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.neuralsearch.query.HybridQueryScorer;
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
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
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
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
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

    @SneakyThrows
    public void testSimpleFieldCollector_whenProfilerMode_thenResultsNotEmpty() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();

        // setup: index 4 documents
        List<Document> documents = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; i++) {
            Document doc = new Document();
            doc.add(new NumericDocValuesField("_id", i));
            doc.add(new IntField(INT_FIELD_NAME, (i + 1) * 100, Field.Store.YES));
            doc.add(new TextField(TEXT_FIELD_NAME, "text" + (i + 1), Field.Store.YES));
            documents.add(doc);
        }
        w.addDocuments(documents);
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);

        SortField sortField = new SortField(DOC_FIELD_NAME, SortField.Type.DOC);
        SimpleFieldCollector collector = new SimpleFieldCollector(
            NUM_DOCS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO),
            new Sort(sortField)
        );
        Weight weight = mock(Weight.class);
        collector.setWeight(weight);
        LeafCollector leafCollector = collector.getLeafCollector(leafReaderContext);

        // setup: profiler mode, where the leaf collector is handed a HybridQueryScorer instead of a
        // HybridSubQueryScorer and reads the per sub-query scores off it on every collect() call
        int[] docIds = IntStream.range(0, NUM_DOCS).toArray();
        float[] subQueryScores = new float[NUM_DOCS];
        for (int doc = 0; doc < NUM_DOCS; doc++) {
            subQueryScores[doc] = 1.0f + doc * 0.1f;
        }
        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(Arrays.asList(scorer(docIds, subQueryScores, weight)));
        leafCollector.setScorer(hybridQueryScorer);

        // execute: collect docs while iterating the hybrid scorer, the way the default bulk scorer does
        DocIdSetIterator iterator = hybridQueryScorer.iterator();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            leafCollector.collect(doc);
        }

        // verify: results should not be empty
        List<TopFieldDocs> topFieldDocs = collector.topDocs();
        assertNotNull(topFieldDocs);
        assertEquals(1, topFieldDocs.size());
        assertTrue("profiler mode should produce non-empty results", topFieldDocs.get(0).scoreDocs.length > 0);
        assertEquals(NUM_DOCS, topFieldDocs.get(0).totalHits.value());

        w.close();
        reader.close();
        directory.close();
    }

    @SneakyThrows
    public void testPagingFieldCollector_whenProfilerMode_thenResultsNotEmpty() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();

        List<Document> documents = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; i++) {
            Document doc = new Document();
            doc.add(new NumericDocValuesField("_id", i));
            doc.add(new IntField(INT_FIELD_NAME, (i + 1) * 100, Field.Store.YES));
            doc.add(new TextField(TEXT_FIELD_NAME, "text" + (i + 1), Field.Store.YES));
            documents.add(doc);
        }
        w.addDocuments(documents);
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);

        SortField sortField = new SortField(DOC_FIELD_NAME, SortField.Type.DOC);
        PagingFieldCollector collector = new PagingFieldCollector(
            NUM_DOCS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO),
            new Sort(sortField),
            new FieldDoc(Integer.MAX_VALUE, 0.0f, new Object[] { 0 })
        );
        Weight weight = mock(Weight.class);
        collector.setWeight(weight);
        LeafCollector leafCollector = collector.getLeafCollector(leafReaderContext);

        // setup: profiler mode, see testSimpleFieldCollector_whenProfilerMode_thenResultsNotEmpty
        int[] docIds = IntStream.range(0, NUM_DOCS).toArray();
        float[] subQueryScores = new float[NUM_DOCS];
        for (int doc = 0; doc < NUM_DOCS; doc++) {
            subQueryScores[doc] = 1.0f + doc * 0.1f;
        }
        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(Arrays.asList(scorer(docIds, subQueryScores, weight)));
        leafCollector.setScorer(hybridQueryScorer);

        // execute: collect all docs, the collector itself pages past the ones on the previous page
        DocIdSetIterator iterator = hybridQueryScorer.iterator();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            leafCollector.collect(doc);
        }

        // verify: results should not be empty
        List<TopFieldDocs> topFieldDocs = collector.topDocs();
        assertNotNull(topFieldDocs);
        assertEquals(1, topFieldDocs.size());
        assertTrue("profiler mode should produce non-empty results", topFieldDocs.get(0).scoreDocs.length > 0);

        w.close();
        reader.close();
        directory.close();
    }

    /**
     * A [_score desc, field asc] sort: every sub-query's window is ordered by that sub-query's OWN score (not by the sum of all
     * sub-query scores the scorer exposes), the SCORE sort value and the FieldDoc score are that own score — also for a document
     * that replaced a queue's bottom entry — and the trailing sort value is the field value.
     */
    @SneakyThrows
    public void testSimpleFieldCollector_whenScorePrimarySortWithTieBreaker_thenEachSubQueryOrderedByItsOwnScore() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        List<Document> documents = new ArrayList<>();
        int[] intValues = new int[] { 100, 200, 300, 400 };
        for (int i = 0; i < NUM_DOCS; i++) {
            Document document = new Document();
            document.add(new NumericDocValuesField("_id", i));
            // a single-valued NUMERIC doc value, which is what an INT SortField reads (IntField writes SORTED_NUMERIC)
            document.add(new NumericDocValuesField(INT_FIELD_NAME, intValues[i]));
            document.add(new TextField(TEXT_FIELD_NAME, "text" + i, Field.Store.YES));
            documents.add(document);
        }
        w.addDocuments(documents);
        w.commit();
        DirectoryReader reader = DirectoryReader.open(w);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);

        // two sub-queries, a window of two per sub-query so the third and fourth document go through the replace-bottom path
        Sort sort = new Sort(SortField.FIELD_SCORE, new SortField(INT_FIELD_NAME, SortField.Type.INT));
        HybridTopFieldDocSortCollector collector = new SimpleFieldCollector(2, new HitsThresholdChecker(TOTAL_HITS_UP_TO), sort);
        collector.setWeight(mock(Weight.class));
        LeafCollector leafCollector = collector.getLeafCollector(leafReaderContext);
        HybridSubQueryScorer scorer = new HybridSubQueryScorer(2);
        leafCollector.setScorer(scorer);

        // doc : sub-query 0 score, sub-query 1 score (0 = no match). The sums are 0.9, 0.9, 1.1, 0.9: a queue keyed on the sum
        // would keep doc 2 for sub-query 0 and could not tell docs 0, 1 and 3 apart.
        float[] subQuery0 = new float[] { 0.1f, 0.9f, 0.5f, 0.7f };
        float[] subQuery1 = new float[] { 0.8f, 0.0f, 0.6f, 0.2f };
        for (int doc = 0; doc < NUM_DOCS; doc++) {
            scorer.getSubQueryScores()[0] = subQuery0[doc];
            scorer.getSubQueryScores()[1] = subQuery1[doc];
            leafCollector.collect(doc);
            scorer.resetScores();
        }

        List<TopFieldDocs> topFieldDocs = collector.topDocs();
        assertEquals(2, topFieldDocs.size());
        // sub-query 0: doc 1 (0.9) then doc 3 (0.7); doc 3 replaced doc 2, which had replaced doc 0
        assertSubQueryWindow(topFieldDocs.get(0), new int[] { 1, 3 }, new float[] { 0.9f, 0.7f }, new int[] { 200, 400 });
        // sub-query 1: doc 0 (0.8) then doc 2 (0.6); doc 1 did not match it and doc 3 (0.2) was not competitive
        assertSubQueryWindow(topFieldDocs.get(1), new int[] { 0, 2 }, new float[] { 0.8f, 0.6f }, new int[] { 100, 300 });

        w.close();
        reader.close();
        directory.close();
    }

    /**
     * A document that is not competitive for one sub-query (whose window is full) is still offered to the other sub-queries:
     * the per-sub-query windows are independent.
     */
    @SneakyThrows
    public void testSimpleFieldCollector_whenDocNotCompetitiveForOneSubQuery_thenStillCollectedForTheOthers() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        List<Document> documents = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; i++) {
            Document document = new Document();
            document.add(new NumericDocValuesField("_id", i));
            document.add(new NumericDocValuesField(INT_FIELD_NAME, (i + 1) * 100));
            document.add(new TextField(TEXT_FIELD_NAME, "text" + i, Field.Store.YES));
            documents.add(document);
        }
        w.addDocuments(documents);
        w.commit();
        DirectoryReader reader = DirectoryReader.open(w);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);

        Sort sort = new Sort(SortField.FIELD_SCORE, new SortField(INT_FIELD_NAME, SortField.Type.INT));
        HybridTopFieldDocSortCollector collector = new SimpleFieldCollector(2, new HitsThresholdChecker(TOTAL_HITS_UP_TO), sort);
        collector.setWeight(mock(Weight.class));
        LeafCollector leafCollector = collector.getLeafCollector(leafReaderContext);
        HybridSubQueryScorer scorer = new HybridSubQueryScorer(2);
        leafCollector.setScorer(scorer);

        // docs 0 and 1 fill sub-query 0's window; docs 2 and 3 are not competitive there but are the only matches of sub-query 1
        float[] subQuery0 = new float[] { 0.9f, 0.8f, 0.1f, 0.7f };
        float[] subQuery1 = new float[] { 0.0f, 0.0f, 0.6f, 0.5f };
        for (int doc = 0; doc < NUM_DOCS; doc++) {
            scorer.getSubQueryScores()[0] = subQuery0[doc];
            scorer.getSubQueryScores()[1] = subQuery1[doc];
            leafCollector.collect(doc);
            scorer.resetScores();
        }

        List<TopFieldDocs> topFieldDocs = collector.topDocs();
        assertEquals(2, topFieldDocs.size());
        assertSubQueryWindow(topFieldDocs.get(0), new int[] { 0, 1 }, new float[] { 0.9f, 0.8f }, new int[] { 100, 200 });
        assertSubQueryWindow(topFieldDocs.get(1), new int[] { 2, 3 }, new float[] { 0.6f, 0.5f }, new int[] { 300, 400 });

        w.close();
        reader.close();
        directory.close();
    }

    /**
     * A field-sorted search_after page: a document that is not competitive for one sub-query (whose window is full) is still
     * offered to the other sub-queries.
     */
    @SneakyThrows
    public void testPagingFieldCollector_whenDocNotCompetitiveForOneSubQuery_thenStillCollectedForTheOthers() {
        // field values 100, 200, 300, 400; every document sorts after the cursor (50)
        PagingCollectorFixture fixture = pagingCollectorFixture(new int[] { 100, 200, 300, 400 }, 2);
        // docs 0 and 1 fill sub-query 0's window; docs 2 and 3 are not competitive there but are the only matches of sub-query 1
        fixture.collect(new float[] { 0.9f, 0.8f, 0.1f, 0.7f }, new float[] { 0.0f, 0.0f, 0.6f, 0.5f });

        List<TopFieldDocs> topFieldDocs = fixture.collector().topDocs();
        assertEquals(2, topFieldDocs.size());
        assertSubQueryWindow(topFieldDocs.get(0), new int[] { 0, 1 }, new float[] { 0.9f, 0.8f }, new int[] { 100, 200 });
        assertSubQueryWindow(topFieldDocs.get(1), new int[] { 2, 3 }, new float[] { 0.6f, 0.5f }, new int[] { 300, 400 });
        fixture.close();
    }

    /**
     * A field-sorted search_after page: a document that replaces the bottom of a full window keeps its own score, not the score
     * of the document it replaced.
     */
    @SneakyThrows
    public void testPagingFieldCollector_whenCompetitiveDocReplacesTheBottom_thenItKeepsItsOwnScore() {
        // field values descend with the doc id, so with an ascending sort docs 2 and 3 each replace the bottom of a window of two
        PagingCollectorFixture fixture = pagingCollectorFixture(new int[] { 400, 300, 200, 100 }, 2);
        fixture.collect(new float[] { 0.1f, 0.2f, 0.3f, 0.4f });

        List<TopFieldDocs> topFieldDocs = fixture.collector().topDocs();
        assertEquals(1, topFieldDocs.size());
        assertSubQueryWindow(topFieldDocs.get(0), new int[] { 3, 2 }, new float[] { 0.4f, 0.3f }, new int[] { 100, 200 });
        fixture.close();
    }

    /** A PagingFieldCollector sorted by the int field ascending, after the cursor value 50, over one segment of documents. */
    private PagingCollectorFixture pagingCollectorFixture(final int[] fieldValues, final int numHits) throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());
        List<Document> documents = new ArrayList<>();
        for (int i = 0; i < fieldValues.length; i++) {
            Document document = new Document();
            document.add(new NumericDocValuesField("_id", i));
            document.add(new NumericDocValuesField(INT_FIELD_NAME, fieldValues[i]));
            document.add(new TextField(TEXT_FIELD_NAME, "text" + i, Field.Store.YES));
            documents.add(document);
        }
        writer.addDocuments(documents);
        writer.commit();
        DirectoryReader reader = DirectoryReader.open(writer);
        PagingFieldCollector collector = new PagingFieldCollector(
            numHits,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO),
            new Sort(new SortField(INT_FIELD_NAME, SortField.Type.INT)),
            new FieldDoc(Integer.MAX_VALUE, 0.0f, new Object[] { 50 })
        );
        collector.setWeight(mock(Weight.class));
        return new PagingCollectorFixture(
            directory,
            writer,
            reader,
            collector,
            collector.getLeafCollector(reader.getContext().leaves().get(0))
        );
    }

    private record PagingCollectorFixture(Directory directory, IndexWriter writer, DirectoryReader reader, PagingFieldCollector collector,
        LeafCollector leafCollector) {

        /** Collects every document of the segment with the given score per sub-query (0 = no match). */
        void collect(final float[]... subQueryScores) throws IOException {
            HybridSubQueryScorer scorer = new HybridSubQueryScorer(subQueryScores.length);
            leafCollector.setScorer(scorer);
            for (int doc = 0; doc < subQueryScores[0].length; doc++) {
                for (int subQuery = 0; subQuery < subQueryScores.length; subQuery++) {
                    scorer.getSubQueryScores()[subQuery] = subQueryScores[subQuery][doc];
                }
                leafCollector.collect(doc);
                scorer.resetScores();
            }
        }

        void close() throws IOException {
            writer.close();
            reader.close();
            directory.close();
        }
    }

    /**
     * Each window entry: the doc, its own sub-query score, and the int field as the last sort value; with a _score-led sort the
     * first sort value is that score too.
     */
    private void assertSubQueryWindow(TopFieldDocs topFieldDocs, int[] expectedDocs, float[] expectedScores, int[] expectedFieldValues) {
        ScoreDoc[] scoreDocs = topFieldDocs.scoreDocs;
        boolean isScoreLed = topFieldDocs.fields[0].getType() == SortField.Type.SCORE;
        assertEquals(expectedDocs.length, scoreDocs.length);
        for (int i = 0; i < expectedDocs.length; i++) {
            FieldDoc fieldDoc = (FieldDoc) scoreDocs[i];
            assertEquals("doc at position " + i, expectedDocs[i], fieldDoc.doc);
            assertEquals("score of doc " + fieldDoc.doc, expectedScores[i], fieldDoc.score, 0.0001f);
            assertEquals(isScoreLed ? 2 : 1, fieldDoc.fields.length);
            if (isScoreLed) {
                assertEquals(
                    "_score sort value of doc " + fieldDoc.doc,
                    expectedScores[i],
                    ((Number) fieldDoc.fields[0]).floatValue(),
                    0.0001f
                );
            }
            assertEquals(
                "field sort value of doc " + fieldDoc.doc,
                expectedFieldValues[i],
                ((Number) fieldDoc.fields[fieldDoc.fields.length - 1]).intValue()
            );
        }
    }
}
