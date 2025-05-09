/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.RandomUtils;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.opensearch.neuralsearch.query.HybridQueryScorer;
import org.opensearch.neuralsearch.query.HybridSubQueryScorer;

import lombok.SneakyThrows;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;

public class HybridTopScoreDocCollectorTests extends HybridCollectorTestCase {

    static final String TEXT_FIELD_NAME = "field";
    private static final int NUM_DOCS = 4;
    private static final int NUM_HITS = 1;
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
    public void testBasics_whenCreateNewCollector_thenSuccessful() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_1, FIELD_1_VALUE, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_2, FIELD_2_VALUE, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_3, FIELD_3_VALUE, ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);

        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);

        HybridTopScoreDocCollector hybridTopScoreDocCollector = new HybridTopScoreDocCollector(
            NUM_DOCS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );
        LeafCollector leafCollector = hybridTopScoreDocCollector.getLeafCollector(leafReaderContext);
        assertNotNull(leafCollector);

        assertEquals(ScoreMode.TOP_SCORES, hybridTopScoreDocCollector.scoreMode());

        Weight weight = mock(Weight.class);
        hybridTopScoreDocCollector.setWeight(weight);

        w.close();
        reader.close();
        directory.close();
    }

    @SneakyThrows
    public void testTopDocs_whenCreateNewAndGetTopDocs_thenSuccessful() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_1, FIELD_1_VALUE, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_2, FIELD_2_VALUE, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_3, FIELD_3_VALUE, ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);

        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);

        HybridTopScoreDocCollector hybridTopScoreDocCollector = new HybridTopScoreDocCollector(
            NUM_DOCS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );
        Weight weight = mock(Weight.class);
        hybridTopScoreDocCollector.setWeight(weight);
        LeafCollector leafCollector = hybridTopScoreDocCollector.getLeafCollector(leafReaderContext);
        assertNotNull(leafCollector);

        int[] docIds = new int[] { DOC_ID_1, DOC_ID_2, DOC_ID_3 };
        Arrays.sort(docIds);
        final List<Float> scores1 = Stream.generate(() -> random().nextFloat()).limit(NUM_DOCS).collect(Collectors.toList());
        final List<Float> scores2 = Stream.generate(() -> random().nextFloat()).limit(NUM_DOCS).collect(Collectors.toList());

        // Create mock HybridSubQueryScorer that extends Scorable
        HybridSubQueryScorer mockHybridSubQueryScorer = mock(HybridSubQueryScorer.class);
        when(mockHybridSubQueryScorer.getNumOfSubQueries()).thenReturn(2);

        HybridSubQueryScorer scorer = new HybridSubQueryScorer(2);
        leafCollector.setScorer(scorer);

        collectDocsAndScores(scorer, scores1, leafCollector, 0, docIds);
        collectDocsAndScores(scorer, scores2, leafCollector, 1, docIds);

        List<TopDocs> topDocs = hybridTopScoreDocCollector.topDocs();

        assertNotNull(topDocs);
        assertEquals(2, topDocs.size());

        for (TopDocs topDoc : topDocs) {
            // assert results for each sub-query, there must be correct number of matches, all doc id are correct and scores must be desc
            // ordered
            assertEquals(3, topDoc.totalHits.value());
            ScoreDoc[] scoreDocs = topDoc.scoreDocs;
            assertNotNull(scoreDocs);
            assertEquals(3, scoreDocs.length);
            assertTrue(IntStream.range(0, scoreDocs.length - 1).noneMatch(idx -> scoreDocs[idx].score < scoreDocs[idx + 1].score));
            List<Integer> resultDocIds = Arrays.stream(scoreDocs).map(scoreDoc -> scoreDoc.doc).collect(Collectors.toList());
            assertTrue(Arrays.stream(docIds).allMatch(resultDocIds::contains));
        }

        w.close();
        reader.close();
        directory.close();
    }

    @SneakyThrows
    public void testTopDocs_whenMatchedDocsDifferentForEachSubQuery_thenSuccessful() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_1, FIELD_1_VALUE, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_2, FIELD_2_VALUE, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_3, FIELD_3_VALUE, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_4, FIELD_4_VALUE, ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);

        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);
        HybridTopScoreDocCollector hybridTopScoreDocCollector = new HybridTopScoreDocCollector(
            NUM_DOCS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        hybridTopScoreDocCollector.setWeight(weight);

        LeafCollector leafCollector = hybridTopScoreDocCollector.getLeafCollector(leafReaderContext);
        assertNotNull(leafCollector);

        // Create mock HybridSubQueryScorer that extends Scorable
        HybridSubQueryScorer mockHybridSubQueryScorer = mock(HybridSubQueryScorer.class);
        when(mockHybridSubQueryScorer.getNumOfSubQueries()).thenReturn(4);

        HybridSubQueryScorer scorer = new HybridSubQueryScorer(4);
        leafCollector.setScorer(scorer);

        int[] docIdsQuery1 = new int[] { DOC_ID_1, DOC_ID_2, DOC_ID_3 };
        int[] docIdsQuery2 = new int[] { DOC_ID_4, DOC_ID_1 };
        int[] docIdsQueryMatchAll = new int[] { DOC_ID_1, DOC_ID_2, DOC_ID_3, DOC_ID_4 };
        Arrays.sort(docIdsQuery1);
        Arrays.sort(docIdsQuery2);
        Arrays.sort(docIdsQueryMatchAll);
        final List<Float> scores1 = Stream.generate(() -> random().nextFloat()).limit(docIdsQuery1.length).collect(Collectors.toList());
        final List<Float> scores2 = Stream.generate(() -> random().nextFloat()).limit(docIdsQuery2.length).collect(Collectors.toList());
        final List<Float> scoresMatchAll = Stream.generate(() -> random().nextFloat())
            .limit(docIdsQueryMatchAll.length)
            .collect(Collectors.toList());

        collectDocsAndScores(scorer, scores1, leafCollector, 0, docIdsQuery1);
        collectDocsAndScores(scorer, scores2, leafCollector, 1, docIdsQuery2);
        collectDocsAndScores(scorer, scoresMatchAll, leafCollector, 3, docIdsQueryMatchAll);

        List<TopDocs> topDocs = hybridTopScoreDocCollector.topDocs();

        assertNotNull(topDocs);
        assertEquals(4, topDocs.size());

        // assert result for each sub query
        // term query 1
        TopDocs topDocQuery1 = topDocs.get(0);
        assertEquals(docIdsQuery1.length, topDocQuery1.totalHits.value());
        ScoreDoc[] scoreDocsQuery1 = topDocQuery1.scoreDocs;
        assertNotNull(scoreDocsQuery1);
        assertEquals(docIdsQuery1.length, scoreDocsQuery1.length);
        assertTrue(
            IntStream.range(0, scoreDocsQuery1.length - 1).noneMatch(idx -> scoreDocsQuery1[idx].score < scoreDocsQuery1[idx + 1].score)
        );
        List<Integer> resultDocIdsQuery1 = Arrays.stream(scoreDocsQuery1).map(scoreDoc -> scoreDoc.doc).collect(Collectors.toList());
        assertTrue(Arrays.stream(docIdsQuery1).allMatch(resultDocIdsQuery1::contains));
        // term query 2
        TopDocs topDocQuery2 = topDocs.get(1);
        assertEquals(docIdsQuery2.length, topDocQuery2.totalHits.value());
        ScoreDoc[] scoreDocsQuery2 = topDocQuery2.scoreDocs;
        assertNotNull(scoreDocsQuery2);
        assertEquals(docIdsQuery2.length, scoreDocsQuery2.length);
        assertTrue(
            IntStream.range(0, scoreDocsQuery2.length - 1).noneMatch(idx -> scoreDocsQuery2[idx].score < scoreDocsQuery2[idx + 1].score)
        );
        List<Integer> resultDocIdsQuery2 = Arrays.stream(scoreDocsQuery2).map(scoreDoc -> scoreDoc.doc).collect(Collectors.toList());
        assertTrue(Arrays.stream(docIdsQuery2).allMatch(resultDocIdsQuery2::contains));
        // no match query
        TopDocs topDocQuery3 = topDocs.get(2);
        assertEquals(0, topDocQuery3.totalHits.value());
        ScoreDoc[] scoreDocsQuery3 = topDocQuery3.scoreDocs;
        assertNotNull(scoreDocsQuery3);
        assertEquals(0, scoreDocsQuery3.length);
        // all match query
        TopDocs topDocQueryMatchAll = topDocs.get(3);
        assertEquals(docIdsQueryMatchAll.length, topDocQueryMatchAll.totalHits.value());
        ScoreDoc[] scoreDocsQueryMatchAll = topDocQueryMatchAll.scoreDocs;
        assertNotNull(scoreDocsQueryMatchAll);
        assertEquals(docIdsQueryMatchAll.length, scoreDocsQueryMatchAll.length);
        assertTrue(
            IntStream.range(0, scoreDocsQueryMatchAll.length - 1)
                .noneMatch(idx -> scoreDocsQueryMatchAll[idx].score < scoreDocsQueryMatchAll[idx + 1].score)
        );
        List<Integer> resultDocIdsQueryMatchAll = Arrays.stream(scoreDocsQueryMatchAll)
            .map(scoreDoc -> scoreDoc.doc)
            .collect(Collectors.toList());
        assertTrue(Arrays.stream(docIdsQueryMatchAll).allMatch(resultDocIdsQueryMatchAll::contains));

        w.close();
        reader.close();
        directory.close();
    }

    @SneakyThrows
    public void testCompoundScorer_whenHybridScorerIsChildScorer_thenSuccessful() {
        HybridTopScoreDocCollector hybridTopScoreDocCollector = new HybridTopScoreDocCollector(
            NUM_DOCS,
            new HitsThresholdChecker(Integer.MAX_VALUE)
        );

        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_1, FIELD_1_VALUE, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_2, FIELD_2_VALUE, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_3, FIELD_3_VALUE, ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);

        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);
        LeafCollector leafCollector = hybridTopScoreDocCollector.getLeafCollector(leafReaderContext);

        assertNotNull(leafCollector);

        Scorer subQueryScorer = mock(Scorer.class);
        DocIdSetIterator iterator = mock(DocIdSetIterator.class);
        when(subQueryScorer.iterator()).thenReturn(iterator);

        int[] docIds = new int[] { DOC_ID_1, DOC_ID_2, DOC_ID_3 };
        Arrays.sort(docIds);
        final List<Float> scores = Stream.generate(() -> random().nextFloat()).limit(NUM_DOCS).collect(Collectors.toList());
        HybridSubQueryScorer hybridQueryScorer = new HybridSubQueryScorer(1);

        Scorer scorer = mock(Scorer.class);
        Collection<Scorable.ChildScorable> childrenCollectors = List.of(new Scorable.ChildScorable(hybridQueryScorer, "MUST"));
        when(scorer.getChildren()).thenReturn(childrenCollectors);
        leafCollector.setScorer(scorer);

        collectDocsAndScores(hybridQueryScorer, scores, leafCollector, 0, docIds);

        w.close();
        reader.close();
        directory.close();
    }

    @SneakyThrows
    public void testTrackTotalHits_whenTotalHitsSetIntegerMaxValue_thenSuccessful() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_1, FIELD_1_VALUE, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_2, FIELD_2_VALUE, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_3, FIELD_3_VALUE, ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);

        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);

        HybridTopScoreDocCollector hybridTopScoreDocCollector = new HybridTopScoreDocCollector(
            NUM_DOCS,
            new HitsThresholdChecker(Integer.MAX_VALUE)
        );
        LeafCollector leafCollector = hybridTopScoreDocCollector.getLeafCollector(leafReaderContext);
        assertNotNull(leafCollector);

        int[] docIds = new int[] { DOC_ID_1, DOC_ID_2, DOC_ID_3 };
        Arrays.sort(docIds);
        final List<Float> scores = Stream.generate(() -> random().nextFloat()).limit(NUM_DOCS).collect(Collectors.toList());

        HybridSubQueryScorer scorer = new HybridSubQueryScorer(1);
        leafCollector.setScorer(scorer);

        collectDocsAndScores(scorer, scores, leafCollector, 0, docIds);

        List<TopDocs> topDocs = hybridTopScoreDocCollector.topDocs();
        assertEquals(1, topDocs.size());
        TopDocs topDoc = topDocs.get(0);
        assertEquals(3, topDoc.totalHits.value());
        ScoreDoc[] scoreDocs = topDoc.scoreDocs;
        assertEquals(3, scoreDocs.length);
        assertFalse(Arrays.stream(scoreDocs).anyMatch(score -> score.score <= 0.0));

        w.close();
        reader.close();
        directory.close();
    }

    @SneakyThrows
    public void testCompoundScorer_whenHybridScorerIsTopLevelScorer_thenSuccessful() {
        HybridTopScoreDocCollector hybridTopScoreDocCollector = new HybridTopScoreDocCollector(
            NUM_DOCS,
            new HitsThresholdChecker(Integer.MAX_VALUE)
        );

        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_1, FIELD_1_VALUE, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_2, FIELD_2_VALUE, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_3, FIELD_3_VALUE, ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);

        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);
        LeafCollector leafCollector = hybridTopScoreDocCollector.getLeafCollector(leafReaderContext);

        assertNotNull(leafCollector);

        Scorer subQueryScorer = mock(Scorer.class);
        DocIdSetIterator iterator = mock(DocIdSetIterator.class);
        when(subQueryScorer.iterator()).thenReturn(iterator);

        int[] docIds = new int[] { DOC_ID_1, DOC_ID_2, DOC_ID_3 };
        Arrays.sort(docIds);
        final List<Float> scores = Stream.generate(() -> random().nextFloat()).limit(NUM_DOCS).collect(Collectors.toList());

        HybridSubQueryScorer scorer = new HybridSubQueryScorer(1);
        leafCollector.setScorer(scorer);
        collectDocsAndScores(scorer, scores, leafCollector, 0, docIds);

        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(Arrays.asList(subQueryScorer));

        leafCollector.setScorer(hybridQueryScorer);
        int nextDoc = hybridQueryScorer.iterator().nextDoc();
        leafCollector.collect(nextDoc);

        w.close();
        reader.close();
        directory.close();
    }

    @SneakyThrows
    public void testTotalHitsCountValidation_whenTotalHitsCollectedAtTopLevelInCollector_thenSuccessful() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_1, FIELD_1_VALUE, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_2, FIELD_2_VALUE, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_3, FIELD_3_VALUE, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, DOC_ID_4, FIELD_4_VALUE, ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);

        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);

        HybridTopScoreDocCollector hybridTopScoreDocCollector = new HybridTopScoreDocCollector(
            NUM_HITS,
            new HitsThresholdChecker(Integer.MAX_VALUE)
        );
        LeafCollector leafCollector = hybridTopScoreDocCollector.getLeafCollector(leafReaderContext);
        assertNotNull(leafCollector);

        int[] docIdsForQuery1 = new int[] { DOC_ID_1, DOC_ID_2 };
        Arrays.sort(docIdsForQuery1);
        int[] docIdsForQuery2 = new int[] { DOC_ID_3, DOC_ID_4 };
        Arrays.sort(docIdsForQuery2);
        final List<Float> scores = Stream.generate(() -> random().nextFloat()).limit(NUM_DOCS).collect(Collectors.toList());

        HybridSubQueryScorer scorer = new HybridSubQueryScorer(2);
        leafCollector.setScorer(scorer);

        collectDocsAndScores(scorer, scores, leafCollector, 0, docIdsForQuery1);
        collectDocsAndScores(scorer, scores, leafCollector, 1, docIdsForQuery2);

        List<TopDocs> topDocs = hybridTopScoreDocCollector.topDocs();
        long totalHits = hybridTopScoreDocCollector.getTotalHits();
        List<ScoreDoc[]> scoreDocs = topDocs.stream()
            .map(topdDoc -> topdDoc.scoreDocs)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        Set<Integer> uniqueDocIds = new HashSet<>();
        for (ScoreDoc[] scoreDocsArray : scoreDocs) {
            uniqueDocIds.addAll(Arrays.stream(scoreDocsArray).map(scoreDoc -> scoreDoc.doc).collect(Collectors.toList()));
        }
        long maxTotalHits = uniqueDocIds.size();
        assertEquals(4, totalHits);
        // Total unique docs on the shard will be 2 as per 1 per sub-query
        assertEquals(2, maxTotalHits);
        w.close();
        reader.close();
        directory.close();
    }
}
