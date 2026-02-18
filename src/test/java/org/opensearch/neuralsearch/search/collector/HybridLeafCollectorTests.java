/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import lombok.SneakyThrows;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.opensearch.neuralsearch.query.HybridQueryScorer;
import org.opensearch.neuralsearch.query.HybridSubQueryScorer;
import org.opensearch.search.profile.ProfilingWrapper;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HybridLeafCollectorTests extends HybridCollectorTestCase {

    static final String TEXT_FIELD_NAME = "field";
    private static final int NUM_DOCS = 4;
    private static final int TOTAL_HITS_UP_TO = 1000;

    @SneakyThrows
    public void testSetScorer_whenHybridQueryScorerPassedDirectly_thenProfilerModeActivated() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();
        w.addDocument(getDocument(TEXT_FIELD_NAME, 1, "text1", ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);

        HybridTopScoreDocCollector collector = new HybridTopScoreDocCollector(NUM_DOCS, new HitsThresholdChecker(TOTAL_HITS_UP_TO));
        LeafCollector leafCollector = collector.getLeafCollector(leafReaderContext);

        // Create a HybridQueryScorer with 2 sub-query scorers
        Weight mockWeight = mock(Weight.class);
        Scorer subScorer1 = mock(Scorer.class);
        Scorer subScorer2 = mock(Scorer.class);
        when(subScorer1.iterator()).thenReturn(DocIdSetIterator.empty());
        when(subScorer2.iterator()).thenReturn(DocIdSetIterator.empty());

        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(Arrays.asList(subScorer1, subScorer2));

        // Set the HybridQueryScorer directly - this tests the profiler path
        leafCollector.setScorer(hybridQueryScorer);

        // Verify the profiler mode was activated
        HybridTopScoreDocCollector.HybridTopScoreLeafCollector hybridLeafCollector =
            (HybridTopScoreDocCollector.HybridTopScoreLeafCollector) leafCollector;
        assertNotNull("compoundQueryScorer should be created as adapter", hybridLeafCollector.getCompoundQueryScorer());
        assertNotNull("hybridQueryScorer should be set", hybridLeafCollector.getHybridQueryScorer());
        assertEquals("adapter should have 2 sub-queries", 2, hybridLeafCollector.getCompoundQueryScorer().getNumOfSubQueries());

        reader.close();
        w.close();
        directory.close();
    }

    @SneakyThrows
    public void testPopulateScores_whenHybridQueryScorerHasMatchingDoc_thenScoresPopulated() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();
        w.addDocument(getDocument(TEXT_FIELD_NAME, 1, "text1", ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);

        HybridTopScoreDocCollector collector = new HybridTopScoreDocCollector(NUM_DOCS, new HitsThresholdChecker(TOTAL_HITS_UP_TO));
        LeafCollector leafCollector = collector.getLeafCollector(leafReaderContext);

        // Create mock scorers positioned on doc 5
        Scorer subScorer1 = mock(Scorer.class);
        Scorer subScorer2 = mock(Scorer.class);
        when(subScorer1.docID()).thenReturn(5);
        when(subScorer2.docID()).thenReturn(5);
        when(subScorer1.score()).thenReturn(1.5f);
        when(subScorer2.score()).thenReturn(2.5f);

        // Mock HybridQueryScorer to control docID and sub-scorers
        HybridQueryScorer mockHybridScorer = mock(HybridQueryScorer.class);
        when(mockHybridScorer.docID()).thenReturn(5);
        when(mockHybridScorer.getSubScorers()).thenReturn(Arrays.asList(subScorer1, subScorer2));

        HybridTopScoreDocCollector.HybridTopScoreLeafCollector hybridLeafCollector =
            (HybridTopScoreDocCollector.HybridTopScoreLeafCollector) leafCollector;

        // Directly set the profiler-mode fields
        hybridLeafCollector.hybridQueryScorer = mockHybridScorer;
        hybridLeafCollector.compoundQueryScorer = new HybridSubQueryScorer(2);

        // Populate scores
        hybridLeafCollector.populateScoresFromHybridQueryScorer();

        // Verify scores were populated
        float[] scores = hybridLeafCollector.getCompoundQueryScorer().getSubQueryScores();
        assertEquals("sub-query 1 score should be 1.5", 1.5f, scores[0], 0.001f);
        assertEquals("sub-query 2 score should be 2.5", 2.5f, scores[1], 0.001f);

        reader.close();
        w.close();
        directory.close();
    }

    @SneakyThrows
    public void testPopulateScores_whenSubScorersOnDifferentDocs_thenOnlyMatchingScorePopulated() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();
        w.addDocument(getDocument(TEXT_FIELD_NAME, 1, "text1", ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);

        HybridTopScoreDocCollector collector = new HybridTopScoreDocCollector(NUM_DOCS, new HitsThresholdChecker(TOTAL_HITS_UP_TO));
        LeafCollector leafCollector = collector.getLeafCollector(leafReaderContext);

        // subScorer1 on doc 5, subScorer2 on doc 10
        Scorer subScorer1 = mock(Scorer.class);
        Scorer subScorer2 = mock(Scorer.class);
        when(subScorer1.docID()).thenReturn(5);
        when(subScorer2.docID()).thenReturn(10);
        when(subScorer1.score()).thenReturn(1.5f);
        when(subScorer2.score()).thenReturn(2.5f);

        HybridQueryScorer mockHybridScorer = mock(HybridQueryScorer.class);
        when(mockHybridScorer.docID()).thenReturn(5);
        when(mockHybridScorer.getSubScorers()).thenReturn(Arrays.asList(subScorer1, subScorer2));

        HybridTopScoreDocCollector.HybridTopScoreLeafCollector hybridLeafCollector =
            (HybridTopScoreDocCollector.HybridTopScoreLeafCollector) leafCollector;

        hybridLeafCollector.hybridQueryScorer = mockHybridScorer;
        hybridLeafCollector.compoundQueryScorer = new HybridSubQueryScorer(2);

        hybridLeafCollector.populateScoresFromHybridQueryScorer();

        float[] scores = hybridLeafCollector.getCompoundQueryScorer().getSubQueryScores();
        assertEquals("sub-query 1 score should be 1.5 (matching doc)", 1.5f, scores[0], 0.001f);
        assertEquals("sub-query 2 score should be 0.0 (different doc)", 0.0f, scores[1], 0.001f);

        reader.close();
        w.close();
        directory.close();
    }

    @SneakyThrows
    public void testPopulateScores_whenNotInProfilerMode_thenNoOp() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();
        w.addDocument(getDocument(TEXT_FIELD_NAME, 1, "text1", ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);

        HybridTopScoreDocCollector collector = new HybridTopScoreDocCollector(NUM_DOCS, new HitsThresholdChecker(TOTAL_HITS_UP_TO));
        LeafCollector leafCollector = collector.getLeafCollector(leafReaderContext);

        // Set a normal HybridSubQueryScorer (non-profiler path)
        HybridSubQueryScorer subQueryScorer = new HybridSubQueryScorer(2);
        subQueryScorer.getSubQueryScores()[0] = 3.0f;
        subQueryScorer.getSubQueryScores()[1] = 4.0f;
        leafCollector.setScorer(subQueryScorer);

        HybridTopScoreDocCollector.HybridTopScoreLeafCollector hybridLeafCollector =
            (HybridTopScoreDocCollector.HybridTopScoreLeafCollector) leafCollector;

        // hybridQueryScorer should be null in non-profiler mode
        assertNull("hybridQueryScorer should be null in normal mode", hybridLeafCollector.getHybridQueryScorer());

        // populateScoresFromHybridQueryScorer should be a no-op
        hybridLeafCollector.populateScoresFromHybridQueryScorer();

        // Scores should remain unchanged
        assertEquals(3.0f, hybridLeafCollector.getCompoundQueryScorer().getSubQueryScores()[0], 0.001f);
        assertEquals(4.0f, hybridLeafCollector.getCompoundQueryScorer().getSubQueryScores()[1], 0.001f);

        reader.close();
        w.close();
        directory.close();
    }

    @SneakyThrows
    public void testPopulateScores_whenSubScorerIsNull_thenScoreIsZero() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();
        w.addDocument(getDocument(TEXT_FIELD_NAME, 1, "text1", ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);

        HybridTopScoreDocCollector collector = new HybridTopScoreDocCollector(NUM_DOCS, new HitsThresholdChecker(TOTAL_HITS_UP_TO));
        LeafCollector leafCollector = collector.getLeafCollector(leafReaderContext);

        Scorer subScorer1 = mock(Scorer.class);
        when(subScorer1.docID()).thenReturn(5);
        when(subScorer1.score()).thenReturn(1.5f);

        HybridQueryScorer mockHybridScorer = mock(HybridQueryScorer.class);
        when(mockHybridScorer.docID()).thenReturn(5);
        when(mockHybridScorer.getSubScorers()).thenReturn(Arrays.asList(subScorer1, null));

        HybridTopScoreDocCollector.HybridTopScoreLeafCollector hybridLeafCollector =
            (HybridTopScoreDocCollector.HybridTopScoreLeafCollector) leafCollector;

        hybridLeafCollector.hybridQueryScorer = mockHybridScorer;
        hybridLeafCollector.compoundQueryScorer = new HybridSubQueryScorer(2);

        hybridLeafCollector.populateScoresFromHybridQueryScorer();

        float[] scores = hybridLeafCollector.getCompoundQueryScorer().getSubQueryScores();
        assertEquals("sub-query 1 score should be 1.5", 1.5f, scores[0], 0.001f);
        assertEquals("sub-query 2 score should be 0.0 (null scorer)", 0.0f, scores[1], 0.001f);

        reader.close();
        w.close();
        directory.close();
    }

    @SneakyThrows
    public void testSetScorer_whenUnknownScorerType_thenExceptionThrown() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();
        w.addDocument(getDocument(TEXT_FIELD_NAME, 1, "text1", ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);

        HybridTopScoreDocCollector collector = new HybridTopScoreDocCollector(NUM_DOCS, new HitsThresholdChecker(TOTAL_HITS_UP_TO));
        LeafCollector leafCollector = collector.getLeafCollector(leafReaderContext);

        // Use a plain Scorable that is neither HybridSubQueryScorer nor HybridQueryScorer
        Scorable unknownScorer = mock(Scorable.class);
        when(unknownScorer.score()).thenReturn(1.0f);

        // Setting an unknown scorer type causes NPE in HybridTopScoreLeafCollector.setScorer()
        // because compoundQueryScorer is null and it tries to call getNumOfSubQueries() on it
        assertThrows(NullPointerException.class, () -> leafCollector.setScorer(unknownScorer));

        reader.close();
        w.close();
        directory.close();
    }

    @SneakyThrows
    public void testSetScorer_whenProfilingWrapperWrapsHybridQueryScorer_thenProfilerModeActivated() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();
        w.addDocument(getDocument(TEXT_FIELD_NAME, 1, "text1", ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);

        HybridTopScoreDocCollector collector = new HybridTopScoreDocCollector(NUM_DOCS, new HitsThresholdChecker(TOTAL_HITS_UP_TO));
        LeafCollector leafCollector = collector.getLeafCollector(leafReaderContext);

        // Create a HybridQueryScorer with 2 sub-query scorers
        Scorer subScorer1 = mock(Scorer.class);
        Scorer subScorer2 = mock(Scorer.class);
        when(subScorer1.iterator()).thenReturn(DocIdSetIterator.empty());
        when(subScorer2.iterator()).thenReturn(DocIdSetIterator.empty());
        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(Arrays.asList(subScorer1, subScorer2));

        // Create a mock that implements both Scorer and ProfilingWrapper<Scorer>,
        // simulating what ProfileScorer does when profiling is enabled
        ProfilingWrapperScorer profilingWrapper = mock(ProfilingWrapperScorer.class);
        when(profilingWrapper.getDelegate()).thenReturn(hybridQueryScorer);
        when(profilingWrapper.getChildren()).thenReturn(Collections.emptyList());

        // Set the profiling wrapper as the scorer
        leafCollector.setScorer(profilingWrapper);

        // Verify profiler mode was activated through ProfilingWrapper unwrap
        HybridTopScoreDocCollector.HybridTopScoreLeafCollector hybridLeafCollector =
            (HybridTopScoreDocCollector.HybridTopScoreLeafCollector) leafCollector;
        assertNotNull("compoundQueryScorer should be created as adapter", hybridLeafCollector.getCompoundQueryScorer());
        assertNotNull("hybridQueryScorer should be found via ProfilingWrapper", hybridLeafCollector.getHybridQueryScorer());
        assertEquals("adapter should have 2 sub-queries", 2, hybridLeafCollector.getCompoundQueryScorer().getNumOfSubQueries());

        reader.close();
        w.close();
        directory.close();
    }

    /**
     * Abstract class combining Scorer and ProfilingWrapper for mocking purposes.
     * This simulates how ProfileScorer extends Scorer and implements ProfilingWrapper<Scorer>.
     */
    static abstract class ProfilingWrapperScorer extends Scorer implements ProfilingWrapper<Scorer> {}

    @SneakyThrows
    public void testSetScorer_whenProfilingWrapperReturnsNullDelegate_thenFallsThrough() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();
        w.addDocument(getDocument(TEXT_FIELD_NAME, 1, "text1", ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);

        HybridTopScoreDocCollector collector = new HybridTopScoreDocCollector(NUM_DOCS, new HitsThresholdChecker(TOTAL_HITS_UP_TO));
        LeafCollector leafCollector = collector.getLeafCollector(leafReaderContext);

        // Create a ProfilingWrapper that returns null from getDelegate()
        ProfilingWrapperScorer profilingWrapper = mock(ProfilingWrapperScorer.class);
        when(profilingWrapper.getDelegate()).thenReturn(null);
        when(profilingWrapper.getChildren()).thenReturn(Collections.emptyList());

        // This should NPE because no HybridQueryScorer is found, compoundQueryScorer is null
        assertThrows(NullPointerException.class, () -> leafCollector.setScorer(profilingWrapper));

        reader.close();
        w.close();
        directory.close();
    }

    @SneakyThrows
    public void testCollect_whenInProfilerMode_thenScoresPopulatedViaCollect() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();
        w.addDocument(getDocument(TEXT_FIELD_NAME, 1, "text1", ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);

        HybridTopScoreDocCollector collector = new HybridTopScoreDocCollector(NUM_DOCS, new HitsThresholdChecker(TOTAL_HITS_UP_TO));
        LeafCollector leafCollector = collector.getLeafCollector(leafReaderContext);

        // Create mock scorers positioned on doc 0
        Scorer subScorer1 = mock(Scorer.class);
        when(subScorer1.docID()).thenReturn(0);
        when(subScorer1.score()).thenReturn(2.0f);
        when(subScorer1.iterator()).thenReturn(DocIdSetIterator.empty());

        HybridQueryScorer mockHybridScorer = mock(HybridQueryScorer.class);
        when(mockHybridScorer.docID()).thenReturn(0);
        when(mockHybridScorer.getSubScorers()).thenReturn(Arrays.asList(subScorer1));

        HybridTopScoreDocCollector.HybridTopScoreLeafCollector hybridLeafCollector =
            (HybridTopScoreDocCollector.HybridTopScoreLeafCollector) leafCollector;

        // Set profiler-mode fields
        hybridLeafCollector.hybridQueryScorer = mockHybridScorer;
        hybridLeafCollector.compoundQueryScorer = new HybridSubQueryScorer(1);

        // Call collect() which should internally call populateScoresFromHybridQueryScorer()
        leafCollector.collect(0);

        // Verify that scores were collected (totalHits incremented)
        assertTrue("totalHits should be > 0 after collect", collector.getTotalHits() > 0);

        reader.close();
        w.close();
        directory.close();
    }

    /**
     * Test findHybridQueryScorer traversal via getChildren() path.
     * When scorer has children containing HybridQueryScorer, it should be found.
     */
    @SneakyThrows
    public void testSetScorer_whenHybridQueryScorerInChildren_thenProfilerModeActivated() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();
        w.addDocument(getDocument(TEXT_FIELD_NAME, 1, "text1", ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);

        HybridTopScoreDocCollector collector = new HybridTopScoreDocCollector(NUM_DOCS, new HitsThresholdChecker(TOTAL_HITS_UP_TO));
        LeafCollector leafCollector = collector.getLeafCollector(leafReaderContext);

        // Create a HybridQueryScorer
        Scorer subScorer1 = mock(Scorer.class);
        when(subScorer1.iterator()).thenReturn(DocIdSetIterator.empty());
        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(Arrays.asList(subScorer1));

        // Create a wrapper that exposes HybridQueryScorer via getChildren()
        Scorable wrapperScorer = mock(Scorable.class);
        when(wrapperScorer.getChildren()).thenReturn(Collections.singletonList(new Scorable.ChildScorable(hybridQueryScorer, "MUST")));

        leafCollector.setScorer(wrapperScorer);

        HybridTopScoreDocCollector.HybridTopScoreLeafCollector hybridLeafCollector =
            (HybridTopScoreDocCollector.HybridTopScoreLeafCollector) leafCollector;
        assertNotNull("hybridQueryScorer should be found via getChildren traversal", hybridLeafCollector.getHybridQueryScorer());
        assertNotNull("compoundQueryScorer adapter should be created", hybridLeafCollector.getCompoundQueryScorer());

        reader.close();
        w.close();
        directory.close();
    }

    @SneakyThrows
    public void testPopulateScores_whenSubScorerOnNoMoreDocs_thenScoreIsZero() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();
        w.addDocument(getDocument(TEXT_FIELD_NAME, 1, "text1", ft));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);

        HybridTopScoreDocCollector collector = new HybridTopScoreDocCollector(NUM_DOCS, new HitsThresholdChecker(TOTAL_HITS_UP_TO));
        LeafCollector leafCollector = collector.getLeafCollector(leafReaderContext);

        // Create scorer positioned at NO_MORE_DOCS
        Scorer subScorer1 = mock(Scorer.class);
        when(subScorer1.iterator()).thenReturn(DocIdSetIterator.empty());
        when(subScorer1.docID()).thenReturn(DocIdSetIterator.NO_MORE_DOCS);
        when(subScorer1.score()).thenReturn(99.9f);

        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(Arrays.asList(subScorer1));

        leafCollector.setScorer(hybridQueryScorer);

        HybridTopScoreDocCollector.HybridTopScoreLeafCollector hybridLeafCollector =
            (HybridTopScoreDocCollector.HybridTopScoreLeafCollector) leafCollector;

        hybridLeafCollector.populateScoresFromHybridQueryScorer();

        float[] scores = hybridLeafCollector.getCompoundQueryScorer().getSubQueryScores();
        assertEquals("score should be 0.0 when doc is NO_MORE_DOCS", 0.0f, scores[0], 0.001f);

        reader.close();
        w.close();
        directory.close();
    }
}
