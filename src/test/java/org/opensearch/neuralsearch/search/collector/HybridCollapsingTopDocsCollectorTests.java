/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import lombok.SneakyThrows;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.neuralsearch.query.HybridQueryScorer;
import org.opensearch.neuralsearch.query.HybridSubQueryScorer;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HybridCollapsingTopDocsCollectorTests extends HybridCollectorTestCase {

    private static final String TEXT_FIELD_NAME = "field";
    private static final String INT_FIELD_NAME = "integerField";
    private static final String COLLAPSE_FIELD_NAME = "collapseField";
    private static final int numHits = 5;
    private static final int TOTAL_HITS_UP_TO = 1001;

    public void testKeywordCollapse_whenCollectAndTopDocs_thenSuccessful() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        // Add 1000 documents with keyword collapse field values
        for (int i = 0; i < 1000; i++) {
            addKeywordDoc(writer, i, "text" + i, 100 + i, "group" + (i % 10));
        }
        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        Sort sort = new Sort(SortField.FIELD_SCORE);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopDocsCollector<?> collector = HybridCollapsingTopDocsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            numHits,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        int[] docIds = IntStream.range(0, 1000).toArray();
        List<Float> scores = Stream.generate(() -> random().nextFloat()).limit(1000).collect(Collectors.toList());

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(1);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        collectDocsAndScores(hybridScorer, scores, leafCollector, 0, docIds);

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();

        assertEquals(1, topDocs.size());  // One for each sub-query

        for (CollapseTopFieldDocs collapseTopFieldDocs : topDocs) {
            // With flat queue, totalHits counts all docs with score > 0 per sub-query
            // random().nextFloat() returns [0.0, 1.0), so nearly all 1000 docs have score > 0
            assertTrue(collapseTopFieldDocs.totalHits.value() >= 999);
            assertEquals(numHits, collapseTopFieldDocs.scoreDocs.length);
        }

        reader.close();
        writer.close();
        directory.close();
    }

    public void testNumericCollapse_whenCollectAndTopDocs_thenSuccessful() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        // Add 1000 documents with numeric collapse field values
        for (int i = 0; i < 1000; i++) {
            addNumericDoc(writer, i, "text" + i, 100 + i, i % 10);
        }
        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        Sort sort = new Sort(new SortField(INT_FIELD_NAME, SortField.Type.INT));
        NumberFieldMapper.NumberFieldType fieldType = new NumberFieldMapper.NumberFieldType(
            COLLAPSE_FIELD_NAME,
            NumberFieldMapper.NumberType.LONG
        );

        HybridCollapsingTopDocsCollector<?> collector = HybridCollapsingTopDocsCollector.createNumeric(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            numHits,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        int[] docIds = IntStream.range(0, 1000).toArray();
        List<Float> scores = Stream.generate(() -> random().nextFloat()).limit(1000).collect(Collectors.toList());

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(1);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        collectDocsAndScores(hybridScorer, scores, leafCollector, 0, docIds);

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();

        assertEquals(1, topDocs.size());

        for (CollapseTopFieldDocs collapseTopFieldDocs : topDocs) {
            // With flat queue, totalHits counts all docs with score > 0 per sub-query
            assertTrue(collapseTopFieldDocs.totalHits.value() >= 999);
            assertEquals(numHits, collapseTopFieldDocs.scoreDocs.length);
        }

        reader.close();
        writer.close();
        directory.close();
    }

    public void testCollapse_whenDefaultConfig_thenFlatQueueCollectsSuccessfully() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        // Add 1000 documents with keyword collapse field values
        for (int i = 0; i < 1000; i++) {
            addKeywordDoc(writer, i, "text" + i, 100 + i, "group" + (i % 10));
        }
        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        Sort sort = new Sort(SortField.FIELD_SCORE);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopDocsCollector<?> collector = HybridCollapsingTopDocsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            numHits,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        int[] docIds = IntStream.range(0, 1000).toArray();
        List<Float> scores = Stream.generate(() -> random().nextFloat()).limit(1000).collect(Collectors.toList());

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(1);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        collectDocsAndScores(hybridScorer, scores, leafCollector, 0, docIds);

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();

        assertEquals(1, topDocs.size());  // One for each sub-query

        for (CollapseTopFieldDocs collapseTopFieldDocs : topDocs) {
            // With flat queue, totalHits counts all docs with score > 0 per sub-query
            assertTrue(collapseTopFieldDocs.totalHits.value() >= 999);
            assertEquals(numHits, collapseTopFieldDocs.scoreDocs.length);
        }

        reader.close();
        writer.close();
        directory.close();
    }

    public void testCollapse_whenManyDocsInSameGroup_thenFlatQueueHandlesCorrectly() throws IOException {
        /*
         * Tests that the flat per-sub-query queue correctly handles many documents
         * mapping to the same collapse group. With the flat queue approach, all docs
         * compete globally for the top-K slots regardless of group membership.
         */

        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        // Add 100 documents where many documents map to the same collapse group
        // This creates the scenario where multiple documents compete for the same small queue
        for (int i = 0; i < 100; i++) {
            // Most documents map to "group0" to trigger the queue overflow scenario
            String collapseValue = (i < 50) ? "group0" : "group" + (i % 5);
            addKeywordDoc(writer, i, "text" + i, 100 + i, collapseValue);
        }
        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        Sort sort = new Sort(SortField.FIELD_SCORE);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        // CONFIGURATION: topNGroups with many docs in same group
        int topNGroups = 10;  // Want to return 10 top results

        HybridCollapsingTopDocsCollector<?> collector = HybridCollapsingTopDocsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            topNGroups,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        int[] docIds = IntStream.range(0, 100).toArray();
        List<Float> scores = Stream.generate(() -> random().nextFloat()).limit(100).collect(Collectors.toList());

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(1);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        // This exercises the flat queue with many docs competing for the same slots
        collectDocsAndScores(hybridScorer, scores, leafCollector, 0, docIds);

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();

        assertEquals(1, topDocs.size());  // One for each sub-query

        for (CollapseTopFieldDocs collapseTopFieldDocs : topDocs) {
            // With flat queue, totalHits counts all docs with score > 0 per sub-query
            assertEquals(100, collapseTopFieldDocs.totalHits.value());

            // Flat queue of size topNGroups=10, so at most 10 results
            assertTrue("Should have some results", collapseTopFieldDocs.scoreDocs.length > 0);
            assertTrue("Should not exceed topNGroups", collapseTopFieldDocs.scoreDocs.length <= 10);
        }

        reader.close();
        writer.close();
        directory.close();
    }

    public void testCollapse_whenAllDocsInSameGroup_thenQueueFillsCorrectly() throws IOException {
        /*
         * Tests that when ALL documents map to the same collapse group,
         * the flat queue correctly fills to capacity and evicts weaker entries.
         */

        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        // Add documents where ALL documents map to the same group
        // This maximizes the pressure on the single-document queue
        for (int i = 0; i < 20; i++) {
            addKeywordDoc(writer, i, "text" + i, 100 + i, "samegroup");
        }
        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        Sort sort = new Sort(SortField.FIELD_SCORE);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        // Extreme case: docsPerGroupPerSubQuery = 1, all docs in same group
        HybridCollapsingTopDocsCollector<?> collector = HybridCollapsingTopDocsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            5,  // topNGroups
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        int[] docIds = IntStream.range(0, 20).toArray();
        List<Float> scores = Stream.generate(() -> random().nextFloat()).limit(20).collect(Collectors.toList());

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(1);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        // Before: queue not full, docs added directly
        // After queue fills to 5: subsequent docs compete via updateExistingEntry
        collectDocsAndScores(hybridScorer, scores, leafCollector, 0, docIds);

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();

        assertEquals(1, topDocs.size());

        for (CollapseTopFieldDocs collapseTopFieldDocs : topDocs) {
            // With flat queue, totalHits counts all docs with score > 0 per sub-query
            assertEquals(20, collapseTopFieldDocs.totalHits.value());

            // Flat queue of size 5, all 20 docs are in same group "samegroup"
            // Queue holds top 5 docs globally
            assertEquals("Should have exactly 5 docs in flat queue", 5, collapseTopFieldDocs.scoreDocs.length);

            // All collapse values should be "samegroup" since all docs are in the same group
            assertEquals("Should have exactly 5 collapse values", 5, collapseTopFieldDocs.collapseValues.length);
            assertEquals("samegroup", ((BytesRef) collapseTopFieldDocs.collapseValues[0]).utf8ToString());
        }

        reader.close();
        writer.close();
        directory.close();
    }

    /**
     * Test sorting by score with collapse - validates HybridLeafFieldComparator wrapper
     * This is the key test case related to the bug fix in the context transfer
     */
    public void testCollapse_whenSortByScore_thenCorrectRanking() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        // Add documents with known scores to verify ranking
        for (int i = 0; i < 50; i++) {
            addKeywordDoc(writer, i, "text" + i, 100 + i, "group" + (i % 5));
        }
        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        // Sort by SCORE - this triggers the HybridLeafFieldComparator wrapper
        Sort sort = new Sort(SortField.FIELD_SCORE);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopDocsCollector<?> collector = HybridCollapsingTopDocsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            numHits,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        int[] docIds = IntStream.range(0, 50).toArray();
        // Create descending scores to verify ranking
        List<Float> scores = IntStream.range(0, 50).mapToObj(i -> 1.0f - (i * 0.01f)).collect(Collectors.toList());

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(1);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        collectDocsAndScores(hybridScorer, scores, leafCollector, 0, docIds);

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();

        assertEquals(1, topDocs.size());

        CollapseTopFieldDocs collapseTopFieldDocs = topDocs.get(0);

        // Verify scores are in descending order (highest first)
        float previousScore = Float.MAX_VALUE;
        for (int i = 0; i < collapseTopFieldDocs.scoreDocs.length; i++) {
            float currentScore = collapseTopFieldDocs.scoreDocs[i].score;
            assertTrue("Scores should be in descending order", currentScore <= previousScore);
            previousScore = currentScore;
        }

        reader.close();
        writer.close();
        directory.close();
    }

    /**
     * Test sorting by field (not score) with collapse
     */
    public void testCollapse_whenSortByField_thenCorrectOrdering() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        // Add documents with varying integer field values
        for (int i = 0; i < 50; i++) {
            addKeywordDoc(writer, i, "text" + i, 200 - i, "group" + (i % 5));
        }
        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        // Sort by integer field ascending
        Sort sort = new Sort(new SortField(INT_FIELD_NAME, SortField.Type.INT, false));
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopDocsCollector<?> collector = HybridCollapsingTopDocsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            numHits,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        int[] docIds = IntStream.range(0, 50).toArray();
        List<Float> scores = Stream.generate(() -> random().nextFloat()).limit(50).collect(Collectors.toList());

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(1);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        collectDocsAndScores(hybridScorer, scores, leafCollector, 0, docIds);

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();

        assertEquals(1, topDocs.size());

        CollapseTopFieldDocs collapseTopFieldDocs = topDocs.get(0);

        // Verify field values are in ascending order
        int previousValue = Integer.MIN_VALUE;
        for (int i = 0; i < collapseTopFieldDocs.scoreDocs.length; i++) {
            FieldDoc fieldDoc = (FieldDoc) collapseTopFieldDocs.scoreDocs[i];
            int currentValue = ((Number) fieldDoc.fields[0]).intValue();
            assertTrue("Field values should be in ascending order", currentValue >= previousValue);
            previousValue = currentValue;
        }

        reader.close();
        writer.close();
        directory.close();
    }

    /**
     * Test multiple sub-queries with collapse
     */
    public void testCollapse_whenMultipleSubQueries_thenEachSubQueryHasResults() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        for (int i = 0; i < 100; i++) {
            addKeywordDoc(writer, i, "text" + i, 100 + i, "group" + (i % 10));
        }
        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        Sort sort = new Sort(SortField.FIELD_SCORE);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopDocsCollector<?> collector = HybridCollapsingTopDocsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            numHits,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        int[] docIds = IntStream.range(0, 100).toArray();

        // Create 3 sub-queries with different score patterns
        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(3);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        // Collect with 3 sub-queries
        for (int docId : docIds) {
            float[] subScores = new float[3];
            // Sub-query 0: high scores for even docs
            subScores[0] = (docId % 2 == 0) ? 0.9f : 0.0f;
            // Sub-query 1: high scores for odd docs
            subScores[1] = (docId % 2 == 1) ? 0.8f : 0.0f;
            // Sub-query 2: medium scores for all
            subScores[2] = 0.5f;

            hybridScorer.resetScores();
            for (int i = 0; i < 3; i++) {
                hybridScorer.getSubQueryScores()[i] = subScores[i];
            }

            leafCollector.collect(docId);
        }

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();

        // Should have results for all 3 sub-queries
        assertEquals(3, topDocs.size());

        // Verify each sub-query has results
        for (int i = 0; i < 3; i++) {
            CollapseTopFieldDocs subQueryDocs = topDocs.get(i);
            assertTrue("Sub-query " + i + " should have results", subQueryDocs.scoreDocs.length > 0);
            assertTrue("Sub-query " + i + " should have total hits", subQueryDocs.totalHits.value() > 0);
        }

        reader.close();
        writer.close();
        directory.close();
    }

    /**
     * Test empty results when no documents match
     */
    public void testCollapse_whenNoDocumentsMatch_thenEmptyResults() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        for (int i = 0; i < 10; i++) {
            addKeywordDoc(writer, i, "text" + i, 100 + i, "group" + i);
        }
        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        Sort sort = new Sort(SortField.FIELD_SCORE);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopDocsCollector<?> collector = HybridCollapsingTopDocsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            numHits,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        // Create scorer but don't collect any documents (all scores are 0)
        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(1);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        // Collect with all zero scores (no matches)
        for (int docId = 0; docId < 10; docId++) {
            hybridScorer.resetScores();
            hybridScorer.getSubQueryScores()[0] = 0.0f;  // Zero score means no match
            leafCollector.collect(docId);
        }

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();

        assertEquals(1, topDocs.size());

        CollapseTopFieldDocs collapseTopFieldDocs = topDocs.get(0);
        assertEquals(0, collapseTopFieldDocs.totalHits.value());
        assertEquals(0, collapseTopFieldDocs.scoreDocs.length);
        assertEquals(0, collapseTopFieldDocs.collapseValues.length);

        reader.close();
        writer.close();
        directory.close();
    }

    /**
     * Test getTotalHits and getMaxScore methods
     */
    public void testCollapse_whenGetTotalHitsAndMaxScore_thenCorrectValues() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        for (int i = 0; i < 50; i++) {
            addKeywordDoc(writer, i, "text" + i, 100 + i, "group" + (i % 5));
        }
        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        Sort sort = new Sort(SortField.FIELD_SCORE);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopDocsCollector<?> collector = HybridCollapsingTopDocsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            numHits,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        int[] docIds = IntStream.range(0, 50).toArray();
        List<Float> scores = IntStream.range(0, 50).mapToObj(i -> 1.0f - (i * 0.01f)).collect(Collectors.toList());

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(1);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        collectDocsAndScores(hybridScorer, scores, leafCollector, 0, docIds);

        // Test getTotalHits
        assertEquals(50, collector.getTotalHits());

        // Test getMaxScore - should be the highest score (1.0f)
        assertEquals(1.0f, collector.getMaxScore(), 0.001f);

        reader.close();
        writer.close();
        directory.close();
    }

    /**
     * Test with reverse sort order (descending)
     */
    public void testCollapse_whenReverseSortOrder_thenCorrectOrdering() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        for (int i = 0; i < 50; i++) {
            addKeywordDoc(writer, i, "text" + i, 100 + i, "group" + (i % 5));
        }
        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        // Sort by integer field descending (reverse=true)
        Sort sort = new Sort(new SortField(INT_FIELD_NAME, SortField.Type.INT, true));
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopDocsCollector<?> collector = HybridCollapsingTopDocsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            numHits,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        int[] docIds = IntStream.range(0, 50).toArray();
        List<Float> scores = Stream.generate(() -> random().nextFloat()).limit(50).collect(Collectors.toList());

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(1);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        collectDocsAndScores(hybridScorer, scores, leafCollector, 0, docIds);

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();

        assertEquals(1, topDocs.size());

        CollapseTopFieldDocs collapseTopFieldDocs = topDocs.get(0);

        // Verify field values are in descending order
        int previousValue = Integer.MAX_VALUE;
        for (int i = 0; i < collapseTopFieldDocs.scoreDocs.length; i++) {
            FieldDoc fieldDoc = (FieldDoc) collapseTopFieldDocs.scoreDocs[i];
            int currentValue = ((Number) fieldDoc.fields[0]).intValue();
            assertTrue("Field values should be in descending order", currentValue <= previousValue);
            previousValue = currentValue;
        }

        reader.close();
        writer.close();
        directory.close();
    }

    /**
     * Test that when more groups than numHits exist, only the top-K groups survive in topDocs.
     */
    public void testCollapse_whenMoreGroupsThanNumHits_thenOnlyTopKGroupsSurvive() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        // 20 unique groups, TOP_N_GROUPS=5, so 15 groups should be evicted
        for (int i = 0; i < 20; i++) {
            addKeywordDoc(writer, i, "text" + i, 100 + i, "group" + i);
        }
        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        Sort sort = new Sort(SortField.FIELD_SCORE);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopDocsCollector<?> collector = HybridCollapsingTopDocsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            numHits,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        // Deterministic descending scores: group0=1.0, group1=0.95, ..., group19=0.05
        List<Float> scores = IntStream.range(0, 20).mapToObj(i -> 1.0f - (i * 0.05f)).collect(Collectors.toList());

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(1);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        collectDocsAndScores(hybridScorer, scores, leafCollector, 0, IntStream.range(0, 20).toArray());

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();
        assertEquals(1, topDocs.size());

        CollapseTopFieldDocs result = topDocs.get(0);
        assertEquals(numHits, result.scoreDocs.length);
        assertEquals(numHits, result.collapseValues.length);

        // Verify the surviving groups are the top-5 scoring ones
        Set<String> survivingGroups = new HashSet<>();
        for (Object cv : result.collapseValues) {
            survivingGroups.add(((BytesRef) cv).utf8ToString());
        }
        for (int i = 0; i < numHits; i++) {
            assertTrue("group" + i + " should survive", survivingGroups.contains("group" + i));
        }

        reader.close();
        writer.close();
        directory.close();
    }

    public void testCollapseWithMultipleSegments_whenMoreGroupsThanNumHits_thenOnlyTopKGroupsSurvive() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig config = newIndexWriterConfig();
        config.setMergePolicy(NoMergePolicy.INSTANCE); // prevent auto-merging
        IndexWriter writer = new IndexWriter(directory, config);

        // Write docs in batches, flushing between to create separate segments
        for (int i = 0; i < 10; i++) {
            addKeywordDoc(writer, i, "text" + i, 100 + i, "group" + i);
        }
        writer.flush();
        writer.commit();

        for (int i = 10; i < 20; i++) {
            addKeywordDoc(writer, i, "text" + i, 100 + i, "group" + i);
        }
        writer.flush();
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);
        assertTrue("Expected multiple segments", reader.leaves().size() > 1);

        Sort sort = new Sort(SortField.FIELD_SCORE);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopDocsCollector<?> collector = HybridCollapsingTopDocsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            numHits,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(1);

        // Global score map: original doc with stored id=i gets score 1.0 - (i * 0.05)
        // We need to read each segment to find which stored IDs are in it,
        // then assign the correct score per segment-local doc.
        for (LeafReaderContext leafCtx : reader.leaves()) {
            LeafCollector leafCollector = collector.getLeafCollector(leafCtx);
            leafCollector.setScorer(hybridScorer);

            int maxDoc = leafCtx.reader().maxDoc();
            for (int segDoc = 0; segDoc < maxDoc; segDoc++) {
                // Read the stored "id" field to determine the original group index
                Document doc = leafCtx.reader().storedFields().document(segDoc);
                int originalId = doc.getField("_id").numericValue().intValue();
                float score = 1.0f - (originalId * 0.05f);

                hybridScorer.resetScores();
                hybridScorer.getSubQueryScores()[0] = score;
                leafCollector.collect(segDoc);
            }
        }

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();
        assertEquals(1, topDocs.size());

        CollapseTopFieldDocs result = topDocs.get(0);
        assertEquals(numHits, result.scoreDocs.length);
        assertEquals(numHits, result.collapseValues.length);

        // Verify the surviving groups are the top-5 scoring ones
        Set<String> survivingGroups = new HashSet<>();
        for (Object cv : result.collapseValues) {
            survivingGroups.add(((BytesRef) cv).utf8ToString());
        }
        for (int i = 0; i < numHits; i++) {
            assertTrue("group" + i + " should survive", survivingGroups.contains("group" + i));
        }

        reader.close();
        writer.close();
        directory.close();
    }

    /**
     * Test that minScores on HybridSubQueryScorer are updated when groups are evicted (sort by score).
     */
    public void testCollapse_whenGroupsEvictedSortByScore_thenMinScoresUpdated() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        // 10 groups, TOP_N_GROUPS=5, so evictions start after 6th group
        for (int i = 0; i < 10; i++) {
            addKeywordDoc(writer, i, "text" + i, 100 + i, "group" + i);
        }
        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        Sort sort = new Sort(SortField.FIELD_SCORE);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopDocsCollector<?> collector = HybridCollapsingTopDocsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            numHits,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(1);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        assertEquals(0.0f, hybridScorer.getMinScores()[0], 0.0001f);

        // Ascending scores: 0.1, 0.2, 0.3, ..., 1.0
        // First 5 (0.1-0.5) fill the queue, then docs 6-10 (0.6-1.0) evict weaker entries
        for (int i = 0; i < 10; i++) {
            hybridScorer.resetScores();
            hybridScorer.getSubQueryScores()[0] = 0.1f + (i * 0.1f);
            leafCollector.collect(i);
        }

        // After 10 groups with numHits=5, minScores should have been updated from evictions
        assertTrue("minScores should be > 0 after evictions", hybridScorer.getMinScores()[0] > 0.0f);

        reader.close();
        writer.close();
        directory.close();
    }

    /**
     * Test that a group evicted from the live top-K can still receive new docs without NPE.
     */
    public void testCollapse_whenEvictedGroupReappears_thenNoError() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        // groupA appears first with low score, then 4 other groups push it out, then groupA reappears with high score
        addKeywordDoc(writer, 0, "text0", 100, "groupA");
        addKeywordDoc(writer, 1, "text1", 101, "groupB");
        addKeywordDoc(writer, 2, "text2", 102, "groupC");
        addKeywordDoc(writer, 3, "text3", 103, "groupD");
        addKeywordDoc(writer, 4, "text4", 104, "groupE");
        addKeywordDoc(writer, 5, "text5", 105, "groupF");
        // groupA reappears
        addKeywordDoc(writer, 6, "text6", 106, "groupA");
        addKeywordDoc(writer, 7, "text7", 107, "groupA");

        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        Sort sort = new Sort(SortField.FIELD_SCORE);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopDocsCollector<?> collector = HybridCollapsingTopDocsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            numHits,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(1);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        // groupA starts low, gets evicted, then comes back with higher scores
        float[] scores = new float[] { 0.1f, 0.9f, 0.8f, 0.7f, 0.6f, 0.5f, 0.95f, 0.99f };

        for (int i = 0; i < scores.length; i++) {
            hybridScorer.resetScores();
            hybridScorer.getSubQueryScores()[0] = scores[i];
            leafCollector.collect(i);
        }

        // Should not throw NPE or any exception
        List<CollapseTopFieldDocs> topDocs = collector.topDocs();
        assertEquals(1, topDocs.size());
        CollapseTopFieldDocs result = topDocs.get(0);
        // 6 unique groups, numHits=5, so exactly 5 groups survive
        assertEquals(numHits, result.collapseValues.length);

        // Verify groupA survived (it re-entered with 0.95 and 0.99)
        Set<String> survivingGroups = new HashSet<>();
        for (Object cv : result.collapseValues) {
            survivingGroups.add(((BytesRef) cv).utf8ToString());
        }
        assertTrue("groupA should be back in top-K after high-score re-entry", survivingGroups.contains("groupA"));

        // groupF (0.5) should have been evicted since it's the weakest among the 6 groups
        assertFalse("groupF should be evicted as the weakest group", survivingGroups.contains("groupF"));

        // Verify scores are in descending order
        float previousScore = Float.MAX_VALUE;
        for (int i = 0; i < result.scoreDocs.length; i++) {
            float currentScore = result.scoreDocs[i].score;
            assertTrue("Scores should be in descending order", currentScore <= previousScore);
            previousScore = currentScore;
        }

        // Verify the top score is from groupA's best doc (0.99)
        assertEquals(0.99f, result.scoreDocs[0].score, 0.001f);

        reader.close();
        writer.close();
        directory.close();
    }

    /**
     * Test that when sorting by field (not score), minScores are NOT updated on eviction.
     */
    public void testCollapse_whenSortByFieldAndGroupsEvicted_thenMinScoresNotUpdated() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        // 10 groups, TOP_N_GROUPS=5, so evictions happen
        for (int i = 0; i < 10; i++) {
            addKeywordDoc(writer, i, "text" + i, 100 + i, "group" + i);
        }
        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        // Sort by INT field, NOT score
        Sort sort = new Sort(new SortField(INT_FIELD_NAME, SortField.Type.INT, false));
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopDocsCollector<?> collector = HybridCollapsingTopDocsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            numHits,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(1);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        List<Float> scores = IntStream.range(0, 10).mapToObj(i -> 1.0f - (i * 0.1f)).collect(Collectors.toList());
        collectDocsAndScores(hybridScorer, scores, leafCollector, 0, IntStream.range(0, 10).toArray());

        // minScores should remain 0 since we're not sorting by score
        assertEquals(0.0f, hybridScorer.getMinScores()[0], 0.0001f);

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();
        assertEquals(1, topDocs.size());
        assertEquals(numHits, topDocs.get(0).scoreDocs.length);

        reader.close();
        writer.close();
        directory.close();
    }

    /**
     * Test that docs with scores below the minScore threshold are skipped during collection.
     */
    public void testCollapse_whenScoreBelowThreshold_thenDocSkipped() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        // 10 unique groups
        for (int i = 0; i < 10; i++) {
            addKeywordDoc(writer, i, "text" + i, 100 + i, "group" + i);
        }
        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        Sort sort = new Sort(SortField.FIELD_SCORE);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopDocsCollector<?> collector = HybridCollapsingTopDocsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            numHits,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(1);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        // First 5: high scores that fill the queue and cause evictions
        // Last 5: very low scores that should be below the evicted threshold
        float[] scores = new float[] { 0.9f, 0.8f, 0.7f, 0.6f, 0.5f, 0.4f, 0.01f, 0.01f, 0.01f, 0.01f };

        for (int i = 0; i < 10; i++) {
            hybridScorer.resetScores();
            hybridScorer.getSubQueryScores()[0] = scores[i];
            leafCollector.collect(i);
        }

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();
        assertEquals(1, topDocs.size());

        CollapseTopFieldDocs result = topDocs.get(0);
        assertEquals(numHits, result.scoreDocs.length);

        // All surviving docs should have scores above the threshold
        for (int i = 0; i < result.scoreDocs.length; i++) {
            assertTrue("Surviving doc score should be above threshold", result.scoreDocs[i].score > 0.1f);
        }

        reader.close();
        writer.close();
        directory.close();
    }

    private void addNumericDoc(IndexWriter writer, int id, String textValue, int intValue, long collapseValue) throws IOException {
        Document doc = new Document();
        // ID field
        doc.add(new NumericDocValuesField("_id", id));
        doc.add(new StoredField("_id", id));

        // Text field
        doc.add(new TextField(TEXT_FIELD_NAME, textValue, Field.Store.YES));

        // Integer field - both stored and doc values for sorting
        doc.add(new StoredField(INT_FIELD_NAME, intValue));
        doc.add(new NumericDocValuesField(INT_FIELD_NAME, intValue));

        // Numeric collapse field
        doc.add(new StoredField(COLLAPSE_FIELD_NAME, collapseValue));
        doc.add(new NumericDocValuesField(COLLAPSE_FIELD_NAME, collapseValue));

        writer.addDocument(doc);
    }

    private void addKeywordDoc(IndexWriter writer, int id, String textValue, int intValue, String collapseValue) throws IOException {
        Document doc = new Document();
        // ID field
        doc.add(new NumericDocValuesField("_id", id));
        doc.add(new StoredField("_id", id));

        // Text field
        doc.add(new TextField(TEXT_FIELD_NAME, textValue, Field.Store.YES));

        // Integer field - both stored and doc values for sorting
        doc.add(new StoredField(INT_FIELD_NAME, intValue));
        doc.add(new NumericDocValuesField(INT_FIELD_NAME, intValue));

        // Collapse field
        doc.add(new TextField(COLLAPSE_FIELD_NAME, collapseValue, Field.Store.YES));
        doc.add(new SortedDocValuesField(COLLAPSE_FIELD_NAME, new BytesRef(collapseValue)));

        writer.addDocument(doc);
    }

    @SneakyThrows
    public void testKeywordCollapse_whenProfilerMode_thenResultsNotEmpty() {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        // setup: index 20 documents across 4 groups
        for (int i = 0; i < 20; i++) {
            addKeywordDoc(writer, i, "text" + i, 100 + i, "group" + (i % 4));
        }
        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        Sort sort = new Sort(SortField.FIELD_SCORE);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopDocsCollector<?> collector = HybridCollapsingTopDocsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            TOP_N_GROUPS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO),
            DOCS_PER_GROUP_PER_SUBQUERY
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        // setup: simulate profiler mode by directly setting hybridQueryScorer and compoundQueryScorer
        Scorer subScorer1 = mock(Scorer.class);
        HybridQueryScorer mockHybridScorer = mock(HybridQueryScorer.class);
        when(mockHybridScorer.getSubScorers()).thenReturn(java.util.Arrays.asList(subScorer1));

        HybridSubQueryScorer adapter = new HybridSubQueryScorer(1);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        HybridLeafCollector hybridLeaf = (HybridLeafCollector) leafCollector;
        hybridLeaf.hybridQueryScorer = mockHybridScorer;
        hybridLeaf.compoundQueryScorer = adapter;

        // execute: collect docs
        for (int doc = 0; doc < 20; doc++) {
            float score = 1.0f + doc * 0.05f;
            when(mockHybridScorer.docID()).thenReturn(doc);
            when(subScorer1.docID()).thenReturn(doc);
            when(subScorer1.score()).thenReturn(score);
            leafCollector.collect(doc);
        }

        // verify: results should not be empty
        List<CollapseTopFieldDocs> topDocs = collector.topDocs();
        assertNotNull(topDocs);
        assertEquals(1, topDocs.size());
        assertTrue("profiler mode should produce non-empty results", topDocs.get(0).scoreDocs.length > 0);
        assertTrue("totalHits should be > 0", topDocs.get(0).totalHits.value() > 0);

        writer.close();
        reader.close();
        directory.close();
    }

}
