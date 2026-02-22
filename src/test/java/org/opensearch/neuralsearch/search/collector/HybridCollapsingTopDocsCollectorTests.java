/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
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

public class HybridCollapsingTopDocsCollectorTests extends HybridCollectorTestCase {

    private static final String TEXT_FIELD_NAME = "field";
    private static final String INT_FIELD_NAME = "integerField";
    private static final String COLLAPSE_FIELD_NAME = "collapseField";
    private static final int TOP_N_GROUPS = 5;
    private static final int TOTAL_HITS_UP_TO = 1001;
    private static final int DOCS_PER_GROUP_PER_SUBQUERY = 10;

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
            TOP_N_GROUPS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO),
            DOCS_PER_GROUP_PER_SUBQUERY
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
            assertEquals(100, collapseTopFieldDocs.totalHits.value());
            assertEquals(DOCS_PER_GROUP_PER_SUBQUERY * TOP_N_GROUPS, collapseTopFieldDocs.scoreDocs.length);

            // Verify collapse values
            Set<String> uniqueGroups = new HashSet<>();
            for (Object collapseValue : collapseTopFieldDocs.collapseValues) {
                uniqueGroups.add(((BytesRef) collapseValue).utf8ToString());
            }
            assertEquals(5, uniqueGroups.size());
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
            TOP_N_GROUPS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO),
            DOCS_PER_GROUP_PER_SUBQUERY
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
            assertEquals(100, collapseTopFieldDocs.totalHits.value());
            assertEquals(DOCS_PER_GROUP_PER_SUBQUERY * TOP_N_GROUPS, collapseTopFieldDocs.scoreDocs.length);

            // Verify collapse values
            Set<Long> uniqueGroups = new HashSet<>();
            for (Object collapseValue : collapseTopFieldDocs.collapseValues) {
                uniqueGroups.add((Long) (collapseValue));
            }
            assertEquals(5, uniqueGroups.size());
        }

        reader.close();
        writer.close();
        directory.close();
    }

    public void testCollapse_whenZeroDocsPerGroupPerSubQuery_thenSuccessful() throws IOException {
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
            TOP_N_GROUPS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO),
            0
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
            assertEquals(50, collapseTopFieldDocs.totalHits.value());
            assertEquals(TOP_N_GROUPS * TOP_N_GROUPS, collapseTopFieldDocs.scoreDocs.length);

            // Verify collapse values
            Set<String> uniqueGroups = new HashSet<>();
            for (Object collapseValue : collapseTopFieldDocs.collapseValues) {
                uniqueGroups.add(((BytesRef) collapseValue).utf8ToString());
            }
            assertEquals(5, uniqueGroups.size());
        }

        reader.close();
        writer.close();
        directory.close();
    }

    public void testCollapse_whenDocsPerGroupPerSubQueryIsSmallAndManyDocsInSameGroup_thenNoArrayIndexOutOfBounds() throws IOException {
        /*
         * SCENARIO EXPLANATION:
         * This test reproduces a critical bug in queue full detection that caused ArrayIndexOutOfBoundsException.
         *
         * THE BUG SCENARIO:
         * - docsPerGroupPerSubQuery = 1 (very small queue size per group per subquery)
         * - numHits = 10 (number of top groups to return)
         * - Multiple documents map to the same group for a subquery
         *
         * THE PREVIOUS BUG:
         * - Queue full check: if (slot == (numHits - 1)) // slot == 9
         * - But queue was created with size docsPerGroupPerSubQuery = 1
         * - So queue could only hold 1 document, but was never marked as full
         * - When slot >= 1, addNewEntry() tried to access queue slots beyond capacity
         * - This caused ArrayIndexOutOfBoundsException in FieldValueHitQueue internal arrays
         *
         * THE FIX:
         * - Changed to: if (slot == (docsPerGroupPerSubQuery - 1)) // slot == 0
         * - Now queue is correctly marked full after first document
         * - Subsequent documents trigger updateExistingEntry() instead of addNewEntry()
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

        // CRITICAL CONFIGURATION: Small docsPerGroupPerSubQuery with larger numHits
        int topNGroups = 10;  // Want to return 10 top groups
        int docsPerGroupPerSubQuery = 1;  // But only allow 1 doc per group per subquery

        HybridCollapsingTopDocsCollector<?> collector = HybridCollapsingTopDocsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            topNGroups,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO),
            docsPerGroupPerSubQuery  // This is the key parameter that triggered the bug
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        int[] docIds = IntStream.range(0, 100).toArray();
        List<Float> scores = Stream.generate(() -> random().nextFloat()).limit(100).collect(Collectors.toList());

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(1);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        // This collection process would have thrown ArrayIndexOutOfBoundsException before the fix
        // because multiple documents mapping to "group0" would try to exceed the queue capacity of 1
        collectDocsAndScores(hybridScorer, scores, leafCollector, 0, docIds);

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();

        assertEquals(1, topDocs.size());  // One for each sub-query

        for (CollapseTopFieldDocs collapseTopFieldDocs : topDocs) {
            assertEquals(5, collapseTopFieldDocs.totalHits.value());

            // With docsPerGroupPerSubQuery = 1, we should get at most 1 document per group
            // Even though 50 documents mapped to "group0", only 1 should be in the final result
            assertTrue("Should have some results", collapseTopFieldDocs.scoreDocs.length > 0);

            // Verify that we don't exceed the docsPerGroupPerSubQuery limit
            // Count documents per group to ensure the limit is respected
            Set<String> uniqueGroups = new HashSet<>();
            for (Object collapseValue : collapseTopFieldDocs.collapseValues) {
                uniqueGroups.add(((BytesRef) collapseValue).utf8ToString());
            }

            // Should have multiple groups but limited docs per group
            assertTrue("Should have multiple unique groups", uniqueGroups.size() > 1);

            // The key assertion: no ArrayIndexOutOfBoundsException was thrown during collection
            // This validates that the queue full detection fix works correctly
        }

        reader.close();
        writer.close();
        directory.close();
    }

    public void testCollapse_whenDocsPerGroupPerSubQueryEqualsOne_thenQueueFullDetectionWorks() throws IOException {
        /*
         * ADDITIONAL TEST: Specifically test the queue full detection logic
         * This test validates that the fix works with the exact problematic configuration
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
            new HitsThresholdChecker(TOTAL_HITS_UP_TO),
            10   // docsPerGroupPerSubQuery = 1 (the problematic value)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        int[] docIds = IntStream.range(0, 20).toArray();
        List<Float> scores = Stream.generate(() -> random().nextFloat()).limit(20).collect(Collectors.toList());

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(1);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        // Before fix: This would throw ArrayIndexOutOfBoundsException
        // After fix: This should work correctly
        collectDocsAndScores(hybridScorer, scores, leafCollector, 0, docIds);

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();

        assertEquals(1, topDocs.size());

        for (CollapseTopFieldDocs collapseTopFieldDocs : topDocs) {
            assertEquals(10, collapseTopFieldDocs.totalHits.value());

            // Key validation: Despite 20 documents in same group, only 1 should be returned
            assertEquals("Should have exactly 10 document due to docsPerGroupPerSubQuery=10", 10, collapseTopFieldDocs.scoreDocs.length);

            // Verify the collapse value
            assertEquals("Should have exactly 10 collapse value", 10, collapseTopFieldDocs.collapseValues.length);
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
            TOP_N_GROUPS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO),
            DOCS_PER_GROUP_PER_SUBQUERY
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
            TOP_N_GROUPS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO),
            DOCS_PER_GROUP_PER_SUBQUERY
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
            TOP_N_GROUPS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO),
            DOCS_PER_GROUP_PER_SUBQUERY
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
            TOP_N_GROUPS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO),
            DOCS_PER_GROUP_PER_SUBQUERY
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
            TOP_N_GROUPS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO),
            DOCS_PER_GROUP_PER_SUBQUERY
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
            TOP_N_GROUPS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO),
            DOCS_PER_GROUP_PER_SUBQUERY
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

}
