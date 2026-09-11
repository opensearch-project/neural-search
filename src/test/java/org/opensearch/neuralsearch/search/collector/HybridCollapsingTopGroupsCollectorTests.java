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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
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
import org.opensearch.neuralsearch.query.HybridQueryScorer;
import org.opensearch.neuralsearch.query.HybridSubQueryScorer;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;

public class HybridCollapsingTopGroupsCollectorTests extends HybridCollectorTestCase {

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

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
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
            // totalHits counts all docs with score > 0 per sub-query
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

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createNumeric(
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
            // totalHits counts all docs with score > 0 per sub-query
            assertTrue(collapseTopFieldDocs.totalHits.value() >= 999);
            assertEquals(numHits, collapseTopFieldDocs.scoreDocs.length);
        }

        reader.close();
        writer.close();
        directory.close();
    }

    public void testCollapse_whenDefaultConfig_thenCollectsSuccessfully() throws IOException {
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

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
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
            // totalHits counts all docs with score > 0 per sub-query
            assertTrue(collapseTopFieldDocs.totalHits.value() >= 999);
            assertEquals(numHits, collapseTopFieldDocs.scoreDocs.length);
        }

        reader.close();
        writer.close();
        directory.close();
    }

    public void testCollapse_whenManyDocsInSameGroup_thenGroupsCollapsedCorrectly() throws IOException {
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

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
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

        // Many docs compete within group0 for its single representative slot
        collectDocsAndScores(hybridScorer, scores, leafCollector, 0, docIds);

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();

        assertEquals(1, topDocs.size());  // One for each sub-query

        for (CollapseTopFieldDocs collapseTopFieldDocs : topDocs) {
            // totalHits counts all docs with score > 0 per sub-query
            assertEquals(100, collapseTopFieldDocs.totalHits.value());

            // 5 distinct groups (group0..group4) and topNGroups=10, so exactly one result per group
            assertEquals("Should have one result per distinct group", 5, collapseTopFieldDocs.scoreDocs.length);
            Set<String> distinctGroups = new HashSet<>();
            for (Object cv : collapseTopFieldDocs.collapseValues) {
                distinctGroups.add(((BytesRef) cv).utf8ToString());
            }
            assertEquals("Collapse values must contain no duplicate groups", 5, distinctGroups.size());
        }

        reader.close();
        writer.close();
        directory.close();
    }

    public void testCollapse_whenAllDocsInSameGroup_thenSingleGroupReturned() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        // Add documents where ALL documents map to the same group
        // This maximizes the pressure on the group bookkeeping
        for (int i = 0; i < 20; i++) {
            addKeywordDoc(writer, i, "text" + i, 100 + i, "samegroup");
        }
        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        Sort sort = new Sort(SortField.FIELD_SCORE);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
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

        collectDocsAndScores(hybridScorer, scores, leafCollector, 0, docIds);

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();

        assertEquals(1, topDocs.size());

        float bestScore = scores.stream().max(Float::compare).orElseThrow();
        for (CollapseTopFieldDocs collapseTopFieldDocs : topDocs) {
            // totalHits counts all docs with score > 0 per sub-query
            assertEquals(20, collapseTopFieldDocs.totalHits.value());

            assertEquals("Should have exactly 1 doc — one per distinct group", 1, collapseTopFieldDocs.scoreDocs.length);
            assertEquals("Should have exactly 1 collapse value", 1, collapseTopFieldDocs.collapseValues.length);
            assertEquals("samegroup", ((BytesRef) collapseTopFieldDocs.collapseValues[0]).utf8ToString());
            assertEquals(bestScore, collapseTopFieldDocs.scoreDocs[0].score, 0.001f);
        }

        reader.close();
        writer.close();
        directory.close();
    }

    /**
     * Test sorting by score with collapse - election reads the compound scorer's summed score
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

        // Sort by SCORE, election reads the summed score from the compound scorer
        Sort sort = new Sort(SortField.FIELD_SCORE);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
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

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
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

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
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

        // Election is leg-independent over the summed scores, so every group's representative is an even
        // doc (sum 1.4 beats odd docs' 1.3). Sub-queries 0 and 2 matched those representatives and report
        // their own scores for them; sub-query 1 matched only odd docs, so its list is empty while its
        // total hits still count the docs it matched.
        assertTrue("Sub-query 0 should have results", topDocs.get(0).scoreDocs.length > 0);
        assertEquals("Sub-query 1 matched no elected representative", 0, topDocs.get(1).scoreDocs.length);
        assertTrue("Sub-query 2 should have results", topDocs.get(2).scoreDocs.length > 0);
        for (int i = 0; i < 3; i++) {
            assertEquals("Sub-query " + i + " total hits counts its own matches", 50 + (i / 2) * 50, topDocs.get(i).totalHits.value());
        }

        // Sub-queries with results emit the same representatives with their own scores
        assertEquals(topDocs.get(0).scoreDocs.length, topDocs.get(2).scoreDocs.length);
        for (int i = 0; i < topDocs.get(0).scoreDocs.length; i++) {
            assertEquals(topDocs.get(0).scoreDocs[i].doc, topDocs.get(2).scoreDocs[i].doc);
            assertEquals(0.9f, topDocs.get(0).scoreDocs[i].score, 0.001f);
            assertEquals(0.5f, topDocs.get(2).scoreDocs[i].score, 0.001f);
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

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
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

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
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

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
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

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
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

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
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
    public void testCollapse_whenGroupsEvictedSortByScore_thenMinScoresNotPropagated() throws IOException {
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

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
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
        // First 5 (0.1-0.5) fill the group map, then docs 6-10 (0.6-1.0) evict weaker groups
        for (int i = 0; i < 10; i++) {
            hybridScorer.resetScores();
            hybridScorer.getSubQueryScores()[0] = 0.1f + (i * 0.1f);
            leafCollector.collect(i);
        }

        // Election is leg-independent, so no per-sub-query competitive threshold is propagated on eviction:
        // a doc with a low score in one sub-query can still be a group's representative through its other
        // sub-query scores, and a per-sub-query threshold would suppress scores the representative needs.
        assertEquals("minScores must not be propagated by group evictions", 0.0f, hybridScorer.getMinScores()[0], 0.0001f);

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

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
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
        Map<String, float[]> scoresByGroup = Map.of(
            "groupA",
            new float[] { 0.1f, 0.95f, 0.99f },
            "groupB",
            new float[] { 0.9f },
            "groupC",
            new float[] { 0.8f },
            "groupD",
            new float[] { 0.7f },
            "groupE",
            new float[] { 0.6f },
            "groupF",
            new float[] { 0.5f }
        );
        collectWithGroupDerivedScores(context, leafCollector, hybridScorer, scoresByGroup);

        // Should not throw NPE or any exception
        List<CollapseTopFieldDocs> topDocs = collector.topDocs();
        assertEquals(1, topDocs.size());
        CollapseTopFieldDocs result = topDocs.get(0);
        // 6 unique groups, numHits=5, so exactly 5 groups survive
        assertEquals(numHits, result.collapseValues.length);

        List<String> survivingGroupList = new ArrayList<>();
        for (Object cv : result.collapseValues) {
            survivingGroupList.add(((BytesRef) cv).utf8ToString());
        }
        Set<String> survivingGroups = new HashSet<>(survivingGroupList);

        assertEquals("Collapse values must contain no duplicate groups: " + survivingGroupList, numHits, survivingGroups.size());

        // Verify groupA survived (it re-entered with 0.95 and 0.99)
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
     * Reproduces https://github.com/opensearch-project/neural-search/issues/1947: a group owning
     * multiple top-scoring docs must not crowd out other distinct groups.
     */
    public void testCollapse_whenGroupOwnsMultipleTopSlots_thenDistinctGroupsReturned() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        addKeywordDoc(writer, 0, "text0", 100, "groupA");
        addKeywordDoc(writer, 1, "text1", 101, "groupB");
        addKeywordDoc(writer, 2, "text2", 102, "groupC");
        addKeywordDoc(writer, 3, "text3", 103, "groupD");
        addKeywordDoc(writer, 4, "text4", 104, "groupE");
        addKeywordDoc(writer, 5, "text5", 105, "groupF");
        addKeywordDoc(writer, 6, "text6", 106, "groupA");
        addKeywordDoc(writer, 7, "text7", 107, "groupA");

        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        Sort sort = new Sort(SortField.FIELD_SCORE);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
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

        Map<String, float[]> scoresByGroup = Map.of(
            "groupA",
            new float[] { 0.10f, 0.95f, 0.99f },
            "groupB",
            new float[] { 0.90f },
            "groupC",
            new float[] { 0.80f },
            "groupD",
            new float[] { 0.70f },
            "groupE",
            new float[] { 0.60f },
            "groupF",
            new float[] { 0.50f }
        );
        collectWithGroupDerivedScores(context, leafCollector, hybridScorer, scoresByGroup);

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();
        assertEquals(1, topDocs.size());
        CollapseTopFieldDocs result = topDocs.get(0);

        List<String> survivingGroups = new ArrayList<>();
        for (Object cv : result.collapseValues) {
            survivingGroups.add(((BytesRef) cv).utf8ToString());
        }

        assertEquals(
            "Collapse values must contain no duplicate groups, but got: " + survivingGroups,
            survivingGroups.size(),
            new HashSet<>(survivingGroups).size()
        );

        assertEquals(
            "Expected numHits distinct groups, but got: " + survivingGroups,
            Set.of("groupA", "groupB", "groupC", "groupD", "groupE"),
            new HashSet<>(survivingGroups)
        );

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

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
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

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
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

    /**
     * Collects every document, deriving its score from its collapse group value rather than its doc id —
     * randomized merge policies can reorder docs, so doc-id-based scores make a test seed-dependent.
     */
    public void testCollapse_whenLegsDisagreeWithinGroup_thenSameRepresentativeElectedAcrossLegs() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        // groupA has two documents the legs rank differently; groupB has one document
        addKeywordDoc(writer, 0, "text0", 100, "groupA");
        addKeywordDoc(writer, 1, "text1", 101, "groupA");
        addKeywordDoc(writer, 2, "text2", 102, "groupB");
        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        Sort sort = new Sort(SortField.FIELD_SCORE);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            numHits,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(2);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        // Leg 0 prefers doc 0, leg 1 prefers doc 1 — the point of hybrid search. The summed
        // (leg-independent) ranking prefers doc 0 for groupA (1.2 vs 1.0), and ranks groupB
        // (doc 2, sum 1.3) above groupA.
        float[][] scoresByDoc = { { 0.9f, 0.3f }, { 0.4f, 0.6f }, { 0.7f, 0.6f } };
        for (int docId = 0; docId < scoresByDoc.length; docId++) {
            hybridScorer.resetScores();
            hybridScorer.getSubQueryScores()[0] = scoresByDoc[docId][0];
            hybridScorer.getSubQueryScores()[1] = scoresByDoc[docId][1];
            leafCollector.collect(docId);
        }

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();
        assertEquals(2, topDocs.size());

        // Every leg must elect the same representative per group: doc 0 for groupA, doc 2 for groupB.
        // A leg reports its own score for the elected representative.
        for (int leg = 0; leg < 2; leg++) {
            CollapseTopFieldDocs legDocs = topDocs.get(leg);
            Map<Object, Integer> docIdByGroup = new HashMap<>();
            Map<Object, Float> scoreByGroup = new HashMap<>();
            for (int i = 0; i < legDocs.scoreDocs.length; i++) {
                docIdByGroup.put(((BytesRef) legDocs.collapseValues[i]).utf8ToString(), legDocs.scoreDocs[i].doc);
                scoreByGroup.put(((BytesRef) legDocs.collapseValues[i]).utf8ToString(), legDocs.scoreDocs[i].score);
            }
            assertEquals("leg " + leg + " must elect doc 0 for groupA", Integer.valueOf(0), docIdByGroup.get("groupA"));
            assertEquals("leg " + leg + " must elect doc 2 for groupB", Integer.valueOf(2), docIdByGroup.get("groupB"));
            assertEquals("leg " + leg + " must report its own score for groupA", scoresByDoc[0][leg], scoreByGroup.get("groupA"), 0.001f);
            assertEquals("leg " + leg + " must report its own score for groupB", scoresByDoc[2][leg], scoreByGroup.get("groupB"), 0.001f);
        }

        // Emission order is leg-independent: groupB (sum 1.3) ranks above groupA (sum 1.2) in every leg.
        for (int leg = 0; leg < 2; leg++) {
            CollapseTopFieldDocs legDocs = topDocs.get(leg);
            assertEquals("groupB", ((BytesRef) legDocs.collapseValues[0]).utf8ToString());
            assertEquals("groupA", ((BytesRef) legDocs.collapseValues[1]).utf8ToString());
        }

        reader.close();
        writer.close();
        directory.close();
    }

    public void testCollapse_whenGroupStrongInOneLegOnly_thenEvictionUsesSummedScore() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        addKeywordDoc(writer, 0, "text0", 100, "groupX");
        addKeywordDoc(writer, 1, "text1", 101, "groupY");
        addKeywordDoc(writer, 2, "text2", 102, "groupZ");
        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        Sort sort = new Sort(SortField.FIELD_SCORE);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        int topNGroups = 2;
        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            topNGroups,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(2);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        // groupY leads leg 1 (0.5 vs 0.0 and 0.4) but has the weakest sum, so it must be the group evicted
        float[][] scoresByDoc = { { 0.9f, 0.0f }, { 0.1f, 0.5f }, { 0.4f, 0.4f } };
        for (int docId = 0; docId < scoresByDoc.length; docId++) {
            hybridScorer.resetScores();
            hybridScorer.getSubQueryScores()[0] = scoresByDoc[docId][0];
            hybridScorer.getSubQueryScores()[1] = scoresByDoc[docId][1];
            leafCollector.collect(docId);
        }

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();
        for (int leg = 0; leg < 2; leg++) {
            Set<String> survivingGroups = new HashSet<>();
            for (Object cv : topDocs.get(leg).collapseValues) {
                survivingGroups.add(((BytesRef) cv).utf8ToString());
            }
            assertFalse("groupY (sum 0.6) must be evicted in leg " + leg, survivingGroups.contains("groupY"));
        }

        reader.close();
        writer.close();
        directory.close();
    }

    public void testCollapse_whenSortByFieldAndLegsMatchDifferentDocs_thenSameRepresentativeElected() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());

        // groupA: doc 0 has the better (lower) sort value but is matched only by leg 0; doc 1 only by leg 1
        addKeywordDoc(writer, 0, "text0", 100, "groupA");
        addKeywordDoc(writer, 1, "text1", 200, "groupA");
        writer.forceMerge(1);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);

        Sort sort = new Sort(new SortField(INT_FIELD_NAME, SortField.Type.INT, false));
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            numHits,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        HybridSubQueryScorer hybridScorer = new HybridSubQueryScorer(2);

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        leafCollector.setScorer(hybridScorer);

        float[][] scoresByDoc = { { 0.7f, 0.0f }, { 0.0f, 0.8f } };
        for (int docId = 0; docId < scoresByDoc.length; docId++) {
            hybridScorer.resetScores();
            hybridScorer.getSubQueryScores()[0] = scoresByDoc[docId][0];
            hybridScorer.getSubQueryScores()[1] = scoresByDoc[docId][1];
            leafCollector.collect(docId);
        }

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();
        // doc 0 wins the field sort, so it represents groupA everywhere: leg 0 reports it with its own
        // score, leg 1 did not match it and emits nothing
        assertEquals(1, topDocs.get(0).scoreDocs.length);
        assertEquals(0, topDocs.get(0).scoreDocs[0].doc);
        assertEquals(0.7f, topDocs.get(0).scoreDocs[0].score, 0.001f);
        assertEquals(0, topDocs.get(1).scoreDocs.length);
        assertEquals(1, topDocs.get(1).totalHits.value());

        reader.close();
        writer.close();
        directory.close();
    }

    private void collectWithGroupDerivedScores(
        LeafReaderContext context,
        LeafCollector leafCollector,
        HybridSubQueryScorer hybridScorer,
        Map<String, float[]> scoresByGroup
    ) throws IOException {
        Map<String, Integer> nextScoreIndexByGroup = new HashMap<>();
        SortedDocValues collapseDocValues = DocValues.getSorted(context.reader(), COLLAPSE_FIELD_NAME);
        int maxDoc = context.reader().maxDoc();
        for (int doc = 0; doc < maxDoc; doc++) {
            assertTrue(collapseDocValues.advanceExact(doc));
            String group = collapseDocValues.lookupOrd(collapseDocValues.ordValue()).utf8ToString();
            int scoreIndex = nextScoreIndexByGroup.merge(group, 1, Integer::sum) - 1;
            hybridScorer.resetScores();
            hybridScorer.getSubQueryScores()[0] = scoresByGroup.get(group)[scoreIndex];
            leafCollector.collect(doc);
        }
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

        HybridCollapsingTopGroupsCollector<?> collector = HybridCollapsingTopGroupsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            numHits,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);

        // setup: profiler mode, where the leaf collector is handed a HybridQueryScorer instead of a
        // HybridSubQueryScorer and reads the per sub-query scores off it on every collect() call
        int[] docIds = IntStream.range(0, 20).toArray();
        float[] subQueryScores = new float[20];
        for (int doc = 0; doc < 20; doc++) {
            subQueryScores[doc] = 1.0f + doc * 0.05f;
        }
        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(
            java.util.Arrays.asList(scorer(docIds, subQueryScores, mock(Weight.class)))
        );

        LeafReaderContext context = reader.leaves().getFirst();
        LeafCollector leafCollector = collector.getLeafCollector(context);

        leafCollector.setScorer(hybridQueryScorer);

        // execute: collect docs while iterating the hybrid scorer, the way the default bulk scorer does
        DocIdSetIterator iterator = hybridQueryScorer.iterator();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
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
