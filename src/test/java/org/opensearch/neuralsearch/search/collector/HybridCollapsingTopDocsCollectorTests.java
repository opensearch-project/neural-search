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
            assertEquals(1000, collapseTopFieldDocs.totalHits.value());
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
            assertEquals(1000, collapseTopFieldDocs.totalHits.value());
            assertEquals(TOP_N_GROUPS * TOP_N_GROUPS, collapseTopFieldDocs.scoreDocs.length);

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
            assertEquals(1000, collapseTopFieldDocs.totalHits.value());
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
