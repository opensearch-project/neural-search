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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.neuralsearch.query.HybridQueryScorer;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HybridCollapsingTopDocsCollectorTests extends OpenSearchQueryTestCase {

    private static final String TEXT_FIELD_NAME = "field";
    private static final String INT_FIELD_NAME = "integerField";
    private static final String COLLAPSE_FIELD_NAME = "collapseField";
    private static final String TEST_QUERY_TEXT = "greeting";
    private static final String TEST_QUERY_TEXT2 = "salute";
    private static final int NUM_DOCS = 4;
    private static final int TOP_N_GROUPS = 2;
    private static final int TOTAL_HITS_UP_TO = 1000;

    public void testKeywordCollapse_whenCollectAndTopDocs_thenSuccessful() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));

        addKeywordDoc(writer, 0, "text1", 100, "group1");
        addKeywordDoc(writer, 1, "text2", 200, "group1");
        addKeywordDoc(writer, 2, "text3", 300, "group2");
        addKeywordDoc(writer, 3, "text4", 400, "group2");
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);
        LeafReaderContext context = reader.leaves().get(0);

        Sort sort = new Sort(SortField.FIELD_SCORE);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(COLLAPSE_FIELD_NAME);

        HybridCollapsingTopDocsCollector<?> collector = HybridCollapsingTopDocsCollector.createKeyword(
            COLLAPSE_FIELD_NAME,
            fieldType,
            sort,
            TOP_N_GROUPS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);
        LeafCollector leafCollector = collector.getLeafCollector(context);

        int[] docIds = new int[] { 0, 1, 2, 3 };
        List<Float> scores1 = Arrays.asList(0.5f, 0.7f, 0.3f, 0.9f);
        List<Float> scores2 = Arrays.asList(0.6f, 0.4f, 0.8f, 0.2f);

        QueryShardContext mockContext = mockQueryShardContext();

        HybridQueryScorer hybridScorer = new HybridQueryScorer(
            Arrays.asList(
                collapseScorer(docIds, scores1, fakeWeight(QueryBuilders.termQuery(TEXT_FIELD_NAME, TEST_QUERY_TEXT).toQuery(mockContext))),
                collapseScorer(docIds, scores2, fakeWeight(QueryBuilders.termQuery(TEXT_FIELD_NAME, TEST_QUERY_TEXT2).toQuery(mockContext)))
            )
        );

        leafCollector.setScorer(hybridScorer);

        DocIdSetIterator iterator = hybridScorer.iterator();
        int doc;
        while ((doc = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            leafCollector.collect(doc);
        }

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();

        assertEquals(2, topDocs.size());  // One for each sub-query

        for (int i = 0; i < topDocs.size(); i++) {
            CollapseTopFieldDocs collapseTopFieldDocs = topDocs.get(i);
            assertEquals(4, collapseTopFieldDocs.totalHits.value());
            assertEquals(4, collapseTopFieldDocs.scoreDocs.length);

            // Verify collapse values
            Set<String> uniqueGroups = new HashSet<>();
            for (Object collapseValue : collapseTopFieldDocs.collapseValues) {
                uniqueGroups.add(((BytesRef) collapseValue).utf8ToString());
            }
            assertEquals(2, uniqueGroups.size());
            assertTrue(uniqueGroups.contains("group1"));
            assertTrue(uniqueGroups.contains("group2"));
        }

        reader.close();
        writer.close();
        directory.close();
    }

    public void testNumericCollapse_whenCollectAndTopDocs_thenSuccessful() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));

        // Add documents with numeric collapse field values
        addNumericDoc(writer, 0, "text1", 100, 1);
        addNumericDoc(writer, 1, "text2", 200, 1);
        addNumericDoc(writer, 2, "text3", 300, 2);
        addNumericDoc(writer, 3, "text4", 400, 2);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);
        LeafReaderContext context = reader.leaves().get(0);

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
            new HitsThresholdChecker(TOTAL_HITS_UP_TO)
        );

        Weight weight = mock(Weight.class);
        collector.setWeight(weight);
        LeafCollector leafCollector = collector.getLeafCollector(context);

        int[] docIds = new int[] { 0, 1, 2, 3 };
        List<Float> scores1 = Stream.generate(() -> random().nextFloat()).limit(NUM_DOCS).collect(Collectors.toList());
        List<Float> scores2 = Stream.generate(() -> random().nextFloat()).limit(NUM_DOCS).collect(Collectors.toList());

        QueryShardContext mockContext = mockQueryShardContext();

        HybridQueryScorer hybridScorer = new HybridQueryScorer(
            Arrays.asList(
                collapseScorer(docIds, scores1, fakeWeight(QueryBuilders.termQuery(TEXT_FIELD_NAME, TEST_QUERY_TEXT).toQuery(mockContext))),
                collapseScorer(docIds, scores2, fakeWeight(QueryBuilders.termQuery(TEXT_FIELD_NAME, TEST_QUERY_TEXT2).toQuery(mockContext)))
            )
        );

        leafCollector.setScorer(hybridScorer);

        DocIdSetIterator iterator = hybridScorer.iterator();
        int doc;
        while ((doc = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            leafCollector.collect(doc);
        }

        List<CollapseTopFieldDocs> topDocs = collector.topDocs();

        assertEquals(2, topDocs.size());  // One for each sub-query

        for (CollapseTopFieldDocs collapseTopFieldDocs : topDocs) {
            assertEquals(4, collapseTopFieldDocs.totalHits.value());
            assertEquals(4, collapseTopFieldDocs.scoreDocs.length);

            // Verify collapse values
            Set<Long> uniqueGroups = new HashSet<>();
            for (Object collapseValue : collapseTopFieldDocs.collapseValues) {
                uniqueGroups.add((Long) (collapseValue));
            }
            assertEquals(2, uniqueGroups.size());
            assertTrue(uniqueGroups.contains(1L));
            assertTrue(uniqueGroups.contains(2L));
        }

        reader.close();
        writer.close();
        directory.close();
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

    private QueryShardContext mockQueryShardContext() throws IOException {
        QueryShardContext mockContext = mock(QueryShardContext.class);
        MappedFieldType fieldType = createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        return mockContext;
    }

}
