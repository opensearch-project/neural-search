/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.commons.lang3.RandomUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.neuralsearch.query.HybridQueryScorer;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

public class HybridTopFieldDocSortCollectorTests extends OpenSearchQueryTestCase {
    static final String TEXT_FIELD_NAME = "field";
    static final String INT_FIELD_NAME = "integerField";
    private static final String TEST_QUERY_TEXT = "greeting";
    private static final String TEST_QUERY_TEXT2 = "salute";
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
        SortField sortField = new SortField("doc", SortField.Type.DOC);
        HybridTopFieldDocSortCollector.SimpleFieldCollector simpleFieldCollector = new HybridTopFieldDocSortCollector.SimpleFieldCollector(
            NUM_DOCS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO),
            new Sort(sortField)
        );
        Weight weight = mock(Weight.class);
        simpleFieldCollector.setWeight(weight);
        LeafCollector leafCollector = simpleFieldCollector.getLeafCollector(leafReaderContext);
        assertNotNull(leafCollector);

        int[] docIdsForQuery1 = new int[] { DOC_ID_1, DOC_ID_2 };
        int[] docIdsForQuery2 = new int[] { DOC_ID_3 };
        final List<Float> scores1 = Stream.generate(() -> random().nextFloat()).limit(NUM_DOCS).collect(Collectors.toList());
        final List<Float> scores2 = Stream.generate(() -> random().nextFloat()).limit(NUM_DOCS).collect(Collectors.toList());
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);

        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(
            weight,
            Arrays.asList(
                scorer(
                    docIdsForQuery1,
                    scores1,
                    fakeWeight(QueryBuilders.termQuery(TEXT_FIELD_NAME, TEST_QUERY_TEXT).toQuery(mockQueryShardContext))
                ),
                scorer(
                    docIdsForQuery2,
                    scores2,
                    fakeWeight(QueryBuilders.termQuery(TEXT_FIELD_NAME, TEST_QUERY_TEXT2).toQuery(mockQueryShardContext))
                )
            )
        );

        leafCollector.setScorer(hybridQueryScorer);
        DocIdSetIterator iterator = hybridQueryScorer.iterator();

        int doc = iterator.nextDoc();
        while (doc != DocIdSetIterator.NO_MORE_DOCS) {
            leafCollector.collect(doc);
            doc = iterator.nextDoc();
        }

        List<TopFieldDocs> topFieldDocs = simpleFieldCollector.topDocs();

        assertNotNull(topFieldDocs);
        // assertEquals(2, topDocs.size());
        w.close();
        reader.close();
        directory.close();
    }

}
