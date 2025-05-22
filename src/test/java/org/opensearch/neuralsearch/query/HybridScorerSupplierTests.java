/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.query.HybridQueryBuilderTests.TEXT_FIELD_NAME;

public class HybridScorerSupplierTests extends OpenSearchQueryTestCase {

    static final String TERM_QUERY_TEXT = "keyword";

    private HybridQueryWeight weight;
    private LeafReaderContext context;
    private ScoreMode scoreMode;

    private Directory directory;
    private IndexWriter w;
    private IndexReader reader;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        directory = newDirectory();
        w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();
        int docId = RandomizedTest.randomInt();
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId, TERM_QUERY_TEXT, ft));
        w.commit();

        reader = DirectoryReader.open(w);
        HybridQuery hybridQuery = new HybridQuery(
            List.of(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext)),
            new HybridQueryContext(10)
        );
        IndexSearcher searcher = newSearcher(reader);
        weight = (HybridQueryWeight) hybridQuery.createWeight(searcher, ScoreMode.TOP_SCORES, 1.0f);
        context = searcher.getIndexReader().leaves().get(0);
        scoreMode = ScoreMode.COMPLETE;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        w.close();
        reader.close();
        directory.close();
    }

    public void testGetWithEmptyScorers() throws IOException {
        HybridScorerSupplier hybridScorerSupplier = new HybridScorerSupplier(Collections.emptyList(), weight, scoreMode, context);

        expectThrows(IllegalArgumentException.class, () -> hybridScorerSupplier.get(randomLong()));
    }

    public void testGetWithNullScorer() throws IOException {
        List<ScorerSupplier> scorerSuppliers = Arrays.asList(null, createMockScorerSupplier());

        HybridScorerSupplier hybridScorerSupplier = new HybridScorerSupplier(scorerSuppliers, weight, scoreMode, context);

        Scorer scorer = hybridScorerSupplier.get(randomLong());
        assertNotNull(scorer);
        assertTrue(scorer instanceof HybridQueryScorer);
    }

    public void testGetWithValidScorers() throws IOException {
        List<ScorerSupplier> scorerSuppliers = Arrays.asList(createMockScorerSupplier(), createMockScorerSupplier());

        HybridScorerSupplier hybridScorerSupplier = new HybridScorerSupplier(scorerSuppliers, weight, scoreMode, context);

        Scorer scorer = hybridScorerSupplier.get(randomLong());
        assertNotNull(scorer);
        assertTrue(scorer instanceof HybridQueryScorer);
    }

    public void testCostCalculation() {
        ScorerSupplier supplier1 = createMockScorerSupplier(100L);
        ScorerSupplier supplier2 = createMockScorerSupplier(200L);
        List<ScorerSupplier> scorerSuppliers = Arrays.asList(supplier1, supplier2);

        HybridScorerSupplier hybridScorerSupplier = new HybridScorerSupplier(scorerSuppliers, weight, scoreMode, context);

        assertEquals(300L, hybridScorerSupplier.cost());
        // assert caching - should return same value without recalculating
        assertEquals(300L, hybridScorerSupplier.cost());
    }

    public void testCostCalculationWithNullScorer() {
        ScorerSupplier supplier = createMockScorerSupplier(100L);
        List<ScorerSupplier> scorerSuppliers = Arrays.asList(null, supplier);

        HybridScorerSupplier hybridScorerSupplier = new HybridScorerSupplier(scorerSuppliers, weight, scoreMode, context);

        assertEquals(100L, hybridScorerSupplier.cost());
    }

    private ScorerSupplier createMockScorerSupplier() throws IOException {
        return createMockScorerSupplier(randomLong());
    }

    private ScorerSupplier createMockScorerSupplier(long cost) {
        ScorerSupplier scorerSupplier = mock(ScorerSupplier.class);
        Scorer scorer = mock(Scorer.class);
        DocIdSetIterator docIdSetIterator = mock(DocIdSetIterator.class);
        try {
            when(docIdSetIterator.cost()).thenReturn(cost);
            when(scorer.iterator()).thenReturn(docIdSetIterator);
            when(scorerSupplier.get(anyLong())).thenReturn(scorer);
            when(scorerSupplier.cost()).thenReturn(cost);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return scorerSupplier;
    }
}
