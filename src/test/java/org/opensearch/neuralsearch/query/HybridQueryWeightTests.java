/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.query.HybridQueryBuilderTests.TEXT_FIELD_NAME;

import java.util.Arrays;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import lombok.SneakyThrows;

public class HybridQueryWeightTests extends OpenSearchQueryTestCase {

    private static final String TERM_QUERY_TEXT = "keyword";
    private static final String RANGE_FIELD = "date_range";
    private static final String FROM_TEXT = "123";
    private static final String TO_TEXT = "456";

    Directory directory;
    IndexWriter writer;
    DirectoryReader reader;

    @SneakyThrows
    public void testScorerIterator_whenExecuteQuery_thenScorerIteratorSuccessful() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();
        int docId = RandomizedTest.randomInt();
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId, TERM_QUERY_TEXT, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        HybridQuery hybridQueryWithTerm = new HybridQuery(
            List.of(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext)),
            new HybridQueryContext(10)
        );
        IndexSearcher searcher = new IndexSearcher(reader);
        Weight weight = hybridQueryWithTerm.createWeight(searcher, ScoreMode.TOP_SCORES, 1.0f);

        assertNotNull(weight);

        LeafReaderContext leafReaderContext = searcher.getIndexReader().leaves().get(0);
        Scorer scorer = weight.scorer(leafReaderContext);

        assertNotNull(scorer);

        DocIdSetIterator iterator = scorer.iterator();
        int actualDoc = iterator.nextDoc();
        int actualDocId = Integer.parseInt(reader.storedFields().document(actualDoc).getField("id").stringValue());

        assertEquals(docId, actualDocId);

        assertTrue(weight.isCacheable(leafReaderContext));

        Matches matches = weight.matches(leafReaderContext, actualDoc);
        MatchesIterator matchesIterator = matches.getMatches(TEXT_FIELD_NAME);
        assertTrue(matchesIterator.next());

        w.close();
        reader.close();
        directory.close();
    }

    @SneakyThrows
    public void testSubQueries_whenMultipleEqualSubQueries_thenSuccessful() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        when(mockQueryShardContext.convertToShardContext()).thenReturn(mockQueryShardContext);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();
        int docId = RandomizedTest.randomInt();
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId, TERM_QUERY_TEXT, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        HybridQuery hybridQueryWithTerm = new HybridQuery(
            List.of(
                QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext),
                QueryBuilders.rangeQuery(RANGE_FIELD)
                    .from(FROM_TEXT)
                    .to(TO_TEXT)
                    .rewrite(mockQueryShardContext)
                    .rewrite(mockQueryShardContext)
                    .toQuery(mockQueryShardContext),
                QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext)
            ),
            new HybridQueryContext(10)
        );
        IndexSearcher searcher = new IndexSearcher(reader);
        Weight weight = hybridQueryWithTerm.createWeight(searcher, ScoreMode.TOP_SCORES, 1.0f);

        assertNotNull(weight);

        LeafReaderContext leafReaderContext = searcher.getIndexReader().leaves().get(0);
        Scorer scorer = weight.scorer(leafReaderContext);

        assertNotNull(scorer);

        DocIdSetIterator iterator = scorer.iterator();
        int actualDoc = iterator.nextDoc();
        int actualDocId = Integer.parseInt(reader.storedFields().document(actualDoc).getField("id").stringValue());

        assertEquals(docId, actualDocId);

        assertTrue(weight.isCacheable(leafReaderContext));

        Matches matches = weight.matches(leafReaderContext, actualDoc);
        MatchesIterator matchesIterator = matches.getMatches(TEXT_FIELD_NAME);
        assertTrue(matchesIterator.next());

        w.close();
        reader.close();
        directory.close();
    }

    @SneakyThrows
    public void testExplain_whenCallExplain_thenSuccessful() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();
        int docId = RandomizedTest.randomInt();
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId, RandomizedTest.randomAsciiAlphanumOfLength(8), ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        HybridQuery hybridQueryWithTerm = new HybridQuery(
            List.of(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext)),
            new HybridQueryContext(10)
        );
        IndexSearcher searcher = new IndexSearcher(reader);
        Weight weight = searcher.createWeight(hybridQueryWithTerm, ScoreMode.COMPLETE, 1.0f);

        assertNotNull(weight);

        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);
        Explanation explanation = weight.explain(leafReaderContext, docId);
        assertNotNull(explanation);

        w.close();
        reader.close();
        directory.close();
    }

    @SneakyThrows
    public void testExplainWithNonMatchingExplanationsAddedToMatchingClauses() {
        float boost = 1.0f;
        List<Query> queries = Arrays.asList(new TermQuery(new Term("field", "term1")), new TermQuery(new Term("field", "term2")));
        HybridQuery hybridQuery = new HybridQuery(queries, new HybridQueryContext(10));

        // Create a real LeafReaderContext
        LeafReaderContext context = createLeafReaderContext();

        IndexSearcher searcher = mock(IndexSearcher.class);
        HybridQueryWeight weight = new HybridQueryWeight(hybridQuery, searcher, null, boost);

        Weight weight1 = mock(Weight.class);
        Weight weight2 = mock(Weight.class);

        Explanation matchingExplanation = Explanation.match(1.0f, "matching explanation");
        Explanation nonMatchingExplanation = Explanation.noMatch("non-matching explanation");

        when(weight1.explain(any(LeafReaderContext.class), anyInt())).thenReturn(matchingExplanation);
        when(weight2.explain(any(LeafReaderContext.class), anyInt())).thenReturn(nonMatchingExplanation);

        weight.getWeights().clear();
        weight.getWeights().addAll(Arrays.asList(weight1, weight2));

        Explanation explanation = weight.explain(context, 0);

        assertTrue(explanation.isMatch());
        assertEquals(1.0f, explanation.getValue().floatValue(), 0.001f);

        Explanation[] details = explanation.getDetails();
        assertEquals(2, details.length);

        assertEquals(matchingExplanation, details[0]);
        assertTrue(details[0].isMatch());
        assertEquals(1.0f, details[0].getValue().floatValue(), 0.001f);

        assertEquals(nonMatchingExplanation, details[1]);
        assertFalse(details[1].isMatch());

        // Cleanup
        cleanup();
    }

    @SneakyThrows
    public void testExplainWithMultipleMatchingClauses() {
        float boost = 1.0f;
        List<Query> queries = Arrays.asList(
            new TermQuery(new Term("field", "term1")),
            new TermQuery(new Term("field", "term2")),
            new TermQuery(new Term("field", "term3"))
        );
        HybridQuery hybridQuery = new HybridQuery(queries, new HybridQueryContext(10));

        LeafReaderContext context = createLeafReaderContext();
        IndexSearcher searcher = mock(IndexSearcher.class);
        HybridQueryWeight weight = new HybridQueryWeight(hybridQuery, searcher, null, boost);

        Weight weight1 = mock(Weight.class);
        Weight weight2 = mock(Weight.class);
        Weight weight3 = mock(Weight.class);

        Explanation matchingExplanation1 = Explanation.match(1.0f, "matching explanation 1");
        Explanation matchingExplanation2 = Explanation.match(2.0f, "matching explanation 2");
        Explanation nonMatchingExplanation = Explanation.noMatch("non-matching explanation");

        when(weight1.explain(any(LeafReaderContext.class), anyInt())).thenReturn(matchingExplanation1);
        when(weight2.explain(any(LeafReaderContext.class), anyInt())).thenReturn(matchingExplanation2);
        when(weight3.explain(any(LeafReaderContext.class), anyInt())).thenReturn(nonMatchingExplanation);

        weight.getWeights().clear();
        weight.getWeights().addAll(Arrays.asList(weight1, weight2, weight3));

        Explanation explanation = weight.explain(context, 0);

        assertTrue(explanation.isMatch());
        assertEquals(2.0f, explanation.getValue().floatValue(), 0.001f);

        Explanation[] details = explanation.getDetails();
        assertEquals(3, details.length);

        assertEquals(matchingExplanation1, details[0]);
        assertEquals(matchingExplanation2, details[1]);
        assertEquals(nonMatchingExplanation, details[2]);

        cleanup();
    }

    @SneakyThrows
    public void testExplainWithAllNonMatchingClauses() {
        float boost = 1.0f;
        List<Query> queries = Arrays.asList(new TermQuery(new Term("field", "term1")), new TermQuery(new Term("field", "term2")));
        HybridQuery hybridQuery = new HybridQuery(queries, new HybridQueryContext(10));

        LeafReaderContext context = createLeafReaderContext();
        IndexSearcher searcher = mock(IndexSearcher.class);
        HybridQueryWeight weight = new HybridQueryWeight(hybridQuery, searcher, null, boost);

        Weight weight1 = mock(Weight.class);
        Weight weight2 = mock(Weight.class);

        Explanation nonMatchingExplanation1 = Explanation.noMatch("non-matching explanation 1");
        Explanation nonMatchingExplanation2 = Explanation.noMatch("non-matching explanation 2");

        when(weight1.explain(any(LeafReaderContext.class), anyInt())).thenReturn(nonMatchingExplanation1);
        when(weight2.explain(any(LeafReaderContext.class), anyInt())).thenReturn(nonMatchingExplanation2);

        weight.getWeights().clear();
        weight.getWeights().addAll(Arrays.asList(weight1, weight2));

        Explanation explanation = weight.explain(context, 0);

        assertFalse(explanation.isMatch());

        Explanation[] details = explanation.getDetails();
        assertEquals(2, details.length);
        assertEquals(nonMatchingExplanation1, details[0]);
        assertEquals(nonMatchingExplanation2, details[1]);

        cleanup();
    }

    @SneakyThrows
    private LeafReaderContext createLeafReaderContext() {
        Directory directory = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig());
        writer.addDocument(new Document());
        DirectoryReader reader = DirectoryReader.open(writer);
        LeafReaderContext context = reader.leaves().get(0);

        // Store resources for cleanup
        this.directory = directory;
        this.writer = writer;
        this.reader = reader;

        return context;
    }

    @SneakyThrows
    private void cleanup() {
        if (reader != null) reader.close();
        if (writer != null) writer.close();
        if (directory != null) directory.close();
    }
}
