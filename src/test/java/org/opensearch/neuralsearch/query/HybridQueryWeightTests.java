/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.query.HybridQueryBuilderTests.TEXT_FIELD_NAME;

import java.util.List;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import lombok.SneakyThrows;

public class HybridQueryWeightTests extends OpenSearchQueryTestCase {

    private static final String TERM_QUERY_TEXT = "keyword";
    private static final String RANGE_FIELD = "date _range";
    private static final String FROM_TEXT = "123";
    private static final String TO_TEXT = "456";

    @SneakyThrows
    public void testScorerIterator_whenExecuteQuery_thenScorerIteratorSuccessful() {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();
        int docId = RandomizedTest.randomInt();
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId, TERM_QUERY_TEXT, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        HybridQuery hybridQueryWithTerm = new HybridQuery(
            List.of(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext))
        );
        IndexSearcher searcher = newSearcher(reader);
        Weight weight = hybridQueryWithTerm.createWeight(searcher, ScoreMode.TOP_SCORES, 1.0f);

        assertNotNull(weight);

        LeafReaderContext leafReaderContext = searcher.getIndexReader().leaves().get(0);
        Scorer scorer = weight.scorer(leafReaderContext);

        assertNotNull(scorer);

        DocIdSetIterator iterator = scorer.iterator();
        int actualDoc = iterator.nextDoc();
        int actualDocId = Integer.parseInt(reader.document(actualDoc).getField("id").stringValue());

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
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
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
            )
        );
        IndexSearcher searcher = newSearcher(reader);
        Weight weight = hybridQueryWithTerm.createWeight(searcher, ScoreMode.TOP_SCORES, 1.0f);

        assertNotNull(weight);

        LeafReaderContext leafReaderContext = searcher.getIndexReader().leaves().get(0);
        Scorer scorer = weight.scorer(leafReaderContext);

        assertNotNull(scorer);

        DocIdSetIterator iterator = scorer.iterator();
        int actualDoc = iterator.nextDoc();
        int actualDocId = Integer.parseInt(reader.document(actualDoc).getField("id").stringValue());

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
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();
        int docId = RandomizedTest.randomInt();
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId, RandomizedTest.randomAsciiAlphanumOfLength(8), ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        HybridQuery hybridQueryWithTerm = new HybridQuery(
            List.of(QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT).toQuery(mockQueryShardContext))
        );
        IndexSearcher searcher = newSearcher(reader);
        Weight weight = searcher.createWeight(hybridQueryWithTerm, ScoreMode.COMPLETE, 1.0f);

        assertNotNull(weight);

        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);
        Explanation explanation = weight.explain(leafReaderContext, docId);
        assertNotNull(explanation);

        w.close();
        reader.close();
        directory.close();
    }
}
