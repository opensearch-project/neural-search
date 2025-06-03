/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.junit.Before;
import org.opensearch.common.document.DocumentField;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class HybridizationFetchSubPhaseTests extends OpenSearchTestCase {

    private ScoreNormalizer scoreNormalizer;
    private HybridScoreRegistry hybridScoreRegistry;
    private FetchContext mockFetchContext;
    private SearchHit searchHit;
    private FetchSubPhase.HitContext hitContext;
    private static final SearchShard SEARCH_SHARD = new SearchShard("my_index", 0, "12345678");

    @Before
    public void setUpMocks() {
        mockFetchContext = mock(FetchContext.class);
        scoreNormalizer = new ScoreNormalizer();
        hybridScoreRegistry = new HybridScoreRegistry();
    }

    public void testHybridizationScoreFieldIsSet() throws IOException {
        try (Directory directory = newDirectory()) {
            // Create dummy Lucene doc
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
                Document doc = new Document();
                doc.add(new NumericDocValuesField("dummy_field", 1));
                writer.addDocument(doc);
            }

            // Prepare subphase
            HybridizationFetchSubPhase subPhase = new HybridizationFetchSubPhase();

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReaderContext leafReaderContext = reader.leaves().get(0);
                int docId = 0;

                searchHit = new SearchHit(docId);
                hitContext = new FetchSubPhase.HitContext(searchHit, leafReaderContext, docId, new SourceLookup());

                // Mock ScoreNormalizer and HybridScoreRegistry
                var mockSearchContext = mock(SearchContext.class);
                // mockedNormalizer.when(ScoreNormalizer::getSearchContext).thenReturn(mockSearchContext);
                final List<CompoundTopDocs> queryTopDocs = List.of(
                    new CompoundTopDocs(
                        new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                        List.of(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 2.0f) })),
                        false,
                        SEARCH_SHARD
                    )
                );
                NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
                    .queryTopDocs(queryTopDocs)
                    .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
                    .build();
                scoreNormalizer.normalizeScores(normalizeScoresDTO, mockSearchContext);

                float[] scores = new float[] { 0.4f, 0.6f };
                hybridScoreRegistry.store(mockSearchContext, Map.of(docId, scores));

                // Run processor
                FetchSubPhaseProcessor processor = subPhase.getProcessor(mockFetchContext);
                processor.setNextReader(leafReaderContext);
                processor.process(hitContext);

                DocumentField field = hitContext.hit().field("_hybridization");
                assertNotNull(String.valueOf(field), "Expected _hybridization field to be present");
                assertEquals(1, field.getValues().size());
                assertSame(scores, field.getValues().get(0));  // Check reference
            }
        }
    }

    public void testNoHybridizationFieldWhenMapIsNull() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
                writer.addDocument(new Document());
            }

            HybridizationFetchSubPhase subPhase = new HybridizationFetchSubPhase();

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReaderContext leafReaderContext = reader.leaves().get(0);
                int docId = 0;

                searchHit = new SearchHit(docId);
                hitContext = new FetchSubPhase.HitContext(searchHit, leafReaderContext, docId, new SourceLookup());

                var mockSearchContext = mock(SearchContext.class);

                NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
                    .queryTopDocs(List.of())
                    .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
                    .build();
                scoreNormalizer.normalizeScores(normalizeScoresDTO, mockSearchContext);

                FetchSubPhaseProcessor processor = subPhase.getProcessor(mockFetchContext);
                processor.setNextReader(leafReaderContext);
                processor.process(hitContext);

                assertNull(hitContext.hit().field("_hybridization"));
            }
        }
    }

    public void testNoHybridizationFieldWhenDocIdNotInMap() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
                writer.addDocument(new Document());
            }

            HybridizationFetchSubPhase subPhase = new HybridizationFetchSubPhase();

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReaderContext leafReaderContext = reader.leaves().get(0);
                int docId = 0;

                searchHit = new SearchHit(docId);
                hitContext = new FetchSubPhase.HitContext(searchHit, leafReaderContext, docId, new SourceLookup());

                var mockSearchContext = mock(SearchContext.class);

                // docId 0 not in hit
                final List<CompoundTopDocs> queryTopDocs = List.of(
                    new CompoundTopDocs(
                        new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                        List.of(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(docId, 2.0f) })),
                        false,
                        SEARCH_SHARD
                    )
                );
                NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
                    .queryTopDocs(queryTopDocs)
                    .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
                    .build();
                scoreNormalizer.normalizeScores(normalizeScoresDTO, mockSearchContext);

                float[] scores = new float[] { 0.4f, 0.6f };
                hybridScoreRegistry.store(mockSearchContext, Map.of(docId, scores));

                FetchSubPhaseProcessor processor = subPhase.getProcessor(mockFetchContext);
                processor.setNextReader(leafReaderContext);
                processor.process(hitContext);

                assertEquals(1, hitContext.hit().docId());
            }
        }
    }
}
