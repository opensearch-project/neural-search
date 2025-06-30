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
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.document.DocumentField;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils.setUpClusterService;

public class HybridizationFetchSubPhaseTests extends OpenSearchTestCase {

    private final ScoreNormalizer scoreNormalizer = new ScoreNormalizer();
    private FetchContext mockFetchContext;
    private SearchHit searchHit;
    private FetchSubPhase.HitContext hitContext;
    private static final SearchShard SEARCH_SHARD = new SearchShard("my_index", 0, "12345678");
    private QueryShardContext mockQueryShardContext;

    public void testHybridizationScoreFieldIsSet() throws IOException {
        setUpClusterService();
        mockFetchContext = mock(FetchContext.class);
        mockQueryShardContext = mock(QueryShardContext.class);
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

                SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
                final List<CompoundTopDocs> queryTopDocs = List.of(
                    new CompoundTopDocs(
                        new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                        List.of(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 2.0f) })),
                        false,
                        SEARCH_SHARD
                    )
                );

                SearchRequest searchRequest = mock(SearchRequest.class);
                when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                when(searchPhaseContext.getRequest().source()).thenReturn(searchSourceBuilder);

                NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
                    .queryTopDocs(queryTopDocs)
                    .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
                    .subQueryScores(true)
                    .searchPhaseContext(searchPhaseContext)
                    .build();

                ScoreNormalizationFactory scoreNormalizationFactory = new ScoreNormalizationFactory();
                ScoreNormalizationTechnique normalizationTechnique = scoreNormalizationFactory.createNormalization("min_max", Map.of());

                normalizationTechnique.normalize(normalizeScoresDTO);

                scoreNormalizer.normalizeScores(normalizeScoresDTO);

                float[] scores = new float[] { 0.4f, 0.6f };
                String key = SEARCH_SHARD.getShardId() + "_" + docId;
                HybridScoreRegistry.store(searchPhaseContext, Map.of(key, scores));

                // Run processor
                when(mockFetchContext.getQueryShardContext()).thenReturn(mockQueryShardContext);
                when(mockQueryShardContext.getShardId()).thenReturn(SEARCH_SHARD.getShardId());
                FetchSubPhaseProcessor processor = subPhase.getProcessor(mockFetchContext);
                processor.setNextReader(leafReaderContext);
                processor.process(hitContext);

                DocumentField field = hitContext.hit().field("hybridization_sub_query_scores");
                assertNotNull(String.valueOf(field), "Expected _hybridization field to be present");
                assertEquals(2, field.getValues().size());
                assertEquals(scores[0], field.getValues().get(0));
                assertEquals(scores[1], field.getValues().get(1));
            }
        }
    }

    public void testNoHybridizationFieldWhenMapIsNull() throws IOException {
        setUpClusterService();
        mockFetchContext = mock(FetchContext.class);
        mockQueryShardContext = mock(QueryShardContext.class);
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
                writer.addDocument(new Document());
            }

            SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);

            HybridizationFetchSubPhase subPhase = new HybridizationFetchSubPhase();

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReaderContext leafReaderContext = reader.leaves().get(0);
                int docId = 0;

                searchHit = new SearchHit(docId);
                hitContext = new FetchSubPhase.HitContext(searchHit, leafReaderContext, docId, new SourceLookup());

                final List<CompoundTopDocs> queryTopDocs = List.of(
                    new CompoundTopDocs(
                        new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                        List.of(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 2.0f) })),
                        false,
                        SEARCH_SHARD
                    )
                );
                SearchRequest searchRequest = mock(SearchRequest.class);
                when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                when(searchPhaseContext.getRequest().source()).thenReturn(searchSourceBuilder);

                NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
                    .queryTopDocs(queryTopDocs)
                    .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
                    .subQueryScores(true)
                    .searchPhaseContext(searchPhaseContext)
                    .build();

                ScoreNormalizationFactory scoreNormalizationFactory = new ScoreNormalizationFactory();
                ScoreNormalizationTechnique normalizationTechnique = scoreNormalizationFactory.createNormalization("min_max", Map.of());

                normalizationTechnique.normalize(normalizeScoresDTO);

                scoreNormalizer.normalizeScores(normalizeScoresDTO);

                when(mockFetchContext.getQueryShardContext()).thenReturn(mockQueryShardContext);
                when(mockQueryShardContext.getShardId()).thenReturn(SEARCH_SHARD.getShardId());
                FetchSubPhaseProcessor processor = subPhase.getProcessor(mockFetchContext);
                processor.setNextReader(leafReaderContext);
                processor.process(hitContext);

                assertNull(hitContext.hit().field("hybridization_sub_query_scores"));
            }
        }
    }

    public void testNoHybridizationFieldWhenDocIdNotInMap() throws IOException {
        setUpClusterService();
        mockFetchContext = mock(FetchContext.class);
        mockQueryShardContext = mock(QueryShardContext.class);
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
                writer.addDocument(new Document());
            }

            HybridizationFetchSubPhase subPhase = new HybridizationFetchSubPhase();

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReaderContext leafReaderContext = reader.leaves().get(0);
                int docId = 5;

                searchHit = new SearchHit(docId);
                hitContext = new FetchSubPhase.HitContext(searchHit, leafReaderContext, docId, new SourceLookup());

                SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);

                // docId 0 not in hit
                final List<CompoundTopDocs> queryTopDocs = List.of(
                    new CompoundTopDocs(
                        new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                        List.of(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 2.0f) })),
                        false,
                        SEARCH_SHARD
                    )
                );
                SearchRequest searchRequest = mock(SearchRequest.class);
                when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                when(searchPhaseContext.getRequest().source()).thenReturn(searchSourceBuilder);

                NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
                    .queryTopDocs(queryTopDocs)
                    .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
                    .subQueryScores(true)
                    .searchPhaseContext(searchPhaseContext)
                    .build();
                scoreNormalizer.normalizeScores(normalizeScoresDTO);

                float[] scores = new float[] { 0.4f, 0.6f };
                String key = SEARCH_SHARD.getShardId() + "_" + "1";
                HybridScoreRegistry.store(searchPhaseContext, Map.of(key, scores));

                when(mockFetchContext.getQueryShardContext()).thenReturn(mockQueryShardContext);
                when(mockQueryShardContext.getShardId()).thenReturn(SEARCH_SHARD.getShardId());
                FetchSubPhaseProcessor processor = subPhase.getProcessor(mockFetchContext);
                processor.setNextReader(leafReaderContext);
                processor.process(hitContext);

                // Check hit context has docId 0 set
                assertEquals(5, hitContext.hit().docId());
                // Check hit does not have _hybridization field as there are no subquery scores for doc 0
                assertNull(hitContext.hit().field("hybridization_sub_query_scores"));
            }
        }
    }
}
