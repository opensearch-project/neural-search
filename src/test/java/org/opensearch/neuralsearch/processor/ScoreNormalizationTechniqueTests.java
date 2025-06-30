/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.List;

import org.apache.commons.lang3.Range;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;

import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import lombok.SneakyThrows;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScoreNormalizationTechniqueTests extends OpenSearchTestCase {

    private static final SearchShard SEARCH_SHARD = new SearchShard("my_index", 0, "12345678");

    public void testEmptyResults_whenEmptyResultsAndDefaultMethod_thenNoProcessing() {
        ScoreNormalizer scoreNormalizationMethod = new ScoreNormalizer();
        NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
            .queryTopDocs(List.of())
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .build();
        scoreNormalizationMethod.normalizeScores(normalizeScoresDTO);
    }

    @SneakyThrows
    public void testNormalization_whenOneSubqueryAndOneShardAndDefaultMethod_thenScoreNormalized() {
        ScoreNormalizer scoreNormalizationMethod = new ScoreNormalizer();
        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
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

        SearchRequest searchRequest = mock(SearchRequest.class);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        when(searchPhaseContext.getRequest().source()).thenReturn(searchSourceBuilder);
        scoreNormalizationMethod.normalizeScores(normalizeScoresDTO);

        scoreNormalizationMethod.normalizeScores(normalizeScoresDTO);
        assertNotNull(queryTopDocs);
        assertEquals(1, queryTopDocs.size());
        CompoundTopDocs resultDoc = queryTopDocs.get(0);
        assertNotNull(resultDoc.getTopDocs());
        assertEquals(1, resultDoc.getTopDocs().size());
        TopDocs topDoc = resultDoc.getTopDocs().get(0);
        assertEquals(1, topDoc.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, topDoc.totalHits.relation());
        assertNotNull(topDoc.scoreDocs);
        assertEquals(1, topDoc.scoreDocs.length);
        ScoreDoc scoreDoc = topDoc.scoreDocs[0];
        assertEquals(1.0, scoreDoc.score, 0.001f);
        assertEquals(1, scoreDoc.doc);
    }

    @SneakyThrows
    public void testNormalization_whenOneSubqueryMultipleHitsAndOneShardAndDefaultMethod_thenScoreNormalized() {
        ScoreNormalizer scoreNormalizationMethod = new ScoreNormalizer();
        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        final List<CompoundTopDocs> queryTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(1, 10.0f), new ScoreDoc(2, 2.5f), new ScoreDoc(4, 0.1f) }
                    )
                ),
                false,
                SEARCH_SHARD
            )
        );
        NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
            .queryTopDocs(queryTopDocs)
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .build();

        SearchRequest searchRequest = mock(SearchRequest.class);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        when(searchPhaseContext.getRequest().source()).thenReturn(searchSourceBuilder);
        scoreNormalizationMethod.normalizeScores(normalizeScoresDTO);

        scoreNormalizationMethod.normalizeScores(normalizeScoresDTO);
        assertNotNull(queryTopDocs);
        assertEquals(1, queryTopDocs.size());
        CompoundTopDocs resultDoc = queryTopDocs.get(0);
        assertNotNull(resultDoc.getTopDocs());
        assertEquals(1, resultDoc.getTopDocs().size());
        TopDocs topDoc = resultDoc.getTopDocs().get(0);
        assertEquals(3, topDoc.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, topDoc.totalHits.relation());
        assertNotNull(topDoc.scoreDocs);
        assertEquals(3, topDoc.scoreDocs.length);
        ScoreDoc highScoreDoc = topDoc.scoreDocs[0];
        assertEquals(1.0, highScoreDoc.score, 0.001f);
        assertEquals(1, highScoreDoc.doc);
        ScoreDoc lowScoreDoc = topDoc.scoreDocs[topDoc.scoreDocs.length - 1];
        assertEquals(0.0, lowScoreDoc.score, 0.001f);
        assertEquals(4, lowScoreDoc.doc);
    }

    public void testNormalization_whenMultipleSubqueriesMultipleHitsAndOneShardAndDefaultMethod_thenScoreNormalized() {
        ScoreNormalizer scoreNormalizationMethod = new ScoreNormalizer();
        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        final List<CompoundTopDocs> queryTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(1, 10.0f), new ScoreDoc(2, 2.5f), new ScoreDoc(4, 0.1f) }
                    ),
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(3, 0.8f), new ScoreDoc(5, 0.5f) }
                    )
                ),
                false,
                SEARCH_SHARD
            )
        );
        NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
            .queryTopDocs(queryTopDocs)
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .build();

        SearchRequest searchRequest = mock(SearchRequest.class);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        when(searchPhaseContext.getRequest().source()).thenReturn(searchSourceBuilder);
        scoreNormalizationMethod.normalizeScores(normalizeScoresDTO);

        scoreNormalizationMethod.normalizeScores(normalizeScoresDTO);

        assertNotNull(queryTopDocs);
        assertEquals(1, queryTopDocs.size());
        CompoundTopDocs resultDoc = queryTopDocs.get(0);
        assertNotNull(resultDoc.getTopDocs());
        assertEquals(2, resultDoc.getTopDocs().size());
        // sub-query one
        TopDocs topDocSubqueryOne = resultDoc.getTopDocs().get(0);
        assertEquals(3, topDocSubqueryOne.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, topDocSubqueryOne.totalHits.relation());
        assertNotNull(topDocSubqueryOne.scoreDocs);
        assertEquals(3, topDocSubqueryOne.scoreDocs.length);
        ScoreDoc highScoreDoc = topDocSubqueryOne.scoreDocs[0];
        assertEquals(1.0, highScoreDoc.score, 0.001f);
        assertEquals(1, highScoreDoc.doc);
        ScoreDoc lowScoreDoc = topDocSubqueryOne.scoreDocs[topDocSubqueryOne.scoreDocs.length - 1];
        assertEquals(0.0, lowScoreDoc.score, 0.001f);
        assertEquals(4, lowScoreDoc.doc);
        // sub-query two
        TopDocs topDocSubqueryTwo = resultDoc.getTopDocs().get(1);
        assertEquals(2, topDocSubqueryTwo.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, topDocSubqueryTwo.totalHits.relation());
        assertNotNull(topDocSubqueryTwo.scoreDocs);
        assertEquals(2, topDocSubqueryTwo.scoreDocs.length);
        assertEquals(1.0, topDocSubqueryTwo.scoreDocs[0].score, 0.001f);
        assertEquals(3, topDocSubqueryTwo.scoreDocs[0].doc);
        assertEquals(0.0, topDocSubqueryTwo.scoreDocs[topDocSubqueryTwo.scoreDocs.length - 1].score, 0.001f);
        assertEquals(5, topDocSubqueryTwo.scoreDocs[topDocSubqueryTwo.scoreDocs.length - 1].doc);
    }

    public void testNormalization_whenMultipleSubqueriesMultipleHitsMultipleShardsAndDefaultMethod_thenScoreNormalized() {
        ScoreNormalizer scoreNormalizationMethod = new ScoreNormalizer();
        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        final List<CompoundTopDocs> queryTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(1, 10.0f), new ScoreDoc(2, 2.5f), new ScoreDoc(4, 0.1f) }
                    ),
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(3, 0.8f), new ScoreDoc(5, 0.5f) }
                    )
                ),
                false,
                SEARCH_SHARD
            ),
            new CompoundTopDocs(
                new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(
                        new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, 2.2f), new ScoreDoc(4, 1.8f), new ScoreDoc(7, 0.9f), new ScoreDoc(9, 0.01f) }
                    )
                ),
                false,
                SEARCH_SHARD
            ),
            new CompoundTopDocs(
                new TotalHits(0, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0])
                ),
                false,
                SEARCH_SHARD
            )
        );
        NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
            .queryTopDocs(queryTopDocs)
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .build();

        SearchRequest searchRequest = mock(SearchRequest.class);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        when(searchPhaseContext.getRequest().source()).thenReturn(searchSourceBuilder);
        scoreNormalizationMethod.normalizeScores(normalizeScoresDTO);
        assertNotNull(queryTopDocs);
        assertEquals(3, queryTopDocs.size());
        // shard one
        CompoundTopDocs resultDocShardOne = queryTopDocs.get(0);
        assertEquals(2, resultDocShardOne.getTopDocs().size());
        // sub-query one
        TopDocs topDocSubqueryOne = resultDocShardOne.getTopDocs().get(0);
        assertEquals(3, topDocSubqueryOne.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, topDocSubqueryOne.totalHits.relation());
        assertNotNull(topDocSubqueryOne.scoreDocs);
        assertEquals(3, topDocSubqueryOne.scoreDocs.length);
        ScoreDoc highScoreDoc = topDocSubqueryOne.scoreDocs[0];
        assertEquals(1.0, highScoreDoc.score, 0.001f);
        assertEquals(1, highScoreDoc.doc);
        ScoreDoc lowScoreDoc = topDocSubqueryOne.scoreDocs[topDocSubqueryOne.scoreDocs.length - 1];
        assertEquals(0.0, lowScoreDoc.score, 0.001f);
        assertEquals(4, lowScoreDoc.doc);
        // sub-query two
        TopDocs topDocSubqueryTwo = resultDocShardOne.getTopDocs().get(1);
        assertEquals(2, topDocSubqueryTwo.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, topDocSubqueryTwo.totalHits.relation());
        assertNotNull(topDocSubqueryTwo.scoreDocs);
        assertEquals(2, topDocSubqueryTwo.scoreDocs.length);
        assertTrue(Range.between(0.0f, 1.0f).contains(topDocSubqueryTwo.scoreDocs[0].score));
        assertEquals(3, topDocSubqueryTwo.scoreDocs[0].doc);
        assertTrue(Range.between(0.0f, 1.0f).contains(topDocSubqueryTwo.scoreDocs[topDocSubqueryTwo.scoreDocs.length - 1].score));
        assertEquals(5, topDocSubqueryTwo.scoreDocs[topDocSubqueryTwo.scoreDocs.length - 1].doc);

        // shard two
        CompoundTopDocs resultDocShardTwo = queryTopDocs.get(1);
        assertEquals(2, resultDocShardTwo.getTopDocs().size());
        // sub-query one
        TopDocs topDocShardTwoSubqueryOne = resultDocShardTwo.getTopDocs().get(0);
        assertEquals(0, topDocShardTwoSubqueryOne.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, topDocShardTwoSubqueryOne.totalHits.relation());
        assertNotNull(topDocShardTwoSubqueryOne.scoreDocs);
        assertEquals(0, topDocShardTwoSubqueryOne.scoreDocs.length);
        // sub-query two
        TopDocs topDocShardTwoSubqueryTwo = resultDocShardTwo.getTopDocs().get(1);
        assertEquals(4, topDocShardTwoSubqueryTwo.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, topDocShardTwoSubqueryTwo.totalHits.relation());
        assertNotNull(topDocShardTwoSubqueryTwo.scoreDocs);
        assertEquals(4, topDocShardTwoSubqueryTwo.scoreDocs.length);
        assertEquals(1.0, topDocShardTwoSubqueryTwo.scoreDocs[0].score, 0.001f);
        assertEquals(2, topDocShardTwoSubqueryTwo.scoreDocs[0].doc);
        assertEquals(0.0, topDocShardTwoSubqueryTwo.scoreDocs[topDocShardTwoSubqueryTwo.scoreDocs.length - 1].score, 0.001f);
        assertEquals(9, topDocShardTwoSubqueryTwo.scoreDocs[topDocShardTwoSubqueryTwo.scoreDocs.length - 1].doc);

        // shard three
        CompoundTopDocs resultDocShardThree = queryTopDocs.get(2);
        assertEquals(2, resultDocShardThree.getTopDocs().size());
        // sub-query one
        TopDocs topDocShardThreeSubqueryOne = resultDocShardThree.getTopDocs().get(0);
        assertEquals(0, topDocShardThreeSubqueryOne.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, topDocShardThreeSubqueryOne.totalHits.relation());
        assertEquals(0, topDocShardThreeSubqueryOne.scoreDocs.length);
        // sub-query two
        TopDocs topDocShardThreeSubqueryTwo = resultDocShardThree.getTopDocs().get(1);
        assertEquals(0, topDocShardThreeSubqueryTwo.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, topDocShardThreeSubqueryTwo.totalHits.relation());
        assertEquals(0, topDocShardThreeSubqueryTwo.scoreDocs.length);
    }
}
