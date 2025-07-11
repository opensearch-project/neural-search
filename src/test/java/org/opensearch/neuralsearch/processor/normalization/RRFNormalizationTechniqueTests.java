/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.processor.NormalizeScoresDTO;
import org.opensearch.neuralsearch.processor.SearchShard;
import org.opensearch.neuralsearch.processor.explain.DocIdAtSearchShard;
import org.opensearch.neuralsearch.processor.explain.ExplanationDetails;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.search.SearchShardTarget;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Abstracts testing of normalization of scores based on RRF method
 */
public class RRFNormalizationTechniqueTests extends OpenSearchQueryTestCase {
    static final int RANK_CONSTANT = 60;
    private ScoreNormalizationUtil scoreNormalizationUtil = new ScoreNormalizationUtil();
    private static final SearchShard SEARCH_SHARD = new SearchShard("my_index", 0, "12345678");

    public void testDescribe() {
        // verify with default values for parameters
        RRFNormalizationTechnique normalizationTechnique = new RRFNormalizationTechnique(Map.of(), scoreNormalizationUtil);
        assertEquals("rrf, rank_constant [60]", normalizationTechnique.describe());

        // verify when parameter values are set
        normalizationTechnique = new RRFNormalizationTechnique(Map.of("rank_constant", 25), scoreNormalizationUtil);
        assertEquals("rrf, rank_constant [25]", normalizationTechnique.describe());
    }

    public void testNormalization_whenResultFromOneShardOneSubQuery_thenSuccessful() {
        RRFNormalizationTechnique normalizationTechnique = new RRFNormalizationTechnique(Map.of(), scoreNormalizationUtil);
        float[] scores = { 0.5f, 0.2f };
        List<CompoundTopDocs> compoundTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, scores[0]), new ScoreDoc(4, scores[1]) }
                    )
                ),
                false,
                SEARCH_SHARD
            )
        );
        NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
            .queryTopDocs(compoundTopDocs)
            .normalizationTechnique(normalizationTechnique)
            .build();
        normalizationTechnique.normalize(normalizeScoresDTO);

        CompoundTopDocs expectedCompoundDocs = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(2, rrfNorm(0)), new ScoreDoc(4, rrfNorm(1)) }
                )
            ),
            false,
            SEARCH_SHARD
        );
        assertNotNull(compoundTopDocs);
        assertEquals(1, compoundTopDocs.size());
        assertNotNull(compoundTopDocs.get(0).getTopDocs());
        assertCompoundTopDocs(
            new TopDocs(expectedCompoundDocs.getTotalHits(), expectedCompoundDocs.getScoreDocs().toArray(new ScoreDoc[0])),
            compoundTopDocs.get(0).getTopDocs().get(0)
        );
    }

    public void testNormalization_whenResultFromOneShardMultipleSubQueries_thenSuccessful() {
        RRFNormalizationTechnique normalizationTechnique = new RRFNormalizationTechnique(Map.of(), scoreNormalizationUtil);
        float[] scoresQuery1 = { 0.5f, 0.2f };
        float[] scoresQuery2 = { 0.9f, 0.7f, 0.1f };
        List<CompoundTopDocs> compoundTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, scoresQuery1[0]), new ScoreDoc(4, scoresQuery1[1]) }
                    ),
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] {
                            new ScoreDoc(3, scoresQuery2[0]),
                            new ScoreDoc(4, scoresQuery2[1]),
                            new ScoreDoc(2, scoresQuery2[2]) }
                    )
                ),
                false,
                SEARCH_SHARD
            )
        );
        NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
            .queryTopDocs(compoundTopDocs)
            .normalizationTechnique(normalizationTechnique)
            .build();
        normalizationTechnique.normalize(normalizeScoresDTO);

        CompoundTopDocs expectedCompoundDocs = new CompoundTopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(2, rrfNorm(0)), new ScoreDoc(4, rrfNorm(1)) }
                ),
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(3, rrfNorm(0)), new ScoreDoc(4, rrfNorm(1)), new ScoreDoc(2, rrfNorm(2)) }
                )
            ),
            false,
            SEARCH_SHARD
        );
        assertNotNull(compoundTopDocs);
        assertEquals(1, compoundTopDocs.size());
        assertNotNull(compoundTopDocs.get(0).getTopDocs());
        for (int i = 0; i < expectedCompoundDocs.getTopDocs().size(); i++) {
            assertCompoundTopDocs(expectedCompoundDocs.getTopDocs().get(i), compoundTopDocs.get(0).getTopDocs().get(i));
        }
    }

    public void testNormalization_whenResultFromMultipleShardsMultipleSubQueries_thenSuccessful() {
        RRFNormalizationTechnique normalizationTechnique = new RRFNormalizationTechnique(Map.of(), scoreNormalizationUtil);
        float[] scoresShard1Query1 = { 0.5f, 0.2f };
        float[] scoresShard1and2Query3 = { 0.9f, 0.7f, 0.1f, 0.8f, 0.7f, 0.6f, 0.5f };
        float[] scoresShard2Query2 = { 2.9f, 0.7f };
        List<CompoundTopDocs> compoundTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, scoresShard1Query1[0]), new ScoreDoc(4, scoresShard1Query1[1]) }
                    ),
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] {
                            new ScoreDoc(3, scoresShard1and2Query3[0]),
                            new ScoreDoc(4, scoresShard1and2Query3[1]),
                            new ScoreDoc(2, scoresShard1and2Query3[2]) }
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
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(7, scoresShard2Query2[0]), new ScoreDoc(9, scoresShard2Query2[1]) }
                    ),
                    new TopDocs(
                        new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] {
                            new ScoreDoc(3, scoresShard1and2Query3[3]),
                            new ScoreDoc(9, scoresShard1and2Query3[4]),
                            new ScoreDoc(10, scoresShard1and2Query3[5]),
                            new ScoreDoc(15, scoresShard1and2Query3[6]) }
                    )
                ),
                false,
                SEARCH_SHARD
            )
        );
        NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
            .queryTopDocs(compoundTopDocs)
            .normalizationTechnique(normalizationTechnique)
            .build();
        normalizationTechnique.normalize(normalizeScoresDTO);

        CompoundTopDocs expectedCompoundDocsShard1 = new CompoundTopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(2, rrfNorm(0)), new ScoreDoc(4, rrfNorm(1)) }
                ),
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(3, rrfNorm(0)), new ScoreDoc(4, rrfNorm(1)), new ScoreDoc(2, rrfNorm(2)) }
                )
            ),
            false,
            SEARCH_SHARD
        );

        CompoundTopDocs expectedCompoundDocsShard2 = new CompoundTopDocs(
            new TotalHits(4, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(7, rrfNorm(0)), new ScoreDoc(9, rrfNorm(1)) }
                ),
                new TopDocs(
                    new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        new ScoreDoc(3, rrfNorm(3)),
                        new ScoreDoc(9, rrfNorm(4)),
                        new ScoreDoc(10, rrfNorm(5)),
                        new ScoreDoc(15, rrfNorm(6)) }
                )
            ),
            false,
            SEARCH_SHARD
        );

        assertNotNull(compoundTopDocs);
        assertEquals(2, compoundTopDocs.size());
        assertNotNull(compoundTopDocs.get(0).getTopDocs());
        for (int i = 0; i < expectedCompoundDocsShard1.getTopDocs().size(); i++) {
            assertCompoundTopDocs(expectedCompoundDocsShard1.getTopDocs().get(i), compoundTopDocs.get(0).getTopDocs().get(i));
        }
        assertNotNull(compoundTopDocs.get(1).getTopDocs());
        for (int i = 0; i < expectedCompoundDocsShard2.getTopDocs().size(); i++) {
            assertCompoundTopDocs(expectedCompoundDocsShard2.getTopDocs().get(i), compoundTopDocs.get(1).getTopDocs().get(i));
        }
    }

    public void testNormalizedScoresAreSetAtCorrectIndices() {
        // Setup test data
        SearchShardTarget shardTarget = new SearchShardTarget("node1", new ShardId("index", "_na_", 0), null, null);
        SearchShard searchShard = SearchShard.createSearchShard(shardTarget);

        // Create TopDocs with different scores and ranks for different subqueries
        TopDocs topDocs1 = new TopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
                new ScoreDoc(1, 0.8f),  // Rank 1: RRF = 1/(60 + 1) = 0.0164
                new ScoreDoc(2, 0.6f)   // Rank 2: RRF = 1/(60 + 2) = 0.0161
            }
        );

        TopDocs topDocs2 = new TopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
                new ScoreDoc(2, 0.9f),  // Rank 1: RRF = 1/(60 + 1) = 0.0164
                new ScoreDoc(1, 0.7f)   // Rank 2: RRF = 1/(60 + 2) = 0.0161
            }
        );

        TopDocs topDocs3 = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 0.5f)   // Rank 1:
                                                                                                                               // RRF =
                                                                                                                               // 1/(60 + 1)
                                                                                                                               // = 0.0164
        });

        // Create CompoundTopDocs with multiple subqueries
        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(5, TotalHits.Relation.EQUAL_TO),
            Arrays.asList(topDocs1, topDocs2, topDocs3),
            false,
            searchShard
        );

        RRFNormalizationTechnique normalizer = new RRFNormalizationTechnique(Map.of(), new ScoreNormalizationUtil());
        Map<DocIdAtSearchShard, ExplanationDetails> result = normalizer.explain(Collections.singletonList(compoundTopDocs));

        // Verify results
        DocIdAtSearchShard doc1 = new DocIdAtSearchShard(1, searchShard);
        DocIdAtSearchShard doc2 = new DocIdAtSearchShard(2, searchShard);

        // Constants for RRF score calculation
        float rank1Score = 1.0f / (60.0f + 1.0f); // ≈ 0.0164
        float rank2Score = 1.0f / (60.0f + 2.0f); // ≈ 0.0161
        float zeroScore = 0.0f;

        // Verify document 1 normalized scores
        ExplanationDetails doc1Details = result.get(doc1);
        assertNotNull(doc1Details);
        List<Pair<Float, String>> doc1Scores = doc1Details.getScoreDetails();
        assertEquals(3, doc1Scores.size());

        // Verify RRF scores for document 1
        assertEquals(rank1Score, doc1Scores.get(0).getKey(), DELTA_FOR_ASSERTION); // First subquery (rank 1)
        assertEquals(rank2Score, doc1Scores.get(1).getKey(), DELTA_FOR_ASSERTION); // Second subquery (rank 2)
        assertEquals(rank1Score, doc1Scores.get(2).getKey(), DELTA_FOR_ASSERTION); // Third subquery (rank 1)

        // Verify document 2 normalized scores
        ExplanationDetails doc2Details = result.get(doc2);
        assertNotNull(doc2Details);
        List<Pair<Float, String>> doc2Scores = doc2Details.getScoreDetails();
        assertEquals(3, doc2Scores.size());

        // Verify RRF scores for document 2
        assertEquals(rank2Score, doc2Scores.get(0).getKey(), DELTA_FOR_ASSERTION); // First subquery (rank 2)
        assertEquals(rank1Score, doc2Scores.get(1).getKey(), DELTA_FOR_ASSERTION); // Second subquery (rank 1)
        assertEquals(zeroScore, doc2Scores.get(2).getKey(), DELTA_FOR_ASSERTION);  // Third subquery (not present)

        // Verify that original ScoreDoc scores were updated with RRF scores
        assertEquals(rank1Score, topDocs1.scoreDocs[0].score, DELTA_FOR_ASSERTION); // doc1 in first subquery
        assertEquals(rank2Score, topDocs1.scoreDocs[1].score, DELTA_FOR_ASSERTION); // doc2 in first subquery
        assertEquals(rank1Score, topDocs2.scoreDocs[0].score, DELTA_FOR_ASSERTION); // doc2 in second subquery
        assertEquals(rank2Score, topDocs2.scoreDocs[1].score, DELTA_FOR_ASSERTION); // doc1 in second subquery
        assertEquals(rank1Score, topDocs3.scoreDocs[0].score, DELTA_FOR_ASSERTION); // doc1 in third subquery

        // Verify explanation descriptions
        assertTrue(doc1Scores.get(0).getValue().contains("rrf"));
        assertTrue(doc1Scores.get(1).getValue().contains("rrf"));
        assertTrue(doc1Scores.get(2).getValue().contains("rrf"));
    }

    public void testExplainWithEmptyAndNullList() {
        RRFNormalizationTechnique normalizationTechnique = new RRFNormalizationTechnique(Map.of(), scoreNormalizationUtil);
        normalizationTechnique.explain(List.of());

        List<CompoundTopDocs> compoundTopDocs = new ArrayList<>();
        compoundTopDocs.add(null);
        normalizationTechnique.explain(compoundTopDocs);
    }

    public void testExplainWithSingleTopDocs() {
        RRFNormalizationTechnique normalizationTechnique = new RRFNormalizationTechnique(Map.of(), scoreNormalizationUtil);
        CompoundTopDocs topDocs = createCompoundTopDocs(new float[] { 0.8f }, 1);
        List<CompoundTopDocs> queryTopDocs = Collections.singletonList(topDocs);

        Map<DocIdAtSearchShard, ExplanationDetails> explanation = normalizationTechnique.explain(queryTopDocs);

        assertNotNull(explanation);
        assertEquals(1, explanation.size());
        assertTrue(explanation.containsKey(new DocIdAtSearchShard(0, new SearchShard("test_index", 0, "uuid"))));
    }

    public void testSubQueryScores_whenSubQueryScoreIsEnabled_thenSuccessful() {
        RRFNormalizationTechnique normalizationTechnique = new RRFNormalizationTechnique(Map.of(), scoreNormalizationUtil);
        float[] scores = { 0.5f, 0.2f };
        List<CompoundTopDocs> compoundTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, scores[0]), new ScoreDoc(4, scores[1]) }
                    )
                ),
                false,
                SEARCH_SHARD
            )
        );
        NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
            .queryTopDocs(compoundTopDocs)
            .normalizationTechnique(normalizationTechnique)
            .subQueryScores(true)
            .build();
        Map<String, float[]> docIdToSubqueryScores = normalizationTechnique.normalize(normalizeScoresDTO);

        CompoundTopDocs expectedCompoundDocs = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(2, rrfNorm(0)), new ScoreDoc(4, rrfNorm(1)) }
                )
            ),
            false,
            SEARCH_SHARD
        );

        Map<String, float[]> expectedDocIdToSubqueryScores = Map.ofEntries(
            Map.entry(SEARCH_SHARD.getShardId() + "_" + "2", new float[] { 0.5f }),
            Map.entry(SEARCH_SHARD.getShardId() + "_" + "4", new float[] { 0.2f })
        );
        assertEquals(expectedDocIdToSubqueryScores.size(), docIdToSubqueryScores.size());
        for (Map.Entry<String, float[]> entry : expectedDocIdToSubqueryScores.entrySet()) {
            String key = entry.getKey();
            float[] expectedScores = entry.getValue();
            float[] actualScores = docIdToSubqueryScores.get(key);

            assertArrayEquals(expectedScores, actualScores, 0.0001f);
        }

        assertNotNull(compoundTopDocs);
        assertEquals(1, compoundTopDocs.size());
        assertNotNull(compoundTopDocs.get(0).getTopDocs());
        assertCompoundTopDocs(
            new TopDocs(expectedCompoundDocs.getTotalHits(), expectedCompoundDocs.getScoreDocs().toArray(new ScoreDoc[0])),
            compoundTopDocs.get(0).getTopDocs().get(0)
        );
    }

    public void testSubQueryScores_whenSubQueryScoreIsDisabled_thenSuccessful() {
        L2ScoreNormalizationTechnique normalizationTechnique = new L2ScoreNormalizationTechnique();
        List<CompoundTopDocs> compoundTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, 0.5f), new ScoreDoc(4, 0.2f) }
                    )
                ),
                false,
                SEARCH_SHARD
            )
        );
        NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
            .queryTopDocs(compoundTopDocs)
            .normalizationTechnique(normalizationTechnique)
            .subQueryScores(false)
            .build();
        Map<String, float[]> docIdToSubqueryScores = normalizationTechnique.normalize(normalizeScoresDTO);

        assertTrue(docIdToSubqueryScores.isEmpty());
    }

    private float rrfNorm(int rank) {
        // 1.0f / (float) (rank + RANK_CONSTANT + 1);
        return BigDecimal.ONE.divide(BigDecimal.valueOf(rank + RANK_CONSTANT + 1), 10, RoundingMode.HALF_UP).floatValue();
    }

    private void assertCompoundTopDocs(TopDocs expected, TopDocs actual) {
        assertEquals(expected.totalHits.value(), actual.totalHits.value());
        assertEquals(expected.totalHits.relation(), actual.totalHits.relation());
        assertEquals(expected.scoreDocs.length, actual.scoreDocs.length);
        for (int i = 0; i < expected.scoreDocs.length; i++) {
            assertEquals(expected.scoreDocs[i].score, actual.scoreDocs[i].score, DELTA_FOR_ASSERTION);
            assertEquals(expected.scoreDocs[i].doc, actual.scoreDocs[i].doc);
            assertEquals(expected.scoreDocs[i].shardIndex, actual.scoreDocs[i].shardIndex);
        }
    }

    private CompoundTopDocs createCompoundTopDocs(float[] scores, int size) {
        ScoreDoc[] scoreDocs = new ScoreDoc[size];
        for (int i = 0; i < size; i++) {
            scoreDocs[i] = new ScoreDoc(i, scores[i]);
        }
        TopDocs singleTopDocs = new TopDocs(new TotalHits(size, TotalHits.Relation.EQUAL_TO), scoreDocs);

        List<TopDocs> topDocsList = Collections.singletonList(singleTopDocs);
        TopDocs topDocs = new TopDocs(new TotalHits(size, TotalHits.Relation.EQUAL_TO), scoreDocs);
        SearchShard searchShard = new SearchShard("test_index", 0, "uuid");

        return new CompoundTopDocs(
            new TotalHits(size, TotalHits.Relation.EQUAL_TO),
            topDocsList,
            false, // isSortEnabled
            searchShard
        );
    }
}
