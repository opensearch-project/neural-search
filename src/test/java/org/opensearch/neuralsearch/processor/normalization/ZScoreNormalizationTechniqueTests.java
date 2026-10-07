/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.processor.dto.ExplainDTO;
import org.opensearch.neuralsearch.processor.dto.NormalizeScoresDTO;
import org.opensearch.neuralsearch.processor.SearchShard;
import org.opensearch.neuralsearch.processor.explain.DocIdAtSearchShard;
import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;
import org.opensearch.neuralsearch.processor.explain.ExplanationDetails;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.search.SearchShardTarget;

import com.google.common.primitives.Floats;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.ToDoubleFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * Abstracts normalization of scores based on z_score method
 */
public class ZScoreNormalizationTechniqueTests extends OpenSearchQueryTestCase {
    private static final float DELTA_FOR_ASSERTION = 0.0001f;
    private static final SearchShard SEARCH_SHARD = new SearchShard("my_index", 0, "12345678");

    public void testNormalization_whenResultFromOneShardOneSubQuery_thenSuccessful() {
        ZScoreNormalizationTechnique normalizationTechnique = new ZScoreNormalizationTechnique();
        Float[] scores = { 0.5f, 0.2f };
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
                    new ScoreDoc[] {
                        new ScoreDoc(2, zscoreNorm(scores[0], Arrays.asList(scores))),
                        new ScoreDoc(4, zscoreNorm(scores[1], Arrays.asList(scores))) }
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
        ZScoreNormalizationTechnique normalizationTechnique = new ZScoreNormalizationTechnique();
        Float[] scoresQuery1 = { 0.5f, 0.2f };
        Float[] scoresQuery2 = { 0.9f, 0.7f, 0.1f };
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
                    new ScoreDoc[] {
                        new ScoreDoc(2, zscoreNorm(scoresQuery1[0], Arrays.asList(scoresQuery1))),
                        new ScoreDoc(4, zscoreNorm(scoresQuery1[1], Arrays.asList(scoresQuery1))) }
                ),
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        new ScoreDoc(3, zscoreNorm(scoresQuery2[0], Arrays.asList(scoresQuery2))),
                        new ScoreDoc(4, zscoreNorm(scoresQuery2[1], Arrays.asList(scoresQuery2))),
                        new ScoreDoc(2, zscoreNorm(scoresQuery2[2], Arrays.asList(scoresQuery2))) }
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
        ZScoreNormalizationTechnique normalizationTechnique = new ZScoreNormalizationTechnique();
        Float[] scoresShard1Query1 = { 0.5f, 0.2f };
        Float[] scoresShard1and2Query3 = { 0.9f, 0.7f, 0.1f, 0.8f, 0.7f, 0.6f, 0.5f };
        Float[] scoresShard2Query2 = { 2.9f, 0.7f };
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
                    new ScoreDoc[] {
                        new ScoreDoc(2, zscoreNorm(scoresShard1Query1[0], Arrays.asList(scoresShard1Query1))),
                        new ScoreDoc(4, zscoreNorm(scoresShard1Query1[1], Arrays.asList(scoresShard1Query1))) }
                ),
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        new ScoreDoc(3, zscoreNorm(scoresShard1and2Query3[0], Arrays.asList(scoresShard1and2Query3))),
                        new ScoreDoc(4, zscoreNorm(scoresShard1and2Query3[1], Arrays.asList(scoresShard1and2Query3))),
                        new ScoreDoc(2, zscoreNorm(scoresShard1and2Query3[2], Arrays.asList(scoresShard1and2Query3))) }
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
                    new ScoreDoc[] {
                        new ScoreDoc(7, zscoreNorm(scoresShard2Query2[0], Arrays.asList(scoresShard2Query2))),
                        new ScoreDoc(9, zscoreNorm(scoresShard2Query2[1], Arrays.asList(scoresShard2Query2))) }
                ),
                new TopDocs(
                    new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        new ScoreDoc(3, zscoreNorm(scoresShard1and2Query3[3], Arrays.asList(scoresShard1and2Query3))),
                        new ScoreDoc(9, zscoreNorm(scoresShard1and2Query3[4], Arrays.asList(scoresShard1and2Query3))),
                        new ScoreDoc(10, zscoreNorm(scoresShard1and2Query3[5], Arrays.asList(scoresShard1and2Query3))),
                        new ScoreDoc(15, zscoreNorm(scoresShard1and2Query3[6], Arrays.asList(scoresShard1and2Query3))) }
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

        // Create TopDocs with different scores for different subqueries
        TopDocs topDocs1 = new TopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
                // Z-score calculation:
                // Given scores [2.0, 7.0, 8.0]
                // mean = (2.0 + 7.0 + 8.0)/3 = 5.667
                // std dev = sqrt(((2.0 - 5.667)² + (7.0 - 5.667)² + (8.0 - 5.667)²)/3) = 2.625
                new ScoreDoc(1, 2.0f),  // Z-score = (2.0 - 5.667)/2.625 = -1.397 --> 0.001f
                new ScoreDoc(2, 7.0f),  // Z-score = (7.0 - 5.667)/2.625 = 0.41478074
                new ScoreDoc(3, 8.0f)   // Z-score = (8.0 - 5.667)/2.625 = 0.72586626
            }
        );

        TopDocs topDocs2 = new TopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
                // Z-score calculation:
                // Given scores [4.0, 5.0, 10.0]
                // mean = (4.0 + 5.0 + 10.0)/3 = 6.333
                // std dev = sqrt(((4.0 - 6.333)² + (5.0 - 6.333)² + (10.0 - 6.333)²)/3) = 2.625
                new ScoreDoc(2, 4.0f),  // Z-score = (4.0 - 6.333)/2.625 = -0.889 --> 0.001f
                new ScoreDoc(1, 5.0f),   //// Z-score = (5.0 - 6.333)/2.625 = -0.508 --> 0.001f
                new ScoreDoc(3, 10.0f) // Z-score = (10.0 - 6.333)/2.625 = 1.1406468
            }
        );

        TopDocs topDocs3 = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 1.0f)   // As
            // this is the only score Z-score = 0
            // (as std dev would be 0, making it undefined and for such case we have an edge case making the score as 0.0f
        });

        // Create CompoundTopDocs with multiple subqueries
        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(7, TotalHits.Relation.EQUAL_TO),
            Arrays.asList(topDocs1, topDocs2, topDocs3),
            false,
            searchShard
        );

        ZScoreNormalizationTechnique normalizer = new ZScoreNormalizationTechnique();
        Map<DocIdAtSearchShard, ExplanationDetails> result = normalizer.explain(
            ExplainDTO.builder()
                .queryTopDocs(Collections.singletonList(compoundTopDocs))
                .explainableTechnique(mock(ExplainableTechnique.class))
                .build()
        );

        // Verify results
        DocIdAtSearchShard doc1 = new DocIdAtSearchShard(1, searchShard);
        DocIdAtSearchShard doc2 = new DocIdAtSearchShard(2, searchShard);
        DocIdAtSearchShard doc3 = new DocIdAtSearchShard(3, searchShard);

        // Verify document 1 normalized scores
        ExplanationDetails doc1Details = result.get(doc1);
        assertNotNull(doc1Details);
        List<Pair<Float, String>> doc1Scores = doc1Details.getScoreDetails();
        assertEquals(3, doc1Scores.size());

        // Verify zscore normalized scores for document 1
        assertEquals(0.001f, doc1Scores.get(0).getKey(), DELTA_FOR_ASSERTION); // First subquery
        assertEquals(0.001f, doc1Scores.get(1).getKey(), DELTA_FOR_ASSERTION); // Second subquery
        assertEquals(1.0000f, doc1Scores.get(2).getKey(), DELTA_FOR_ASSERTION); // Third subquery

        // Verify document 2 normalized scores
        ExplanationDetails doc2Details = result.get(doc2);
        assertNotNull(doc2Details);
        List<Pair<Float, String>> doc2Scores = doc2Details.getScoreDetails();
        assertEquals(3, doc2Scores.size());

        // Verify zscore normalized scores for document 2
        assertEquals(0.41478074f, doc2Scores.get(0).getKey(), DELTA_FOR_ASSERTION); // First subquery
        assertEquals(0.001f, doc2Scores.get(1).getKey(), DELTA_FOR_ASSERTION); // Second subquery
        assertEquals(0.0000f, doc2Scores.get(2).getKey(), DELTA_FOR_ASSERTION); // Third subquery (doc2 not present)

        // Verify document 2 normalized scores
        ExplanationDetails doc3Details = result.get(doc3);
        assertNotNull(doc3Details);
        List<Pair<Float, String>> doc3Scores = doc3Details.getScoreDetails();
        assertEquals(3, doc3Scores.size());

        // Verify zscore normalized scores for document 2
        assertEquals(0.72586626f, doc3Scores.get(0).getKey(), DELTA_FOR_ASSERTION); // First subquery
        assertEquals(1.1406468f, doc3Scores.get(1).getKey(), DELTA_FOR_ASSERTION); // Second subquery
        assertEquals(0.0000f, doc3Scores.get(2).getKey(), DELTA_FOR_ASSERTION); // Third subquery (doc2 not present)

        // Verify that original ScoreDoc scores were updated with z score normalized values
        assertEquals(0.001f, topDocs1.scoreDocs[0].score, DELTA_FOR_ASSERTION); // doc1 in first subquery
        assertEquals(0.41478074f, topDocs1.scoreDocs[1].score, DELTA_FOR_ASSERTION); // doc2 in first subquery
        assertEquals(0.72586626f, topDocs1.scoreDocs[2].score, DELTA_FOR_ASSERTION); // doc3 in first subquery
        assertEquals(0.001f, topDocs2.scoreDocs[0].score, DELTA_FOR_ASSERTION); // doc2 in second subquery
        assertEquals(0.001f, topDocs2.scoreDocs[1].score, DELTA_FOR_ASSERTION); // doc1 in second subquery
        assertEquals(1.1406468f, topDocs2.scoreDocs[2].score, DELTA_FOR_ASSERTION); // doc3 in second subquery
        assertEquals(1.0000f, topDocs3.scoreDocs[0].score, DELTA_FOR_ASSERTION); // doc1 in third subquery

        // Verify explanation descriptions
        assertTrue(doc1Scores.get(0).getValue().contains("z_score normalization"));
        assertTrue(doc1Scores.get(1).getValue().contains("z_score normalization"));
        assertTrue(doc1Scores.get(2).getValue().contains("z_score normalization"));
    }

    /**
     * The four statistics z_score needs (mean, standard deviation, max, min) are now read off one DescriptiveStatistics
     * pass per sub query; each used to be computed by its own independent walk of every hit. This asserts the collapse is
     * exact rather than merely close — normalized scores must be bit-identical to the four-pass computation.
     *
     * <p>Mean, standard deviation and max are pinned by the assertions below. Min is not, and cannot be: it is only read on
     * the {@code standardDeviation == 0} branch, and a zero standard deviation means every score equals the mean, so the
     * preceding {@code mean == score} check returns max first. That branch is unreachable for any realistic score.
     */
    public void testNormalization_whenStatsComputedInOnePass_thenBitIdenticalToFourPassReference() {
        int numOfSubqueries = randomIntBetween(2, 4);
        int numOfShards = randomIntBetween(1, 3);

        // Raw scores are held aside because normalize() overwrites the ScoreDoc scores in place.
        List<List<float[]>> rawScores = new ArrayList<>();
        for (int shard = 0; shard < numOfShards; shard++) {
            List<float[]> perSubQuery = new ArrayList<>();
            for (int subQuery = 0; subQuery < numOfSubqueries; subQuery++) {
                float[] scores;
                if (subQuery == 1) {
                    // A fixed triple whose mean (2.0) is exactly one of its scores, on one shard only. That score takes the
                    // `mean == score` branch, which returns the sub query max — and because max (3.0), mean (2.0) and min
                    // (1.0) all differ here, the assertion below can tell those three statistics apart.
                    scores = shard == 0 ? new float[] { 3.0f, 2.0f, 1.0f } : new float[0];
                } else {
                    // Sub query 0 is populated on every shard so mean and standard deviation drive the main branch; the
                    // rest are free to be empty, exercising the NaN-statistics case.
                    int numOfHits = subQuery == 0 ? randomIntBetween(2, 8) : randomIntBetween(0, 8);
                    scores = new float[numOfHits];
                    for (int hit = 0; hit < numOfHits; hit++) {
                        scores[hit] = randomFloat() * 100.0f;
                    }
                    // hits reach normalization in descending score order, as they do from a real shard
                    Arrays.sort(scores);
                    reverse(scores);
                }
                perSubQuery.add(scores);
            }
            rawScores.add(perSubQuery);
        }

        List<CompoundTopDocs> queryTopDocs = new ArrayList<>();
        for (int shard = 0; shard < numOfShards; shard++) {
            List<TopDocs> topDocsPerSubQuery = new ArrayList<>();
            long hitsOnShard = 0;
            for (float[] scores : rawScores.get(shard)) {
                ScoreDoc[] scoreDocs = new ScoreDoc[scores.length];
                for (int hit = 0; hit < scores.length; hit++) {
                    scoreDocs[hit] = new ScoreDoc(hit, scores[hit]);
                }
                topDocsPerSubQuery.add(new TopDocs(new TotalHits(scores.length, TotalHits.Relation.EQUAL_TO), scoreDocs));
                hitsOnShard += scores.length;
            }
            queryTopDocs.add(
                new CompoundTopDocs(
                    new TotalHits(hitsOnShard, TotalHits.Relation.EQUAL_TO),
                    topDocsPerSubQuery,
                    false,
                    new SearchShard("my_index", shard, "shard-" + shard)
                )
            );
        }

        // Four fully independent walks, one per statistic — the computation the single pass replaced.
        float[] referenceMax = referenceStatisticPerSubquery(rawScores, numOfSubqueries, DescriptiveStatistics::getMax);
        float[] referenceMin = referenceStatisticPerSubquery(rawScores, numOfSubqueries, DescriptiveStatistics::getMin);
        float[] referenceMean = referenceStatisticPerSubquery(rawScores, numOfSubqueries, DescriptiveStatistics::getMean);
        float[] referenceStd = referenceStatisticPerSubquery(rawScores, numOfSubqueries, DescriptiveStatistics::getStandardDeviation);

        ZScoreNormalizationTechnique normalizationTechnique = new ZScoreNormalizationTechnique();
        normalizationTechnique.normalize(
            NormalizeScoresDTO.builder().queryTopDocs(queryTopDocs).normalizationTechnique(normalizationTechnique).build()
        );

        int comparisons = 0;
        for (int shard = 0; shard < numOfShards; shard++) {
            List<TopDocs> topDocsPerSubQuery = queryTopDocs.get(shard).getTopDocs();
            for (int subQuery = 0; subQuery < numOfSubqueries; subQuery++) {
                float[] scores = rawScores.get(shard).get(subQuery);
                ScoreDoc[] scoreDocs = topDocsPerSubQuery.get(subQuery).scoreDocs;
                for (int hit = 0; hit < scores.length; hit++) {
                    float expected = referenceNormalizeSingleScore(
                        scores[hit],
                        referenceStd[subQuery],
                        referenceMean[subQuery],
                        referenceMax[subQuery],
                        referenceMin[subQuery]
                    );
                    assertEquals(
                        "bit-level mismatch on shard " + shard + ", sub query " + subQuery + ", hit " + hit,
                        Float.floatToIntBits(expected),
                        Float.floatToIntBits(scoreDocs[hit].score)
                    );
                    comparisons++;
                }
            }
        }
        assertTrue("fixture produced no scores to compare", comparisons > 0);
    }

    /**
     * Rebuilds one statistic the way the pre-collapse code did: its own DescriptiveStatistics array, its own full walk of
     * every hit, in the same shard-then-sub-query order.
     */
    private static float[] referenceStatisticPerSubquery(
        final List<List<float[]>> rawScores,
        final int numOfSubqueries,
        final ToDoubleFunction<DescriptiveStatistics> statistic
    ) {
        DescriptiveStatistics[] statsPerSubquery = new DescriptiveStatistics[numOfSubqueries];
        for (int subQuery = 0; subQuery < numOfSubqueries; subQuery++) {
            statsPerSubquery[subQuery] = new DescriptiveStatistics();
        }
        for (List<float[]> perSubQuery : rawScores) {
            for (int subQuery = 0; subQuery < numOfSubqueries; subQuery++) {
                for (float score : perSubQuery.get(subQuery)) {
                    statsPerSubquery[subQuery].addValue(score);
                }
            }
        }
        float[] statisticPerSubquery = new float[numOfSubqueries];
        for (int subQuery = 0; subQuery < numOfSubqueries; subQuery++) {
            statisticPerSubquery[subQuery] = (float) statistic.applyAsDouble(statsPerSubquery[subQuery]);
        }
        return statisticPerSubquery;
    }

    /**
     * The z_score formula as ZScoreNormalizationTechnique applies it, restated here so the reference is independent of the
     * production code path under test.
     */
    private static float referenceNormalizeSingleScore(
        final float score,
        final float standardDeviation,
        final float mean,
        final float maxScore,
        final float minScore
    ) {
        if (Floats.compare(mean, score) == 0) {
            return maxScore;
        }
        if (Floats.compare(standardDeviation, 0.0f) == 0) {
            return minScore;
        }
        float normalizedScore = (score - mean) / standardDeviation;
        return normalizedScore <= 0.0f ? 0.001f : normalizedScore;
    }

    private static void reverse(final float[] values) {
        for (int i = 0, j = values.length - 1; i < j; i++, j--) {
            float swapped = values[i];
            values[i] = values[j];
            values[j] = swapped;
        }
    }

    private float zscoreNorm(float score, List<Float> scores) {
        DescriptiveStatistics stats = new DescriptiveStatistics();

        // Add all scores to DescriptiveStatistics
        for (Float s : scores) {
            stats.addValue(s);
        }

        // Calculate mean and standard deviation
        double mean = stats.getMean();
        double standardDeviation = stats.getStandardDeviation();

        // Handle case when standard deviation is 0
        if (Double.compare(standardDeviation, 0.0) == 0) {
            return 0.0f;
        }

        float normalizedScore = (float) ((score - mean) / standardDeviation);

        return normalizedScore <= 0.0f ? 0.001f : normalizedScore;
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
}
