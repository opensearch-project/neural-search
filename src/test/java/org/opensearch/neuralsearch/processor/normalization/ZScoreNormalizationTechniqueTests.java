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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
                new ScoreDoc(2, 7.0f),  // Z-score = (7.0 - 5.667)/2.625 = 0.508
                new ScoreDoc(3, 8.0f)   // Z-score = (8.0 - 5.667)/2.625 = 0.889
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
                new ScoreDoc(3, 10.0f) // Z-score = (10.0 - 6.333)/2.625 = 1.397
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
        Map<DocIdAtSearchShard, ExplanationDetails> result = normalizer.explain(Collections.singletonList(compoundTopDocs));

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
        assertEquals(0.508f, doc2Scores.get(0).getKey(), DELTA_FOR_ASSERTION); // First subquery
        assertEquals(0.001f, doc2Scores.get(1).getKey(), DELTA_FOR_ASSERTION); // Second subquery
        assertEquals(0.0000f, doc2Scores.get(2).getKey(), DELTA_FOR_ASSERTION); // Third subquery (doc2 not present)

        // Verify document 2 normalized scores
        ExplanationDetails doc3Details = result.get(doc3);
        assertNotNull(doc3Details);
        List<Pair<Float, String>> doc3Scores = doc3Details.getScoreDetails();
        assertEquals(3, doc3Scores.size());

        // Verify zscore normalized scores for document 2
        assertEquals(0.889f, doc3Scores.get(0).getKey(), DELTA_FOR_ASSERTION); // First subquery
        assertEquals(1.397f, doc3Scores.get(1).getKey(), DELTA_FOR_ASSERTION); // Second subquery
        assertEquals(0.0000f, doc3Scores.get(2).getKey(), DELTA_FOR_ASSERTION); // Third subquery (doc2 not present)

        // Verify that original ScoreDoc scores were updated with z score normalized values
        assertEquals(0.001f, topDocs1.scoreDocs[0].score, DELTA_FOR_ASSERTION); // doc1 in first subquery
        assertEquals(0.508f, topDocs1.scoreDocs[1].score, DELTA_FOR_ASSERTION); // doc2 in first subquery
        assertEquals(0.889f, topDocs1.scoreDocs[2].score, DELTA_FOR_ASSERTION); // doc3 in first subquery
        assertEquals(0.001f, topDocs2.scoreDocs[0].score, DELTA_FOR_ASSERTION); // doc2 in second subquery
        assertEquals(0.001f, topDocs2.scoreDocs[1].score, DELTA_FOR_ASSERTION); // doc1 in second subquery
        assertEquals(1.397f, topDocs2.scoreDocs[2].score, DELTA_FOR_ASSERTION); // doc3 in second subquery
        assertEquals(1.0000f, topDocs3.scoreDocs[0].score, DELTA_FOR_ASSERTION); // doc1 in third subquery

        // Verify explanation descriptions
        assertTrue(doc1Scores.get(0).getValue().contains("z_score normalization"));
        assertTrue(doc1Scores.get(1).getValue().contains("z_score normalization"));
        assertTrue(doc1Scores.get(2).getValue().contains("z_score normalization"));
    }

    private float zscoreNorm(float score, List<Float> scores) {
        // Calculate mean
        float mean = (float) scores.stream().mapToDouble(Float::doubleValue).average().orElse(0.0);

        // Calculate standard deviation
        float standardDeviation = (float) Math.sqrt(scores.stream().mapToDouble(s -> Math.pow(s - mean, 2)).average().orElse(0.0));

        // Handle case when standard deviation is 0
        if (Float.compare(standardDeviation, 0.0f) == 0) {
            return 0.0f;
        }

        float normalizedScore = (score - mean) / standardDeviation;

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
