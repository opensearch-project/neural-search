/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.processor.SearchShard;
import org.opensearch.neuralsearch.processor.explain.DocIdAtSearchShard;
import org.opensearch.neuralsearch.processor.explain.ExplanationDetails;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.processor.NormalizeScoresDTO;
import org.opensearch.search.SearchShardTarget;

/**
 * Abstracts normalization of scores based on L2 method
 */
public class L2ScoreNormalizationTechniqueTests extends OpenSearchQueryTestCase {
    private static final float DELTA_FOR_ASSERTION = 0.0001f;
    private static final SearchShard SEARCH_SHARD = new SearchShard("my_index", 0, "12345678");

    public void testNormalization_whenResultFromOneShardOneSubQuery_thenSuccessful() {
        L2ScoreNormalizationTechnique normalizationTechnique = new L2ScoreNormalizationTechnique();
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
                        new ScoreDoc(2, l2Norm(scores[0], Arrays.asList(scores))),
                        new ScoreDoc(4, l2Norm(scores[1], Arrays.asList(scores))) }
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
        L2ScoreNormalizationTechnique normalizationTechnique = new L2ScoreNormalizationTechnique();
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
                        new ScoreDoc(2, l2Norm(scoresQuery1[0], Arrays.asList(scoresQuery1))),
                        new ScoreDoc(4, l2Norm(scoresQuery1[1], Arrays.asList(scoresQuery1))) }
                ),
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        new ScoreDoc(3, l2Norm(scoresQuery2[0], Arrays.asList(scoresQuery2))),
                        new ScoreDoc(4, l2Norm(scoresQuery2[1], Arrays.asList(scoresQuery2))),
                        new ScoreDoc(2, l2Norm(scoresQuery2[2], Arrays.asList(scoresQuery2))) }
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
        L2ScoreNormalizationTechnique normalizationTechnique = new L2ScoreNormalizationTechnique();
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
                        new ScoreDoc(2, l2Norm(scoresShard1Query1[0], Arrays.asList(scoresShard1Query1))),
                        new ScoreDoc(4, l2Norm(scoresShard1Query1[1], Arrays.asList(scoresShard1Query1))) }
                ),
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        new ScoreDoc(3, l2Norm(scoresShard1and2Query3[0], Arrays.asList(scoresShard1and2Query3))),
                        new ScoreDoc(4, l2Norm(scoresShard1and2Query3[1], Arrays.asList(scoresShard1and2Query3))),
                        new ScoreDoc(2, l2Norm(scoresShard1and2Query3[2], Arrays.asList(scoresShard1and2Query3))) }
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
                        new ScoreDoc(7, l2Norm(scoresShard2Query2[0], Arrays.asList(scoresShard2Query2))),
                        new ScoreDoc(9, l2Norm(scoresShard2Query2[1], Arrays.asList(scoresShard2Query2))) }
                ),
                new TopDocs(
                    new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        new ScoreDoc(3, l2Norm(scoresShard1and2Query3[3], Arrays.asList(scoresShard1and2Query3))),
                        new ScoreDoc(9, l2Norm(scoresShard1and2Query3[4], Arrays.asList(scoresShard1and2Query3))),
                        new ScoreDoc(10, l2Norm(scoresShard1and2Query3[5], Arrays.asList(scoresShard1and2Query3))),
                        new ScoreDoc(15, l2Norm(scoresShard1and2Query3[6], Arrays.asList(scoresShard1and2Query3))) }
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
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
                new ScoreDoc(1, 2.0f),  // L2 norm will be 2/sqrt(4+9) = 0.5547
                new ScoreDoc(2, 3.0f)   // L2 norm will be 3/sqrt(4+9) = 0.8321
            }
        );

        TopDocs topDocs2 = new TopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
                new ScoreDoc(2, 4.0f),  // L2 norm will be 4/sqrt(16+25) = 0.6247
                new ScoreDoc(1, 5.0f)   // L2 norm will be 5/sqrt(16+25) = 0.7809
            }
        );

        TopDocs topDocs3 = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 1.0f)   // L2 norm
                                                                                                                               // will be
                                                                                                                               // 1/sqrt(1)
                                                                                                                               // = 1.0
        });

        // Create CompoundTopDocs with multiple subqueries
        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(5, TotalHits.Relation.EQUAL_TO),
            Arrays.asList(topDocs1, topDocs2, topDocs3),
            false,
            searchShard
        );

        L2ScoreNormalizationTechnique normalizer = new L2ScoreNormalizationTechnique();
        Map<DocIdAtSearchShard, ExplanationDetails> result = normalizer.explain(Collections.singletonList(compoundTopDocs));

        // Verify results
        DocIdAtSearchShard doc1 = new DocIdAtSearchShard(1, searchShard);
        DocIdAtSearchShard doc2 = new DocIdAtSearchShard(2, searchShard);

        // Verify document 1 normalized scores
        ExplanationDetails doc1Details = result.get(doc1);
        assertNotNull(doc1Details);
        List<Pair<Float, String>> doc1Scores = doc1Details.getScoreDetails();
        assertEquals(3, doc1Scores.size());

        // Verify L2 normalized scores for document 1
        assertEquals(0.5547f, doc1Scores.get(0).getKey(), DELTA_FOR_ASSERTION); // First subquery
        assertEquals(0.7809f, doc1Scores.get(1).getKey(), DELTA_FOR_ASSERTION); // Second subquery
        assertEquals(1.0000f, doc1Scores.get(2).getKey(), DELTA_FOR_ASSERTION); // Third subquery

        // Verify document 2 normalized scores
        ExplanationDetails doc2Details = result.get(doc2);
        assertNotNull(doc2Details);
        List<Pair<Float, String>> doc2Scores = doc2Details.getScoreDetails();
        assertEquals(3, doc2Scores.size());

        // Verify L2 normalized scores for document 2
        assertEquals(0.8321f, doc2Scores.get(0).getKey(), DELTA_FOR_ASSERTION); // First subquery
        assertEquals(0.6247f, doc2Scores.get(1).getKey(), DELTA_FOR_ASSERTION); // Second subquery
        assertEquals(0.0000f, doc2Scores.get(2).getKey(), DELTA_FOR_ASSERTION); // Third subquery (doc2 not present)

        // Verify that original ScoreDoc scores were updated with L2 normalized values
        assertEquals(0.5547f, topDocs1.scoreDocs[0].score, DELTA_FOR_ASSERTION); // doc1 in first subquery
        assertEquals(0.8321f, topDocs1.scoreDocs[1].score, DELTA_FOR_ASSERTION); // doc2 in first subquery
        assertEquals(0.6247f, topDocs2.scoreDocs[0].score, DELTA_FOR_ASSERTION); // doc2 in second subquery
        assertEquals(0.7809f, topDocs2.scoreDocs[1].score, DELTA_FOR_ASSERTION); // doc1 in second subquery
        assertEquals(1.0000f, topDocs3.scoreDocs[0].score, DELTA_FOR_ASSERTION); // doc1 in third subquery

        // Verify explanation descriptions
        assertTrue(doc1Scores.get(0).getValue().contains("l2 normalization"));
        assertTrue(doc1Scores.get(1).getValue().contains("l2 normalization"));
        assertTrue(doc1Scores.get(2).getValue().contains("l2 normalization"));
    }

    public void testInvalidParameters() {
        Map<String, Object> parameters = new HashMap<>();
        List<Map<String, Object>> lowerBoundsList = List.of(
            Map.of("min_score", 0.1, "mode", "clip"),
            Map.of("mode", "ignore", "invalid_param", "value")
        );
        parameters.put("lower_bounds", lowerBoundsList);

        try {
            new L2ScoreNormalizationTechnique(parameters, new ScoreNormalizationUtil());
            fail("expected IllegalArgumentException was not thrown");
        } catch (IllegalArgumentException e) {
            assertEquals("unrecognized parameters in normalization technique", e.getMessage());
        }
    }

    public void testUnsupportedTopLevelParameter() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("invalid_top_level_param", "value"); // Adding an invalid top-level parameter

        try {
            new L2ScoreNormalizationTechnique(parameters, new ScoreNormalizationUtil());
            fail("expected IllegalArgumentException was not thrown");
        } catch (IllegalArgumentException e) {
            assertEquals("unrecognized parameters in normalization technique", e.getMessage());
        }
    }

    private float l2Norm(float score, List<Float> scores) {
        return score / (float) Math.sqrt(scores.stream().map(Float::doubleValue).map(s -> s * s).mapToDouble(Double::doubleValue).sum());
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
