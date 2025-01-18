/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.processor.SearchShard;
import org.opensearch.neuralsearch.processor.NormalizeScoresDTO;
import org.opensearch.neuralsearch.processor.explain.DocIdAtSearchShard;
import org.opensearch.neuralsearch.processor.explain.ExplanationDetails;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.search.SearchShardTarget;

import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;

/**
 * Abstracts normalization of scores based on min-max method
 */
public class MinMaxScoreNormalizationTechniqueTests extends OpenSearchQueryTestCase {
    private static final float DELTA_FOR_ASSERTION = 0.0001f;
    private static final SearchShard SEARCH_SHARD = new SearchShard("my_index", 0, "12345678");

    public void testNormalization_whenResultFromOneShardOneSubQuery_thenSuccessful() {
        MinMaxScoreNormalizationTechnique normalizationTechnique = new MinMaxScoreNormalizationTechnique();
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
            .build();
        normalizationTechnique.normalize(normalizeScoresDTO);

        CompoundTopDocs expectedCompoundDocs = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(2, 1.0f), new ScoreDoc(4, DELTA_FOR_SCORE_ASSERTION) }
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
        MinMaxScoreNormalizationTechnique normalizationTechnique = new MinMaxScoreNormalizationTechnique();
        List<CompoundTopDocs> compoundTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, 0.5f), new ScoreDoc(4, 0.2f) }
                    ),
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(3, 0.9f), new ScoreDoc(4, 0.7f), new ScoreDoc(2, 0.1f) }
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
                    new ScoreDoc[] { new ScoreDoc(2, 1.0f), new ScoreDoc(4, DELTA_FOR_SCORE_ASSERTION) }
                ),
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(3, 1.0f), new ScoreDoc(4, 0.75f), new ScoreDoc(2, DELTA_FOR_SCORE_ASSERTION) }
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
        MinMaxScoreNormalizationTechnique normalizationTechnique = new MinMaxScoreNormalizationTechnique();
        List<CompoundTopDocs> compoundTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, 0.5f), new ScoreDoc(4, 0.2f) }
                    ),
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(3, 0.9f), new ScoreDoc(4, 0.7f), new ScoreDoc(2, 0.1f) }
                    )
                ),
                false,
                SEARCH_SHARD
            ),
            new CompoundTopDocs(
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(7, 2.9f), new ScoreDoc(9, 0.7f) }
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
                    new ScoreDoc[] { new ScoreDoc(2, 1.0f), new ScoreDoc(4, DELTA_FOR_SCORE_ASSERTION) }
                ),
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(3, 1.0f), new ScoreDoc(4, 0.75f), new ScoreDoc(2, DELTA_FOR_SCORE_ASSERTION) }
                )
            ),
            false,
            SEARCH_SHARD
        );

        CompoundTopDocs expectedCompoundDocsShard2 = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(7, 1.0f), new ScoreDoc(9, DELTA_FOR_SCORE_ASSERTION) }
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
            new ScoreDoc[] { new ScoreDoc(1, 0.8f), new ScoreDoc(2, 0.6f) }
        );

        TopDocs topDocs2 = new TopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] { new ScoreDoc(2, 0.9f), new ScoreDoc(1, 0.7f) }
        );

        TopDocs topDocs3 = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 0.5f) });

        // Create CompoundTopDocs with multiple subqueries
        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(5, TotalHits.Relation.EQUAL_TO),
            Arrays.asList(topDocs1, topDocs2, topDocs3),
            false,  // isSortEnabled
            searchShard
        );

        MinMaxScoreNormalizationTechnique normalizer = new MinMaxScoreNormalizationTechnique();
        Map<DocIdAtSearchShard, ExplanationDetails> result = normalizer.explain(Collections.singletonList(compoundTopDocs));

        // Verify results
        DocIdAtSearchShard doc1 = new DocIdAtSearchShard(1, searchShard);
        DocIdAtSearchShard doc2 = new DocIdAtSearchShard(2, searchShard);

        // Verify document 1 normalized scores
        ExplanationDetails doc1Details = result.get(doc1);
        assertNotNull(doc1Details);
        List<Pair<Float, String>> doc1Scores = doc1Details.getScoreDetails();
        assertEquals(3, doc1Scores.size());

        // First subquery (0.8 is max, 0.6 is min)
        assertEquals(1.0f, doc1Scores.get(0).getKey(), DELTA_FOR_SCORE_ASSERTION);
        // Second subquery (0.9 is max, 0.7 is min)
        assertEquals(0.0f, doc1Scores.get(1).getKey(), DELTA_FOR_SCORE_ASSERTION);
        // Third subquery (0.5 is both max and min)
        assertEquals(1.0f, doc1Scores.get(2).getKey(), DELTA_FOR_SCORE_ASSERTION);

        // Verify document 2 normalized scores
        ExplanationDetails doc2Details = result.get(doc2);
        assertNotNull(doc2Details);
        List<Pair<Float, String>> doc2Scores = doc2Details.getScoreDetails();
        assertEquals(3, doc2Scores.size());

        // First subquery (0.8 is max, 0.6 is min)
        assertEquals(0.0f, doc2Scores.get(0).getKey(), DELTA_FOR_SCORE_ASSERTION);
        // Second subquery (0.9 is max, 0.7 is min)
        assertEquals(1.0f, doc2Scores.get(1).getKey(), DELTA_FOR_SCORE_ASSERTION);
        // Third subquery (document 2 not present in third subquery)
        assertEquals(0.0f, doc2Scores.get(2).getKey(), DELTA_FOR_SCORE_ASSERTION);

        // Verify that original ScoreDoc scores were updated
        assertEquals(1.0f, topDocs1.scoreDocs[0].score, DELTA_FOR_SCORE_ASSERTION); // doc1 in first subquery
        assertEquals(0.0f, topDocs1.scoreDocs[1].score, DELTA_FOR_SCORE_ASSERTION); // doc2 in first subquery
        assertEquals(1.0f, topDocs2.scoreDocs[0].score, DELTA_FOR_SCORE_ASSERTION); // doc2 in second subquery
        assertEquals(0.0f, topDocs2.scoreDocs[1].score, DELTA_FOR_SCORE_ASSERTION); // doc1 in second subquery
        assertEquals(1.0f, topDocs3.scoreDocs[0].score, DELTA_FOR_SCORE_ASSERTION); // doc1 in third subquery
    }

    private void assertCompoundTopDocs(TopDocs expected, TopDocs actual) {
        assertEquals(expected.totalHits.value, actual.totalHits.value);
        assertEquals(expected.totalHits.relation, actual.totalHits.relation);
        assertEquals(expected.scoreDocs.length, actual.scoreDocs.length);
        for (int i = 0; i < expected.scoreDocs.length; i++) {
            assertEquals(expected.scoreDocs[i].score, actual.scoreDocs[i].score, DELTA_FOR_ASSERTION);
            assertEquals(expected.scoreDocs[i].doc, actual.scoreDocs[i].doc);
            assertEquals(expected.scoreDocs[i].shardIndex, actual.scoreDocs[i].shardIndex);
        }
    }
}
