/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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

import static org.opensearch.neuralsearch.processor.normalization.MinMaxScoreNormalizationTechnique.MIN_SCORE;
import static org.opensearch.neuralsearch.query.HybridQueryBuilder.MAX_NUMBER_OF_SUB_QUERIES;
import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;
import static org.opensearch.neuralsearch.util.TestUtils.PARAM_NAME_LOWER_BOUNDS;

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

        // Verify explanation descriptions
        assertTrue(doc1Scores.get(0).getValue().contains("min_max normalization"));
        assertTrue(doc1Scores.get(1).getValue().contains("min_max normalization"));
        assertTrue(doc1Scores.get(2).getValue().contains("min_max normalization"));
    }

    public void testLowerBoundsModeFromString_whenValidValues_thenSuccessful() {
        assertEquals(
            MinMaxScoreNormalizationTechnique.LowerBound.Mode.APPLY,
            MinMaxScoreNormalizationTechnique.LowerBound.Mode.fromString("apply")
        );
        assertEquals(
            MinMaxScoreNormalizationTechnique.LowerBound.Mode.CLIP,
            MinMaxScoreNormalizationTechnique.LowerBound.Mode.fromString("clip")
        );
        assertEquals(
            MinMaxScoreNormalizationTechnique.LowerBound.Mode.IGNORE,
            MinMaxScoreNormalizationTechnique.LowerBound.Mode.fromString("ignore")
        );
        // Case insensitive check
        assertEquals(
            MinMaxScoreNormalizationTechnique.LowerBound.Mode.APPLY,
            MinMaxScoreNormalizationTechnique.LowerBound.Mode.fromString("APPLY")
        );
    }

    public void testMode_fromString_invalidValues() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> MinMaxScoreNormalizationTechnique.LowerBound.Mode.fromString("invalid")
        );
        assertEquals("invalid mode: invalid, valid values are: apply, clip, ignore", exception.getMessage());
    }

    public void testLowerBoundsModeFromString_whenNullOrEmpty_thenFail() {
        IllegalArgumentException nullException = expectThrows(
            IllegalArgumentException.class,
            () -> MinMaxScoreNormalizationTechnique.LowerBound.Mode.fromString(null)
        );
        assertEquals("mode value cannot be null or empty", nullException.getMessage());
    }

    public void testLowerBounds_whenModeIsApply_thenSuccessful() {
        float score = 0.5f;
        float minScore = 0.1f;
        float maxScore = 0.8f;
        float lowerBoundScore = 0.3f;

        float normalizedScore = MinMaxScoreNormalizationTechnique.LowerBound.Mode.APPLY.normalize(
            score,
            minScore,
            maxScore,
            lowerBoundScore
        );
        // we expect score as 0.5 - 0.3 / 0.8 - 0.3 = 0.2 / 0.5 = 0.4
        assertEquals(0.4f, normalizedScore, DELTA_FOR_SCORE_ASSERTION);

        // Test when score is below lower bound
        float lowScore = 0.2f;
        float normalizedLowScore = MinMaxScoreNormalizationTechnique.LowerBound.Mode.APPLY.normalize(
            lowScore,
            minScore,
            maxScore,
            lowerBoundScore
        );
        // we expect score as 0.2 - 0.1 / 0.8 - 0.1 = 0.1 / 0.7 = 0.1
        assertEquals(0.143f, normalizedLowScore, DELTA_FOR_SCORE_ASSERTION);
    }

    public void testLowerBounds_whenModeIsClip_thenSuccessful() {
        float score = 0.5f;
        float minScore = 0.2f;
        float maxScore = 0.8f;
        float lowerBoundScore = 0.3f;

        float normalizedScore = MinMaxScoreNormalizationTechnique.LowerBound.Mode.CLIP.normalize(
            score,
            minScore,
            maxScore,
            lowerBoundScore
        );
        assertEquals(0.4f, normalizedScore, DELTA_FOR_SCORE_ASSERTION);

        // Test when score is below min score
        float lowScore = 0.1f;
        float normalizedLowScore = MinMaxScoreNormalizationTechnique.LowerBound.Mode.CLIP.normalize(
            lowScore,
            minScore,
            maxScore,
            lowerBoundScore
        );
        assertEquals(0.0f, normalizedLowScore, DELTA_FOR_SCORE_ASSERTION);
    }

    public void testLowerBounds_whenModeIsIgnore_thenSuccessful() {
        float score = 0.5f;
        float minScore = 0.2f;
        float maxScore = 0.8f;
        float lowerBoundScore = 0.3f;

        float normalizedScore = MinMaxScoreNormalizationTechnique.LowerBound.Mode.IGNORE.normalize(
            score,
            minScore,
            maxScore,
            lowerBoundScore
        );
        assertEquals(0.5f, normalizedScore, DELTA_FOR_SCORE_ASSERTION);

        // Test when normalized score would be 0
        float lowScore = 0.2f;
        float normalizedLowScore = MinMaxScoreNormalizationTechnique.LowerBound.Mode.IGNORE.normalize(
            lowScore,
            minScore,
            maxScore,
            lowerBoundScore
        );
        assertEquals(MIN_SCORE, normalizedLowScore, DELTA_FOR_SCORE_ASSERTION);
    }

    public void testLowerBoundsMode_whenDefaultValue_thenSuccessful() {
        assertEquals(MinMaxScoreNormalizationTechnique.LowerBound.Mode.APPLY, MinMaxScoreNormalizationTechnique.LowerBound.Mode.DEFAULT);
    }

    public void testLowerBounds_whenExceedsMaxSubQueries_thenFail() {
        List<Map<String, Object>> lowerBounds = new ArrayList<>();

        for (int i = 0; i <= 100; i++) {
            Map<String, Object> bound = new HashMap<>();
            if (i % 3 == 0) {
                bound.put("mode", "apply");
                bound.put("min_score", 0.1f);
            } else if (i % 3 == 1) {
                bound.put("mode", "clip");
                bound.put("min_score", 0.1f);
            } else {
                bound.put("mode", "ignore");
            }
            lowerBounds.add(bound);
        }

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("lower_bounds", lowerBounds);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new MinMaxScoreNormalizationTechnique(parameters, new ScoreNormalizationUtil(), false)
        );

        assertEquals(
            String.format(
                Locale.ROOT,
                "lower_bounds size %d should be less than or equal to %d",
                lowerBounds.size(),
                MAX_NUMBER_OF_SUB_QUERIES
            ),
            exception.getMessage()
        );
    }

    public void testDescribe_whenLowerBoundsArePresent_thenSuccessful() {
        Map<String, Object> parameters = new HashMap<>();
        List<Map<String, Object>> lowerBounds = Arrays.asList(
            Map.of("mode", "apply", "min_score", 0.2),

            Map.of("mode", "clip", "min_score", 0.1)
        );
        parameters.put("lower_bounds", lowerBounds);
        MinMaxScoreNormalizationTechnique techniqueWithBounds = new MinMaxScoreNormalizationTechnique(
            parameters,
            new ScoreNormalizationUtil(),
            false
        );
        assertEquals("min_max, lower bounds [(apply, 0.2), (clip, 0.1)]", techniqueWithBounds.describe());

        // Test case 2: without lower bounds
        Map<String, Object> emptyParameters = new HashMap<>();
        MinMaxScoreNormalizationTechnique techniqueWithoutBounds = new MinMaxScoreNormalizationTechnique(
            emptyParameters,
            new ScoreNormalizationUtil(),
            false
        );
        assertEquals("min_max", techniqueWithoutBounds.describe());

        Map<String, Object> parametersMissingMode = new HashMap<>();
        List<Map<String, Object>> lowerBoundsMissingMode = Arrays.asList(
            Map.of("min_score", 0.2),
            Map.of("mode", "clip", "min_score", 0.1)
        );
        parametersMissingMode.put("lower_bounds", lowerBoundsMissingMode);
        MinMaxScoreNormalizationTechnique techniqueMissingMode = new MinMaxScoreNormalizationTechnique(
            parametersMissingMode,
            new ScoreNormalizationUtil(),
            false
        );
        assertEquals("min_max, lower bounds [(apply, 0.2), (clip, 0.1)]", techniqueMissingMode.describe());

        Map<String, Object> parametersMissingScore = new HashMap<>();
        List<Map<String, Object>> lowerBoundsMissingScore = Arrays.asList(
            Map.of("mode", "apply"),
            Map.of("mode", "clip", "min_score", 0.1)
        );
        parametersMissingScore.put("lower_bounds", lowerBoundsMissingScore);
        MinMaxScoreNormalizationTechnique techniqueMissingScore = new MinMaxScoreNormalizationTechnique(
            parametersMissingScore,
            new ScoreNormalizationUtil(),
            false
        );
        assertEquals("min_max, lower bounds [(apply, 0.0), (clip, 0.1)]", techniqueMissingScore.describe());
    }

    public void testLowerBounds_whenInvalidInput_thenFail() {
        // Test case 1: Invalid mode value
        Map<String, Object> parametersInvalidMode = new HashMap<>();
        List<Map<String, Object>> lowerBoundsInvalidMode = Arrays.asList(
            Map.of("mode", "invalid_mode", "min_score", 0.2),
            Map.of("mode", "clip", "min_score", 0.1)
        );
        parametersInvalidMode.put("lower_bounds", lowerBoundsInvalidMode);
        IllegalArgumentException invalidModeException = expectThrows(
            IllegalArgumentException.class,
            () -> new MinMaxScoreNormalizationTechnique(parametersInvalidMode, new ScoreNormalizationUtil(), false)
        );
        assertEquals("invalid mode: invalid_mode, valid values are: apply, clip, ignore", invalidModeException.getMessage());

        // Test case 4: Invalid min_score type
        Map<String, Object> parametersInvalidScore = new HashMap<>();
        List<Map<String, Object>> lowerBoundsInvalidScore = Arrays.asList(
            Map.of("mode", "apply", "min_score", "not_a_number"),
            Map.of("mode", "clip", "min_score", 0.1)
        );
        parametersInvalidScore.put("lower_bounds", lowerBoundsInvalidScore);
        IllegalArgumentException invalidScoreException = expectThrows(
            IllegalArgumentException.class,
            () -> new MinMaxScoreNormalizationTechnique(parametersInvalidScore, new ScoreNormalizationUtil(), false)
        );
        assertEquals("invalid format for min_score: must be a valid float value", invalidScoreException.getMessage());
    }

    public void testLowerBoundsValidation_whenLowerBoundsAndSubQueriesCountMismatch_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        List<Map<String, Object>> lowerBounds = Arrays.asList(Map.of("mode", "clip", "min_score", 0.1));
        parameters.put(PARAM_NAME_LOWER_BOUNDS, lowerBounds);

        List<CompoundTopDocs> compoundTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, 0.5f), new ScoreDoc(4, 0.2f) }
                    ),
                    new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(3, 0.1f) })
                ),
                false,
                SEARCH_SHARD
            )
        );
        ScoreNormalizationTechnique minMaxTechnique = new MinMaxScoreNormalizationTechnique(
            parameters,
            new ScoreNormalizationUtil(),
            false
        );
        NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
            .queryTopDocs(compoundTopDocs)
            .normalizationTechnique(minMaxTechnique)
            .build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> minMaxTechnique.normalize(normalizeScoresDTO)
        );

        assertEquals(
            "expected lower bounds array to contain 2 elements matching the number of sub-queries, but found a mismatch",
            exception.getMessage()
        );
    }

    public void testLowerBoundsValidation_whenTopDocsIsEmpty_thenSuccessful() {
        Map<String, Object> parameters = new HashMap<>();
        List<Map<String, Object>> lowerBounds = Arrays.asList(
            Map.of("mode", "clip", "min_score", 0.1),
            Map.of("mode", "apply", "min_score", 0.0)
        );
        parameters.put(PARAM_NAME_LOWER_BOUNDS, lowerBounds);

        List<CompoundTopDocs> compoundTopDocs = List.of(
            new CompoundTopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), List.of(), false, SEARCH_SHARD),
            new CompoundTopDocs(
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, 0.5f), new ScoreDoc(4, 0.2f) }
                    ),
                    new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(3, 0.1f) })
                ),
                false,
                SEARCH_SHARD
            )
        );
        ScoreNormalizationTechnique minMaxTechnique = new MinMaxScoreNormalizationTechnique(
            parameters,
            new ScoreNormalizationUtil(),
            false
        );
        NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
            .queryTopDocs(compoundTopDocs)
            .normalizationTechnique(minMaxTechnique)
            .build();

        minMaxTechnique.normalize(normalizeScoresDTO);

        CompoundTopDocs expectedCompoundDocsZero = new CompoundTopDocs(
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),
            List.of(),
            false,
            SEARCH_SHARD
        );
        CompoundTopDocs expectedCompoundDocsOne = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(2, 1.0f), new ScoreDoc(4, 0.25f) }
                ),
                new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(3, 1.0f) })
            ),
            false,
            SEARCH_SHARD
        );
        expectedCompoundDocsOne.setScoreDocs(List.of(new ScoreDoc(2, 0.5f), new ScoreDoc(4, 0.2f)));
        assertNotNull(compoundTopDocs);
        assertEquals(2, compoundTopDocs.size());
        CompoundTopDocs compoundTopDocsZero = compoundTopDocs.get(0);
        assertEquals(expectedCompoundDocsZero, compoundTopDocsZero);
        CompoundTopDocs compoundTopDocsOne = compoundTopDocs.get(1);
        assertEquals(expectedCompoundDocsOne, compoundTopDocsOne);
    }

    public void testInvalidParameters() {
        Map<String, Object> parameters = new HashMap<>();
        List<Map<String, Object>> lowerBoundsList = List.of(
            Map.of("min_score", 0.1, "mode", "clip"),
            Map.of("mode", "ignore", "invalid_param", "value") // Adding an invalid nested parameter
        );
        parameters.put("lower_bounds", lowerBoundsList);

        try {
            new MinMaxScoreNormalizationTechnique(parameters, new ScoreNormalizationUtil(), false);
            fail("Expected IllegalArgumentException was not thrown");
        } catch (IllegalArgumentException e) {
            assertEquals("unrecognized parameters in normalization technique", e.getMessage());
        }
    }

    public void testUnsupportedTopLevelParameter() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("invalid_top_level_param", "value"); // Adding an invalid top-level parameter

        try {
            new MinMaxScoreNormalizationTechnique(parameters, new ScoreNormalizationUtil(), false);
            fail("Expected IllegalArgumentException was not thrown");
        } catch (IllegalArgumentException e) {
            assertEquals("unrecognized parameters in normalization technique", e.getMessage());
        }
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
