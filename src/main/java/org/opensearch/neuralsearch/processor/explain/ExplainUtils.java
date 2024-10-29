/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.explain;

import org.apache.lucene.search.Explanation;
import org.opensearch.neuralsearch.processor.SearchShard;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for explain functionality
 */
public class ExplainUtils {

    /**
     * Creates map of DocIdAtQueryPhase to String containing source and normalized scores
     * @param normalizedScores map of DocIdAtQueryPhase to normalized scores
     * @param sourceScores map of DocIdAtQueryPhase to source scores
     * @return map of DocIdAtQueryPhase to String containing source and normalized scores
     */
    public static Map<DocIdAtSearchShard, ExplainDetails> getDocIdAtQueryForNormalization(
        final Map<DocIdAtSearchShard, List<Float>> normalizedScores,
        final Map<DocIdAtSearchShard, List<Float>> sourceScores
    ) {
        Map<DocIdAtSearchShard, ExplainDetails> explain = sourceScores.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                List<Float> srcScores = entry.getValue();
                List<Float> normScores = normalizedScores.get(entry.getKey());
                return new ExplainDetails(
                    normScores.stream().reduce(0.0f, Float::max),
                    String.format(Locale.ROOT, "source scores: %s, normalized scores: %s", srcScores, normScores)
                );
            }));
        return explain;
    }

    /**
     * Creates map of DocIdAtQueryPhase to String containing source scores and combined score
     * @param scoreCombinationTechnique the combination technique used
     * @param normalizedScoresPerDoc map of DocIdAtQueryPhase to normalized scores
     * @param searchShard the search shard
     * @return map of DocIdAtQueryPhase to String containing source scores and combined score
     */
    public static Map<DocIdAtSearchShard, ExplainDetails> getDocIdAtQueryForCombination(
        ScoreCombinationTechnique scoreCombinationTechnique,
        Map<Integer, float[]> normalizedScoresPerDoc,
        SearchShard searchShard
    ) {
        Map<DocIdAtSearchShard, ExplainDetails> explain = new HashMap<>();
        // - create map of combined scores per doc id
        Map<Integer, Float> combinedNormalizedScoresByDocId = normalizedScoresPerDoc.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> scoreCombinationTechnique.combine(entry.getValue())));
        normalizedScoresPerDoc.forEach((key, srcScores) -> {
            float combinedScore = combinedNormalizedScoresByDocId.get(key);
            explain.put(
                new DocIdAtSearchShard(key, searchShard),
                new ExplainDetails(
                    combinedScore,
                    String.format(Locale.ROOT, "source scores: %s, combined score %s", Arrays.toString(srcScores), combinedScore)
                )
            );
        });
        return explain;
    }

    /*    public static Map<DocIdAtSearchShard, List<ExplainDetails>> getExplainsByShardForCombination(
        ScoreCombinationTechnique scoreCombinationTechnique,
        Map<Integer, float[]> normalizedScoresPerDoc,
        SearchShard searchShard
    ) {
        Map<DocIdAtSearchShard, ExplainDetails> explain = new HashMap<>();
        // - create map of combined scores per doc id
        Map<Integer, Float> combinedNormalizedScoresByDocId = normalizedScoresPerDoc.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> scoreCombinationTechnique.combine(entry.getValue())));
        normalizedScoresPerDoc.forEach((key, srcScores) -> {
            float combinedScore = combinedNormalizedScoresByDocId.get(key);
            explain.put(
                new DocIdAtSearchShard(key, searchShard),
                new ExplainDetails(
                    combinedScore,
                    String.format("source scores: %s, combined score %s", Arrays.toString(srcScores), combinedScore)
                )
            );
        });
        return explain;
    }
     */

    /**
     * Creates a string describing the combination technique and its parameters
     * @param techniqueName the name of the combination technique
     * @param weights the weights used in the combination technique
     * @return a string describing the combination technique and its parameters
     */
    public static String describeCombinationTechnique(final String techniqueName, final List<Float> weights) {
        return String.format(Locale.ROOT, "combination technique [%s] with optional parameters [%s]", techniqueName, weights);
    }

    /**
     * Creates an Explanation object for the top-level explanation of the combined score
     * @param explainableNormalizationTechnique the normalization technique used
     * @param explainableCombinationTechnique the combination technique used
     * @return an Explanation object for the top-level explanation of the combined score
     */
    public static Explanation topLevelExpalantionForCombinedScore(
        final ExplainableTechnique explainableNormalizationTechnique,
        final ExplainableTechnique explainableCombinationTechnique
    ) {
        String explanationDetailsMessage = String.format(
            Locale.ROOT,
            "combine score with techniques: %s, %s",
            explainableNormalizationTechnique.describe(),
            explainableCombinationTechnique.describe()
        );

        return Explanation.match(0.0f, explanationDetailsMessage);
    }

}
