/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util.pruning;

import org.opensearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * Utility class providing methods for pruning sparse vectors using different strategies.
 * Pruning helps reduce the dimensionality of sparse vectors by removing less significant elements
 * based on various criteria.
 */
public class PruneUtils {
    public static final String PRUNE_TYPE_FIELD = "prune_type";
    public static final String PRUNE_RATIO_FIELD = "prune_ratio";

    /**
     * Prunes a sparse vector by keeping only the top K elements with the highest values.
     *
     * @param sparseVector The input sparse vector as a map of string keys to float values
     * @param k The number of top elements to keep
     * @param requiresPrunedEntries Whether to return pruned entries
     * @return A tuple containing two maps: the first with top K elements, the second with remaining elements (or null)
     */
    private static Tuple<Map<String, Float>, Map<String, Float>> pruningByTopK(
        Map<String, Float> sparseVector,
        int k,
        boolean requiresPrunedEntries
    ) {
        PriorityQueue<Map.Entry<String, Float>> pq = new PriorityQueue<>((a, b) -> Float.compare(a.getValue(), b.getValue()));

        for (Map.Entry<String, Float> entry : sparseVector.entrySet()) {
            if (pq.size() < k) {
                pq.offer(entry);
            } else if (entry.getValue() > pq.peek().getValue()) {
                pq.poll();
                pq.offer(entry);
            }
        }

        Map<String, Float> highScores = new HashMap<>();
        Map<String, Float> lowScores = requiresPrunedEntries ? new HashMap<>(sparseVector) : null;

        while (!pq.isEmpty()) {
            Map.Entry<String, Float> entry = pq.poll();
            highScores.put(entry.getKey(), entry.getValue());
            if (requiresPrunedEntries) {
                lowScores.remove(entry.getKey());
            }
        }

        return new Tuple<>(highScores, lowScores);
    }

    /**
     * Prunes a sparse vector by keeping only elements whose values are within a certain ratio
     * of the maximum value in the vector.
     *
     * @param sparseVector The input sparse vector as a map of string keys to float values
     * @param ratio The minimum ratio relative to the maximum value for elements to be kept
     * @param requiresPrunedEntries Whether to return pruned entries
     * @return A tuple containing two maps: the first with elements meeting the ratio threshold,
     *         the second with elements below the threshold (or null)
     */
    private static Tuple<Map<String, Float>, Map<String, Float>> pruningByMaxRatio(
        Map<String, Float> sparseVector,
        float ratio,
        boolean requiresPrunedEntries
    ) {
        float maxValue = sparseVector.values().stream().max(Float::compareTo).orElse(0f);

        Map<String, Float> highScores = new HashMap<>();
        Map<String, Float> lowScores = requiresPrunedEntries ? new HashMap<>() : null;

        for (Map.Entry<String, Float> entry : sparseVector.entrySet()) {
            if (entry.getValue() >= ratio * maxValue) {
                highScores.put(entry.getKey(), entry.getValue());
            } else if (requiresPrunedEntries) {
                lowScores.put(entry.getKey(), entry.getValue());
            }
        }

        return new Tuple<>(highScores, lowScores);
    }

    /**
     * Prunes a sparse vector by removing elements with values below a certain threshold.
     *
     * @param sparseVector The input sparse vector as a map of string keys to float values
     * @param thresh The minimum absolute value for elements to be kept
     * @param requiresPrunedEntries Whether to return pruned entries
     * @return A tuple containing two maps: the first with elements above the threshold,
     *         the second with elements below the threshold (or null)
     */
    private static Tuple<Map<String, Float>, Map<String, Float>> pruningByValue(
        Map<String, Float> sparseVector,
        float thresh,
        boolean requiresPrunedEntries
    ) {
        Map<String, Float> highScores = new HashMap<>();
        Map<String, Float> lowScores = requiresPrunedEntries ? new HashMap<>() : null;

        for (Map.Entry<String, Float> entry : sparseVector.entrySet()) {
            if (entry.getValue() >= thresh) {
                highScores.put(entry.getKey(), entry.getValue());
            } else if (requiresPrunedEntries) {
                lowScores.put(entry.getKey(), entry.getValue());
            }
        }

        return new Tuple<>(highScores, lowScores);
    }

    /**
     * Prunes a sparse vector by keeping only elements whose cumulative sum of values
     * is within a certain ratio of the total sum.
     *
     * @param sparseVector The input sparse vector as a map of string keys to float values
     * @param alpha The minimum ratio relative to the total sum for elements to be kept
     * @param requiresPrunedEntries Whether to return pruned entries
     * @return A tuple containing two maps: the first with elements meeting the alpha mass threshold,
     *         the second with remaining elements (or null)
     */
    private static Tuple<Map<String, Float>, Map<String, Float>> pruningByAlphaMass(
        Map<String, Float> sparseVector,
        float alpha,
        boolean requiresPrunedEntries
    ) {
        List<Map.Entry<String, Float>> sortedEntries = new ArrayList<>(sparseVector.entrySet());
        sortedEntries.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));

        float sum = (float) sparseVector.values().stream().mapToDouble(Float::doubleValue).sum();
        float topSum = 0f;

        Map<String, Float> highScores = new HashMap<>();
        Map<String, Float> lowScores = requiresPrunedEntries ? new HashMap<>() : null;

        for (Map.Entry<String, Float> entry : sortedEntries) {
            float value = entry.getValue();
            topSum += value;

            if (topSum <= alpha * sum) {
                highScores.put(entry.getKey(), value);
            } else if (requiresPrunedEntries) {
                lowScores.put(entry.getKey(), value);
            }
        }

        return new Tuple<>(highScores, lowScores);
    }

    /**
     * Prunes a sparse vector using the specified pruning type and ratio.
     *
     * @param pruneType The type of pruning strategy to use
     * @param pruneRatio The ratio or threshold for pruning
     * @param sparseVector The input sparse vector as a map of string keys to float values
     * @param requiresPrunedEntries Whether to return pruned entries
     * @return A tuple containing two maps: the first with high-scoring elements,
     *         the second with low-scoring elements (or null if requiresPrunedEntries is false)
     */
    public static Tuple<Map<String, Float>, Map<String, Float>> pruningSparseVector(
        PruneType pruneType,
        float pruneRatio,
        Map<String, Float> sparseVector,
        boolean requiresPrunedEntries
    ) {
        if (Objects.isNull(pruneType) || Objects.isNull(pruneRatio)) {
            throw new IllegalArgumentException("Prune type and prune ratio must be provided");
        }

        if (Objects.isNull(sparseVector)) {
            throw new IllegalArgumentException("Sparse vector must be provided");
        }

        for (Map.Entry<String, Float> entry : sparseVector.entrySet()) {
            if (entry.getValue() <= 0) {
                throw new IllegalArgumentException("Pruned values must be positive");
            }
        }

        switch (pruneType) {
            case TOP_K:
                return pruningByTopK(sparseVector, (int) pruneRatio, requiresPrunedEntries);
            case ALPHA_MASS:
                return pruningByAlphaMass(sparseVector, pruneRatio, requiresPrunedEntries);
            case MAX_RATIO:
                return pruningByMaxRatio(sparseVector, pruneRatio, requiresPrunedEntries);
            case ABS_VALUE:
                return pruningByValue(sparseVector, pruneRatio, requiresPrunedEntries);
            default:
                return new Tuple<>(new HashMap<>(sparseVector), requiresPrunedEntries ? new HashMap<>() : null);
        }
    }

    /**
     * Validates whether a prune ratio is valid for a given pruning type.
     *
     * @param pruneType The type of pruning strategy
     * @param pruneRatio The ratio or threshold to validate
     * @return true if the ratio is valid for the given pruning type, false otherwise
     * @throws IllegalArgumentException if pruning type is null
     */
    public static boolean isValidPruneRatio(PruneType pruneType, float pruneRatio) {
        if (pruneType == null) {
            throw new IllegalArgumentException("Pruning type cannot be null");
        }

        switch (pruneType) {
            case TOP_K:
                return pruneRatio > 0 && pruneRatio == Math.floor(pruneRatio);
            case ALPHA_MASS:
            case MAX_RATIO:
                return pruneRatio >= 0 && pruneRatio < 1;
            case ABS_VALUE:
                return pruneRatio >= 0;
            default:
                return true;
        }
    }
}
